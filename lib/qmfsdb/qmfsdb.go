package qmfsdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/steinarvk/orclib/lib/uniqueid"
	"github.com/steinarvk/qmfs/lib/qmfsquery"
	"github.com/steinarvk/qmfs/lib/qmfsshard"
	"github.com/steinarvk/qmfs/lib/sqlitedb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/steinarvk/qmfs/gen/qmfspb"
)

var (
	schema = sqlitedb.Schema{
		Name: "qmfs",
		Upgrades: sqlitedb.SequentialUpgrades(
			`
			CREATE TABLE items (
				row_guid TEXT NOT NULL PRIMARY KEY,
				namespace TEXT NOT NULL,
				tombstone BOOLEAN NOT NULL CHECK (tombstone=0 OR tombstone=1),
				active BOOLEAN NOT NULL CHECK (active=0 OR active=1),
				directory BOOLEAN NOT NULL CHECK (directory=0 OR directory=1),
				timestamp_unix_nano INTEGER NOT NULL,
				entity_id TEXT NOT NULL,
				entity_id_shard1 TEXT NOT NULL,
				entity_id_shard2 TEXT NOT NULL,
				filename TEXT NOT NULL,
				sha256_hash BLOB NULL,
				trimmed_sha256_hash BLOB NULL,
				data_length INTEGER NULL,
				trimmed_data_length INTEGER NULL,
				whitespace_prefix BLOB NULL,
				trimmed_data BLOB NULL,
				whitespace_suffix BLOB NULL,
				authorship_metadata BLOB NULL
			);

			CREATE TABLE sharding_key (
				always_one INTEGER UNIQUE CHECK (always_one=1),
				sharding_key_bytes BLOB NOT NULL
			)
			`,
			`
			CREATE UNIQUE INDEX idx_row_guid ON items (row_guid);

			CREATE INDEX idx_nef_active_tombstone ON items (namespace, entity_id, filename, active, tombstone);
			CREATE INDEX idx_nef_shards_active_tombstone ON items (namespace, entity_id_shard1, entity_id_shard2, entity_id, filename, active, tombstone);
			`,
		),
	}
)

const (
	channelSize = 1000
)

type Options struct {
	ChangeHook func()
}

type Database struct {
	db *sqlitedb.Database

	shardingKey []byte
	opts        Options

	stmtInsertNewRow        *sqlitedb.PreparedExec
	stmtMarkOldRowsInactive *sqlitedb.PreparedExec
	stmtSetShardingKey      *sqlitedb.PreparedExec

	queryListEntityFiles    *sqlitedb.PreparedQuery
	queryGlobalLastChanged  *sqlitedb.PreparedQuery
	queryGlobalMetadata     *sqlitedb.PreparedQuery
	queryEntityFileHeaders  *sqlitedb.PreparedQuery
	queryAllEntities        *sqlitedb.PreparedQuery
	queryEntitiesByFilename *sqlitedb.PreparedQuery
	queryReadFile           *sqlitedb.PreparedQuery
	queryListNamespaces     *sqlitedb.PreparedQuery
	queryGetShardingKey     *sqlitedb.PreparedQuery
}

type MaybeString struct {
	Value string
	Err   error
}

var notImplemented = errors.New("Not implemented")

type EntityFile struct {
	EntityID string `sql:"entity_id"`
	Filename string `sql:"filename"`
}

var listEntityFilesTransactor = sqlitedb.Transactor("ListEntityFiles")

func (d *Database) ListEntityFiles(ctx context.Context) ([]EntityFile, error) {
	var rv []EntityFile

	var row EntityFile
	err := listEntityFilesTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		return d.queryListEntityFiles.Query(ctx, tx, map[string]interface{}{
			"namespace": "",
		}, &row, func() (bool, error) {
			rv = append(rv, row)
			return true, nil
		})

	})
	if err != nil {
		return nil, err
	}
	return rv, nil
}

var queryEntitiesTransactor = sqlitedb.Transactor("QueryEntities")

func (d *Database) QueryEntities(req *pb.QueryEntitiesRequest, stream pb.QMetadataService_QueryEntitiesServer) error {
	ctx := stream.Context()

	argmap := map[string]interface{}{
		"namespace": req.GetNamespace(),
	}

	var prepq *sqlitedb.PreparedQuery

	var checkfunc func(context.Context, string) (bool, error)

	switch value := req.Kind.(type) {
	case *pb.QueryEntitiesRequest_All:
		prepq = d.queryAllEntities

	case *pb.QueryEntitiesRequest_HasFilename:
		prepq = d.queryEntitiesByFilename
		if value.HasFilename == "" {
			return status.Errorf(codes.InvalidArgument, "HasFilename query with empty filename")
		}
		argmap["filename"] = value.HasFilename

	case *pb.QueryEntitiesRequest_ParsedQuery:
		dynq, dynargmap, dyncheckfunc, err := d.prepareDynamicEntitiesQuery(ctx, req.GetNamespace(), value.ParsedQuery)
		if err != nil {
			return err
		}

		for k, v := range dynargmap {
			argmap[k] = v
		}

		prepq = dynq
		checkfunc = dyncheckfunc

	case nil:
		return status.Errorf(codes.InvalidArgument, "no query")

	case *pb.QueryEntitiesRequest_RawQuery:

	default:
		return status.Errorf(codes.Unimplemented, "unsupported query kind %v", req.Kind)
	}

	type rowType struct {
		EntityID string
	}

	var row rowType

	if err := queryEntitiesTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		return prepq.Query(ctx, tx, argmap, &row, func() (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}

			if checkfunc != nil {
				ok, err := checkfunc(ctx, row.EntityID)
				if err != nil {
					return false, err
				}
				if !ok {
					return true, nil
				}
			}

			if err := stream.Send(&pb.QueryEntitiesResponse{
				EntityId: row.EntityID,
			}); err != nil {
				return false, err
			}

			return true, nil
		})
	}); err != nil {
		return err
	}

	return nil
}

type entityFileHeader struct {
	EntityID          string
	Filename          string
	RowGUID           string
	TimestampUnixNano int64
	Sha256Hash        []byte
	DataLength        int64
	TrimmedSha256Hash []byte
	TrimmedDataLength int64
	Directory         bool
}

var getEntityTransactor = sqlitedb.Transactor("GetEntity")

func (d *Database) GetEntity(ctx context.Context, req *pb.GetEntityRequest) (*pb.GetEntityResponse, error) {
	entityID := req.GetEntityId()
	if entityID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing EntityID")
	}

	var rv []*pb.EntityFileHeader

	var row entityFileHeader
	err := getEntityTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		return d.queryEntityFileHeaders.Query(ctx, tx, map[string]interface{}{
			"namespace": req.GetNamespace(),
			"entity_id": entityID,
		}, &row, func() (bool, error) {
			rv = append(rv, &pb.EntityFileHeader{
				EntityId: row.EntityID,
				Filename: row.Filename,
				Checksums: &pb.Checksums{
					Length:        row.DataLength,
					TrimmedLength: row.TrimmedDataLength,
					Sha256:        row.Sha256Hash,
					TrimmedSha256: row.TrimmedSha256Hash,
				},
				LastChanged: &pb.Timestamp{
					UnixNano: row.TimestampUnixNano,
				},
				RowGuid:   row.RowGUID,
				Directory: row.Directory,
			})
			return true, nil
		})
	})
	if err != nil {
		return nil, err
	}

	if len(rv) == 0 {
		return nil, status.Errorf(codes.NotFound, "Entity not found: %q", entityID)
	}

	rrv := &pb.GetEntityResponse{
		Entity: &pb.Entity{
			EntityId: entityID,
			Files:    map[string]*pb.EntityFileHeader{},
		},
	}
	for _, x := range rv {
		rrv.Entity.Files[x.Filename] = x
	}

	return rrv, nil
}

func computeFileMetadata(data []byte) (*pb.Checksums, error) {
	trimmed := []byte(strings.TrimSpace(string(data)))

	dataSha := sha256.Sum256(data)
	trimmedDataSha := sha256.Sum256(trimmed)

	return &pb.Checksums{
		Length:        int64(len(data)),
		TrimmedLength: int64(len(trimmed)),
		Sha256:        dataSha[:],
		TrimmedSha256: trimmedDataSha[:],
	}, nil
}

func serializeAuthorshipMetadata(md *pb.AuthorshipMetadata) ([]byte, error) {
	if md == nil {
		return nil, nil
	}

	rv, err := proto.Marshal(md)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

var writeFileTx = sqlitedb.Transactor("qmfsdb.WriteOrDeleteFile")

func (d *Database) writeOrDeleteFile(ctx context.Context, namespace, entityID, filename, oldRevisionGUID string, tombstone bool, data []byte, authorship *pb.AuthorshipMetadata, directory bool, replaceType pb.DeletionType) (*pb.EntityFileHeader, error) {
	if len(data) > 0 && tombstone {
		return nil, status.Errorf(codes.Internal, "Cannot both delete and write file")
	}

	if entityID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing EntityID")
	}

	if len(d.shardingKey) == 0 {
		return nil, status.Errorf(codes.Internal, "No sharding key available")
	}

	entityIDShards := qmfsshard.Shard(d.shardingKey, entityID)
	if len(entityIDShards) != 2 {
		return nil, status.Errorf(codes.Internal, "failed to shard EntityID (got %d parts: %v)", len(entityIDShards), entityIDShards)
	}
	logrus.Infof("entityID %q ==> %v", entityID, entityIDShards)

	if filename == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing Filename")
	}

	authorshipBytes, err := serializeAuthorshipMetadata(authorship)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error serializing authorship metadata: %v", err)
	}

	t := time.Now()

	rowGUID, err := uniqueid.New()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error generating GUID: %v", err)
	}

	logrus.Infof("generated new GUID for operation: %q", rowGUID)

	returnedHeader := &pb.EntityFileHeader{
		Namespace: namespace,
		EntityId:  entityID,
		Filename:  filename,
		LastChanged: &pb.Timestamp{
			UnixNano: t.UnixNano(),
		},
		RowGuid:   rowGUID,
		Tombstone: tombstone,
		Directory: directory && !tombstone,
	}

	fields := map[string]interface{}{}

	if !tombstone {
		checksums, err := computeFileMetadata(data)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Error computing checksums: %v", err)
		}
		returnedHeader.Checksums = checksums
		fields["data_length"] = checksums.Length
		fields["sha256_hash"] = checksums.Sha256
		fields["trimmed_data_length"] = checksums.TrimmedLength
		fields["trimmed_sha256_hash"] = checksums.TrimmedSha256
	} else {
		fields["data_length"] = nil
		fields["sha256_hash"] = nil
		fields["trimmed_data_length"] = nil
		fields["trimmed_sha256_hash"] = nil
	}

	actuallyChanging := true

	if err := writeFileTx(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		var previousContents fullFileData
		var hadPreviousContents bool

		if err := d.queryReadFile.Query(ctx, tx, map[string]interface{}{
			"namespace": namespace,
			"entity_id": entityID,
			"filename":  filename,
		}, &previousContents, func() (bool, error) {
			hadPreviousContents = true
			return false, nil
		}); err != nil {
			return err
		}

		if hadPreviousContents {
			// Check the file we're overwriting or deleting.
			switch replaceType {
			case pb.DeletionType_DELETE_NONE:
				return status.Errorf(codes.FailedPrecondition, "file %q already exists", filename)

			case pb.DeletionType_DELETE_FILE:
				if previousContents.Directory {
					return status.Errorf(codes.FailedPrecondition, "file %q is a directory", filename)
				}

			case pb.DeletionType_DELETE_DIR:
				if !previousContents.Directory {
					return status.Errorf(codes.FailedPrecondition, "file %q is not a directory", filename)
				}
			}
		}

		if oldRevisionGUID != "" && oldRevisionGUID != previousContents.RowGUID {
			return status.Errorf(codes.FailedPrecondition, "Conflict: modification of %q but last revision was %q", oldRevisionGUID, previousContents.RowGUID)
		}

		if tombstone && !hadPreviousContents {
			return status.Errorf(codes.NotFound, "File not found")
		} else if !tombstone && hadPreviousContents {
			if hasDataEqualTo(&previousContents, data) {
				actuallyChanging = false

				returnedHeader.LastChanged = &pb.Timestamp{
					UnixNano: previousContents.TimestampUnixNano,
				}
				returnedHeader.RowGuid = previousContents.RowGUID
				return nil
			}
		}

		if err := d.stmtMarkOldRowsInactive.Exec(ctx, tx, map[string]interface{}{
			"namespace": namespace,
			"entity_id": entityID,
			"filename":  filename,
		}); err != nil {
			return err
		}

		fields["entity_id_shard1"] = entityIDShards[0]
		fields["entity_id_shard2"] = entityIDShards[1]

		fields["row_guid"] = rowGUID
		fields["tombstone"] = tombstone
		fields["active"] = true
		fields["timestamp_unix_nano"] = t.UnixNano()

		fields["namespace"] = namespace
		fields["entity_id"] = entityID
		fields["filename"] = filename

		fields["directory"] = directory

		prefix, trimmed, suffix := partitionData(data)

		fields["whitespace_prefix"] = prefix
		fields["trimmed_data"] = trimmed
		fields["whitespace_suffix"] = suffix

		fields["authorship_metadata"] = authorshipBytes

		if err := d.stmtInsertNewRow.Exec(ctx, tx, fields); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if actuallyChanging {
		d.onChange()
	}

	logrus.Infof("writeOrDeleteFile returning: %v", returnedHeader)

	return returnedHeader, nil
}

func (d *Database) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	if !qmfsquery.ValidPath(req.GetFilename()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filename: %q", req.GetFilename())
	}

	if req.GetDirectory() && len(req.GetData()) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "file cannot be both a directory and contain data")
	}

	replaceType := pb.DeletionType_DELETE_FILE
	if req.GetDirectory() {
		replaceType = pb.DeletionType_DELETE_NONE
	}

	header, err := d.writeOrDeleteFile(ctx, req.GetNamespace(), req.GetEntityId(), req.GetFilename(), req.GetOldRevisionGuid(), false, req.GetData(), req.GetAuthorshipMetadata(), req.GetDirectory(), replaceType)
	if err != nil {
		return nil, err
	}

	logrus.Infof("writeFile returning: %v", header)

	return &pb.WriteFileResponse{
		Header: header,
	}, nil
}

func (d *Database) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	switch req.GetDeletionType() {
	case pb.DeletionType_DELETE_ANY:
	case pb.DeletionType_DELETE_FILE:
	case pb.DeletionType_DELETE_DIR:
	case pb.DeletionType_DELETE_NONE:

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid deletion_type (%v)", req.GetDeletionType())
	}

	header, err := d.writeOrDeleteFile(ctx, req.GetNamespace(), req.GetEntityId(), req.GetFilename(), req.GetOldRevisionGuid(), true, nil, req.GetAuthorshipMetadata(), false, req.GetDeletionType())
	if err != nil {
		return nil, err
	}

	return &pb.DeleteFileResponse{
		Header: header,
	}, nil
}

func (d *Database) prepareStatements() error {
	var err error

	d.stmtMarkOldRowsInactive = d.db.PrepareExec(&err, "qmfsdb-mark-old-rows-inactive", `
UPDATE items
SET    active = 0, trimmed_data = NULL, whitespace_prefix = NULL, whitespace_suffix = NULL
WHERE  entity_id = :entity_id
AND    filename = :filename
AND    namespace = :namespace
;
`)

	d.stmtInsertNewRow = d.db.PrepareExec(&err, "qmfsdb-insert-row", `
INSERT INTO items
	(row_guid, tombstone, active, timestamp_unix_nano, entity_id, filename,
	 sha256_hash, trimmed_sha256_hash, data_length, trimmed_data_length,
	 authorship_metadata, namespace, directory,
	 whitespace_prefix, trimmed_data, whitespace_suffix,
   entity_id_shard1, entity_id_shard2)
VALUES
	(:row_guid, :tombstone, :active, :timestamp_unix_nano, :entity_id, :filename,
	:sha256_hash, :trimmed_sha256_hash, :data_length, :trimmed_data_length,
	:authorship_metadata, :namespace, :directory,
	:whitespace_prefix, :trimmed_data, :whitespace_suffix,
  :entity_id_shard1, :entity_id_shard2)
;
`)

	d.stmtSetShardingKey = d.db.PrepareExec(&err, "qmfsdb-set-sharding-key", `
INSERT OR REPLACE INTO sharding_key
  (always_one, sharding_key_bytes)
VALUES
  (1, :sharding_key_bytes)
`)

	d.queryGlobalLastChanged = d.db.PrepareQuery(&err, "qmfsdb-last-changed-globally", `
SELECT MAX(timestamp_unix_nano) AS last_changed_unix_nano
FROM items
`)

	d.queryGlobalMetadata = d.db.PrepareQuery(&err, "qmfsdb-metadata-globally", `
SELECT
	  MAX(timestamp_unix_nano) AS last_changed_unix_nano
	, COUNT(1) AS total_rows
	, SUM(active) AS active_rows
	, SUM(length(whitespace_prefix)+length(trimmed_data)+length(whitespace_suffix)) AS total_stored_data_bytes
FROM items
`)

	d.queryListEntityFiles = d.db.PrepareQuery(&err, "qmfsdb-list-entity-files", `
SELECT entity_id, filename
FROM items
WHERE active=1
AND   tombstone=0
AND   namespace = :namespace
ORDER BY entity_id, filename
`)

	d.queryAllEntities = d.db.PrepareQuery(&err, "qmfsdb-query-all-entities", `
SELECT DISTINCT entity_id
FROM items
WHERE active=1 AND tombstone=0 AND namespace = :namespace
ORDER BY entity_id
`)

	d.queryEntitiesByFilename = d.db.PrepareQuery(&err, "qmfsdb-query-all-entities-with-filename", `
SELECT DISTINCT entity_id
FROM items
WHERE active=1
AND tombstone=0
AND namespace = :namespace
AND filename = :filename
ORDER BY entity_id
`)

	d.queryEntityFileHeaders = d.db.PrepareQuery(&err, "qmfsdb-query-entity-file-headers", `
SELECT entity_id, filename, row_guid, timestamp_unix_nano,
       sha256_hash, data_length, trimmed_sha256_hash, trimmed_data_length,
			 directory
FROM items
WHERE active=1
AND   tombstone=0
AND   namespace = :namespace
AND   entity_id = :entity_id
ORDER BY entity_id, filename
`)

	d.queryReadFile = d.db.PrepareQuery(&err, "qmfsdb-query-read-file", `
SELECT namespace, entity_id, filename, row_guid, timestamp_unix_nano,
       sha256_hash, data_length, trimmed_sha256_hash, trimmed_data_length,
			 whitespace_prefix, trimmed_data, whitespace_suffix,
			 directory
FROM items
WHERE active=1
AND   tombstone=0
AND   namespace = :namespace
AND   entity_id = :entity_id
AND   filename = :filename
`)

	d.queryListNamespaces = d.db.PrepareQuery(&err, "qmfsdb-query-list-namespaces", `
SELECT DISTINCT namespace
FROM items
WHERE active=1 AND tombstone=0
`)

	d.queryGetShardingKey = d.db.PrepareQuery(&err, "qmfsdb-get-sharding-key", `
SELECT sharding_key_bytes
FROM sharding_key
LIMIT 1
`)

	return err
}

func (d *Database) Close() error {
	return d.db.Close()
}

var startupTransactor = sqlitedb.Transactor("qmfsdbStartup")

func (d *Database) onStartup(ctx context.Context) error {
	return startupTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		var row struct {
			ShardingKeyBytes []byte
		}
		var success bool

		if err := d.queryGetShardingKey.Query(ctx, tx, nil, &row, func() (bool, error) {
			success = true
			return false, nil
		}); err != nil {
			return err
		}

		if !success {
			logrus.Infof("No sharding key found; generating")

			key, err := qmfsshard.GenerateKey()
			if err != nil {
				return err
			}

			if err := d.stmtSetShardingKey.Exec(ctx, tx, map[string]interface{}{
				"sharding_key_bytes": key,
			}); err != nil {
				return err
			}

			row.ShardingKeyBytes = key
		}

		d.shardingKey = row.ShardingKeyBytes
		return nil
	})
}

func (d *Database) onChange() {
	if d.opts.ChangeHook != nil {
		d.opts.ChangeHook()
	}
}

func Open(ctx context.Context, localDBFilename string, opts *Options) (*Database, error) {
	if opts == nil {
		opts = &Options{}
	}

	db, err := schema.Open(ctx, localDBFilename)
	if err != nil {
		return nil, err
	}

	rv := &Database{
		db:   db,
		opts: *opts,
	}

	if err := rv.prepareStatements(); err != nil {
		db.Close()
		return nil, err
	}

	if err := rv.onStartup(ctx); err != nil {
		return nil, err
	}

	return rv, nil
}

type fullFileData struct {
	Namespace         string
	EntityID          string
	Filename          string
	RowGUID           string
	TimestampUnixNano int64
	Sha256Hash        []byte
	DataLength        int64
	TrimmedSha256Hash []byte
	TrimmedDataLength int64
	WhitespacePrefix  []byte
	TrimmedData       []byte
	WhitespaceSuffix  []byte
	Directory         bool
}

func hasDataEqualTo(f *fullFileData, data []byte) bool {
	totalLen := len(f.WhitespacePrefix) + len(f.TrimmedData) + len(f.WhitespaceSuffix)
	if len(data) != totalLen {
		return false
	}
	if bytes.Compare(f.WhitespacePrefix, data[:len(f.WhitespacePrefix)]) != 0 {
		return false
	}
	if bytes.Compare(f.WhitespaceSuffix, data[len(data)-len(f.WhitespaceSuffix):]) != 0 {
		return false
	}
	if bytes.Compare(f.TrimmedData, data[len(f.WhitespacePrefix):len(data)-len(f.WhitespaceSuffix)]) != 0 {
		return false
	}
	return true
}

func partitionData(x []byte) ([]byte, []byte, []byte) {
	if len(x) == 0 {
		return nil, nil, nil
	}

	firstNonSpace := -1
	lastNonSpace := -1

	for i, b := range x {
		if !unicode.IsSpace(rune(b)) {
			if firstNonSpace == -1 {
				firstNonSpace = i
				break
			}
		}
	}

	for i := len(x) - 1; i >= 0; i-- {
		if !unicode.IsSpace(rune(x[i])) {
			if lastNonSpace == -1 {
				lastNonSpace = i
				break
			}
		}
	}

	if firstNonSpace == -1 {
		// All spaces.
		return x, nil, nil
	}

	prefixLen := firstNonSpace
	suffixLen := len(x) - 1 - lastNonSpace

	var prefix, suffix []byte
	if prefixLen > 0 {
		prefix = x[:prefixLen]
	}
	if suffixLen > 0 {
		suffix = x[len(x)-suffixLen:]
	}
	return prefix, x[prefixLen : len(x)-suffixLen], suffix
}

var readFileTransactor = sqlitedb.Transactor("ReadFile")

func (d *Database) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	namespace := req.GetNamespace()

	entityID := req.GetEntityId()
	if entityID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing EntityID")
	}

	filename := req.GetFilename()
	if filename == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing Filename")
	}

	success := false

	var row fullFileData
	err := readFileTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		return d.queryReadFile.Query(ctx, tx, map[string]interface{}{
			"namespace": namespace,
			"entity_id": entityID,
			"filename":  filename,
		}, &row, func() (bool, error) {
			success = true
			return false, nil
		})
	})
	if err != nil {
		return nil, err
	}

	if !success {
		return nil, status.Errorf(codes.NotFound, "File not found: entity_id=%q filename=%q", entityID, filename)
	}

	data := append(row.WhitespacePrefix, append(row.TrimmedData, row.WhitespaceSuffix...)...)

	hdr := &pb.EntityFileHeader{
		Namespace: row.Namespace,
		EntityId:  row.EntityID,
		Filename:  row.Filename,
		Checksums: &pb.Checksums{
			Length:        row.DataLength,
			TrimmedLength: row.TrimmedDataLength,
			Sha256:        row.Sha256Hash,
			TrimmedSha256: row.TrimmedSha256Hash,
		},
		LastChanged: &pb.Timestamp{
			UnixNano: row.TimestampUnixNano,
		},
		RowGuid:   row.RowGUID,
		Directory: row.Directory,
	}

	return &pb.ReadFileResponse{
		File: &pb.EntityFile{
			Header: hdr,
			Data:   data,
		},
	}, nil
}

var listNamespacesTransactor = sqlitedb.Transactor("ListNamespaces")

func (d *Database) ListNamespaces(ctx context.Context, req *pb.ListNamespacesRequest) (*pb.ListNamespacesResponse, error) {
	type rowType struct {
		Namespace string
	}
	var row rowType

	var rv pb.ListNamespacesResponse

	err := listNamespacesTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		return d.queryListNamespaces.Query(ctx, tx, nil, &row, func() (bool, error) {
			rv.Namespace = append(rv.Namespace, row.Namespace)
			return true, nil
		})
	})
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

var getMetadataTransactor = sqlitedb.Transactor("GetDatabaseMetadata")

func (d *Database) GetDatabaseMetadata(ctx context.Context, req *pb.GetDatabaseMetadataRequest) (*pb.GetDatabaseMetadataResponse, error) {
	type rowType struct {
		LastChangedUnixNano  *int64
		TotalRows            *int64
		ActiveRows           *int64
		TotalStoredDataBytes *int64
	}
	var row rowType

	var rv pb.DatabaseMetadata

	err := getMetadataTransactor(ctx, d.db, func(ctx context.Context, tx *sql.Tx) error {
		var tsRow struct {
			LastChangedUnixNano *int64
		}
		if err := d.queryGlobalLastChanged.Query(ctx, tx, nil, &tsRow, func() (bool, error) {
			if tsRow.LastChangedUnixNano != nil {
				rv.LastChanged = &pb.Timestamp{
					UnixNano: *tsRow.LastChangedUnixNano,
				}
			}
			return false, nil
		}); err != nil {
			return err
		}

		if !req.OnlyTimestamps {
			if err := d.queryGlobalMetadata.Query(ctx, tx, nil, &row, func() (bool, error) {
				rv.Size = &pb.SizeMetadata{}

				if p := row.TotalRows; p != nil {
					rv.Size.TotalRows = *p
				}
				if p := row.ActiveRows; p != nil {
					rv.Size.ActiveRows = *p
				}
				if p := row.TotalStoredDataBytes; p != nil {
					rv.Size.TotalStoredDataBytes = *p
				}

				return false, nil
			}); err != nil {
				return err
			}

			if len(d.shardingKey) > 0 {
				rv.ShardingKey = &pb.ShardingKey{
					Key: d.shardingKey,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &pb.GetDatabaseMetadataResponse{
		Metadata: &rv,
	}, nil
}

func andJoinSQL(clauses []string) string {
	return "(" + strings.Join(clauses, ") AND (") + ")"
}

func (d *Database) prepareDynamicEntitiesQuery(ctx context.Context, namespace string, query *pb.EntitiesQuery) (*sqlitedb.PreparedQuery, map[string]interface{}, func(context.Context, string) (bool, error), error) {
	sqlquery := `
SELECT DISTINCT base.entity_id AS entity_id
FROM items AS base
`

	whereClauses := []string{
		"base.active=1",
		"base.tombstone=0",
		"base.namespace = :namespace",
	}

	moreArgs := map[string]interface{}{}

	basicJoinExpr := "{tbl}.namespace = :namespace AND base.entity_id = {tbl}.entity_id AND {tbl}.active=1 AND {tbl}.tombstone=0"

	nextTable := 1
	addCondition := func(moreJoinexpr, condexpr string, invert bool) {
		tblName := fmt.Sprintf("j%d", nextTable)
		nextTable++
		joinexpr := basicJoinExpr + " AND " + moreJoinexpr
		joinexprRepl := strings.Replace(joinexpr, "{tbl}", tblName, -1)
		condexprRepl := strings.Replace(condexpr, "{tbl}", tblName, -1)
		sqlquery += fmt.Sprintf("LEFT JOIN items AS %s ON %s\n", tblName, joinexprRepl)
		if invert {
			condexprRepl = "NOT (" + condexprRepl + ")"
		}
		whereClauses = append(whereClauses, condexprRepl)
	}

	nextVar := 1
	assocVariable := func(value interface{}) string {
		varName := fmt.Sprintf("var%d", nextVar)
		nextVar++

		moreArgs[varName] = value
		return ":" + varName
	}

	for _, clause := range query.Clause {
		switch value := clause.Kind.(type) {
		case *pb.EntitiesQuery_Clause_FileExists:
			varname := assocVariable(value.FileExists)
			addCondition(
				"{tbl}.filename = "+varname,
				"{tbl}.row_guid IS NOT NULL",
				clause.Invert)

		case *pb.EntitiesQuery_Clause_EntityId:
			varname := assocVariable(value.EntityId)
			if clause.Invert {
				addCondition(
					"{tbl}.entity_id = "+varname,
					"{tbl}.row_guid IS NULL",
					false)
			} else {
				whereClauses = append(whereClauses, "base.entity_id = "+varname)
			}

		case *pb.EntitiesQuery_Clause_Shard:
			shards := value.Shard.Shard
			if len(shards) > 2 || len(shards) < 1 {
				return nil, nil, nil, status.Errorf(codes.InvalidArgument, "invalid number of shards: %d (%v)", len(shards), shards)
			}
			shard1 := assocVariable(shards[0])
			var shard2 string
			if len(shards) >= 2 {
				shard2 = assocVariable(shards[1])
			}
			if clause.Invert {
				cond := "{tbl}.entity_id_shard1 = " + shard1
				if len(shards) >= 2 {
					cond += " AND {tbl}.entity_id_shard2 = " + shard2
				}
				addCondition(cond, "{tbl}.row_guid IS NULL", false)
			} else {
				whereClauses = append(whereClauses, "base.entity_id_shard1 = "+shard1)
				if len(shards) >= 2 {
					whereClauses = append(whereClauses, "base.entity_id_shard2 = "+shard2)
				}
			}

		case *pb.EntitiesQuery_Clause_FileContents:
			contents := []byte(value.FileContents.GetContents())
			filename := value.FileContents.GetFilename()

			checksums, err := computeFileMetadata(contents)
			if err != nil {
				return nil, nil, nil, status.Errorf(codes.Internal, "error computing checksums: %v", err)
			}

			varFilename := assocVariable(filename)
			varTrimmedLength := assocVariable(int64(checksums.TrimmedLength))
			varTrimmedSha256 := assocVariable(checksums.TrimmedSha256)

			trimmedData := string(contents)

			varTrimmedData := assocVariable([]byte(trimmedData))

			addCondition(
				"{tbl}.filename = "+varFilename+
					" AND {tbl}.trimmed_data_length = "+varTrimmedLength+
					" AND {tbl}.trimmed_sha256_hash = "+varTrimmedSha256+
					" AND {tbl}.trimmed_data = "+varTrimmedData,
				"{tbl}.row_guid IS NOT NULL",
				clause.Invert)

		default:
			return nil, nil, nil, status.Errorf(codes.Unimplemented, "unsupported query clause %v", clause)
		}
	}

	fullSQL := sqlquery + "\nWHERE\n" + andJoinSQL(whereClauses)

	logrus.Infof("Final SQL: %s", fullSQL)
	logrus.Infof("Final fields: %v", moreArgs)

	var err error
	prepared := d.db.PrepareQuery(&err, "qmfsdb-dynamic-entities-query", fullSQL)
	if err != nil {
		return nil, nil, nil, err
	}

	return prepared, moreArgs, nil, nil
}

// TODO when creating anything, require parent director(ies) to exist
// TODO when deleting a directory, require no child entries to exist
// TODO don't return non-direct children ? no -- probably at the FS layer.
