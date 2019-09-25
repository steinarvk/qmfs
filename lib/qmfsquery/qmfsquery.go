package qmfsquery

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	pb "github.com/steinarvk/qmfs/gen/qmfspb"
)

var (
	// Explicitly used for queries:
	//  - (negation)
	//  , (AND clause separation)
	//  = (content query)

	// Allowed:
	validFilenameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

func EntityIDShards(shards []string) *pb.EntitiesQuery_Clause {
	return &pb.EntitiesQuery_Clause{
		Kind: &pb.EntitiesQuery_Clause_Shard{
			Shard: &pb.EntitiesQuery_Clause_EntityInShard{
				Shard: shards,
			},
		},
	}
}

func EntityIDEquals(entityID string) *pb.EntitiesQuery_Clause {
	return &pb.EntitiesQuery_Clause{
		Kind: &pb.EntitiesQuery_Clause_EntityId{
			EntityId: entityID,
		},
	}
}

func parseClause(clausestring string) (*pb.EntitiesQuery_Clause, error) {
	unescaped, err := url.PathUnescape(clausestring)
	if err != nil {
		return nil, err
	}
	clausestring = unescaped

	clause := &pb.EntitiesQuery_Clause{}
	if strings.HasPrefix(clausestring, "-") {
		clausestring = clausestring[1:]
		clause.Invert = true
	}

	sep := "="

	if strings.Contains(clausestring, sep) {
		keyval := strings.SplitN(clausestring, sep, 2)
		filename := keyval[0]
		contents := keyval[1]

		if !ValidPath(filename) {
			return nil, fmt.Errorf("invalid filename: %q", filename)
		}

		clause.Kind = &pb.EntitiesQuery_Clause_FileContents{
			FileContents: &pb.EntitiesQuery_Clause_FileHasTrimmedContents{
				Filename: filename,
				Contents: contents,
			},
		}

		return clause, nil
	}

	filename := clausestring

	if !ValidPath(filename) {
		return nil, fmt.Errorf("invalid filename: %q", filename)
	}

	clause.Kind = &pb.EntitiesQuery_Clause_FileExists{
		FileExists: clausestring,
	}

	return clause, nil
}

func Parse(querystring string) (*pb.EntitiesQuery, error) {
	clausestrings := strings.Split(querystring, ",")
	query := &pb.EntitiesQuery{}
	for _, clausestring := range clausestrings {
		clauseq, err := parseClause(clausestring)
		if err != nil {
			return nil, err
		}
		query.Clause = append(query.Clause, clauseq)
	}
	return query, nil
}

func ValidPath(fn string) bool {
	if strings.HasPrefix(fn, "/") || strings.HasSuffix(fn, "/") {
		return false
	}

	for _, comp := range strings.Split(fn, "/") {
		if !ValidFilename(comp) {
			return false
		}
	}

	return true
}

func ValidFilename(fn string) bool {
	if fn == "" {
		return false
	}
	if strings.HasPrefix(fn, "-") {
		return false
	}
	return validFilenameRE.MatchString(fn)
}
