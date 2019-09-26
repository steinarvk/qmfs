package qmfsquery

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
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

func parseArgs(unparsedArgs []string, spec string) ([]interface{}, error) {
	if len(spec) != len(unparsedArgs) {
		return nil, fmt.Errorf("want %d args got %d (%v)", len(spec), len(unparsedArgs), unparsedArgs)
	}

	var rv []interface{}

	for i, code := range spec {
		v := unparsedArgs[i]

		switch code {
		case 'f':
			if !ValidPath(v) {
				return nil, fmt.Errorf("bad filename: %q", v)
			}
			fallthrough

		case 's':
			rv = append(rv, string(v))

		case 'i':
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("bad number %q: %v", v, err)
			}
			rv = append(rv, int(n))

		default:
			return nil, fmt.Errorf("internal error: unknown arg code %q", code)
		}
	}

	return rv, nil
}

func parseFunctionClause(clausestring string, clause *pb.EntitiesQuery_Clause) error {
	simp, err := parseSimpleFunction(clausestring)
	if err != nil {
		return err
	}

	switch simp.functionName {
	case "blank":
		args, err := parseArgs(simp.args, "f")
		if err != nil {
			return err
		}

		clause.Kind = &pb.EntitiesQuery_Clause_FileContents{
			FileContents: &pb.EntitiesQuery_Clause_FileHasTrimmedContents{
				Filename: args[0].(string),
			},
		}

	case "random":
		args, err := parseArgs(simp.args, "i")
		if err != nil {
			return err
		}
		n := args[0].(int)

		clause.Kind = &pb.EntitiesQuery_Clause_Random{
			Random: &pb.EntitiesQuery_Clause_RandomSelection{
				Number: int32(n),
			},
		}

	default:
		return fmt.Errorf("unknown query function %q", simp.functionName)
	}

	return nil
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

	if strings.Contains(clausestring, "[") {
		if err := parseFunctionClause(clausestring, clause); err != nil {
			return nil, err
		}
		return clause, nil
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

type simpleFunction struct {
	functionName string
	args         []string
}

func parseSimpleFunction(s string) (*simpleFunction, error) {
	singleComp, err := splitQuerystring(s)
	if err != nil {
		return nil, err
	}
	if len(singleComp) != 1 {
		return nil, fmt.Errorf("malformed function call clause (top-level comma): %q", s)
	}

	if !strings.Contains(s, "[") || !strings.Contains(s, "]") {
		return nil, fmt.Errorf("malformed function call clause (must contain brackets): %q", s)
	}

	argsStart := strings.Index(s, "[")
	argsEnd := strings.LastIndex(s, "]")

	unparsedArgsString := strings.TrimSpace(s[argsStart+1 : argsEnd])

	unparsedArgs, err := splitQuerystring(unparsedArgsString)
	if err != nil {
		return nil, fmt.Errorf("malformed function call clause (bad args): %v", err)
	}

	functionName := s[:argsStart]
	if strings.TrimSpace(functionName) == "" {
		return nil, fmt.Errorf("malformed function call clause (no function name): %q", s)
	}

	return &simpleFunction{functionName, unparsedArgs}, nil
}

func splitQuerystring(s string) ([]string, error) {
	sep := ','
	inc := '['
	dec := ']'

	var rv []string
	var level int
	var collected []rune

	flush := func() {
		if len(collected) > 0 {
			rv = append(rv, string(collected))
		}
		collected = nil
	}

	for _, ch := range s {
		switch ch {
		case sep:
			if level == 0 {
				flush()
				continue
			}
		case inc:
			level++
		case dec:
			if level == 0 {
				return nil, fmt.Errorf("unmatched %q", dec)
			}
			level--
		}
		collected = append(collected, ch)
	}

	flush()

	return rv, nil
}

func Parse(querystring string) (*pb.EntitiesQuery, error) {
	clausestrings, err := splitQuerystring(querystring)
	if err != nil {
		return nil, err
	}

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
