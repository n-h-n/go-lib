package rds_postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lib/pq"
)

// pgIdentRE matches unquoted Postgres identifiers we allow in DDL helpers.
// Restricting the charset avoids SQL injection via role/table/schema names.
var pgIdentRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validatePGIdent(name string) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}
	if !pgIdentRE.MatchString(name) {
		return fmt.Errorf("invalid postgres identifier %q (expected [a-zA-Z_][a-zA-Z0-9_]*)", name)
	}
	return nil
}

func quoteIdent(name string) (string, error) {
	if err := validatePGIdent(name); err != nil {
		return "", err
	}
	return pq.QuoteIdentifier(name), nil
}

func normalizePrivileges(privileges []string, valid map[string]bool) ([]string, error) {
	if len(privileges) == 0 {
		return nil, fmt.Errorf("no privileges specified")
	}
	out := make([]string, 0, len(privileges))
	for _, p := range privileges {
		up := strings.ToUpper(strings.TrimSpace(p))
		if !valid[up] {
			return nil, fmt.Errorf("invalid privilege: %s", p)
		}
		out = append(out, up)
	}
	return out, nil
}
