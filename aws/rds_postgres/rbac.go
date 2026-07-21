package rds_postgres

import (
	"fmt"
	"strings"

	"github.com/n-h-n/go-lib/log"
)

// IAMDBUserSpec declares a Postgres login role that authenticates via RDS IAM
// and the privileges it should hold. AlignIAMDBUsers makes the role consistent
// with this spec (idempotent CREATE / GRANT).
//
// Typical caller: a service connecting as the RDS master user at startup to
// provision its own IAM DB user before switching to IAM auth.
type IAMDBUserSpec struct {
	// Name is the Postgres role name. For RDS IAM auth this must match the
	// trailing segment of the IAM role used in rds-db:connect (e.g. "daemon").
	Name string

	// GrantRDSIAM grants the built-in rds_iam role so IAM tokens work.
	GrantRDSIAM bool

	// DatabasePrivileges are granted on the connected database
	// (e.g. "ALL", "CONNECT", "CREATE", "TEMPORARY").
	DatabasePrivileges []string

	// Schema is the schema for SchemaPrivileges / default privileges.
	// Defaults to "public" when empty.
	Schema string

	// SchemaPrivileges are granted on the schema (e.g. "ALL", "USAGE", "CREATE").
	SchemaPrivileges []string

	// DefaultTablePrivileges set ALTER DEFAULT PRIVILEGES … ON TABLES for objects
	// created by the connecting user (usually the master) going forward.
	DefaultTablePrivileges []string

	// DefaultSequencePrivileges set ALTER DEFAULT PRIVILEGES … ON SEQUENCES.
	DefaultSequencePrivileges []string

	// TableGrants maps table name → privileges for peer-style grants after
	// tables exist (e.g. {"okta_users": {"SELECT"}}). Empty tables are skipped.
	TableGrants map[string][]string
}

var (
	validDatabasePrivileges = map[string]bool{
		"CREATE":    true,
		"CONNECT":   true,
		"TEMPORARY": true,
		"TEMP":      true,
		"ALL":       true,
	}
	validSchemaPrivileges = map[string]bool{
		"CREATE": true,
		"USAGE":  true,
		"ALL":    true,
	}
	validTablePrivileges = map[string]bool{
		"SELECT":     true,
		"INSERT":     true,
		"UPDATE":     true,
		"DELETE":     true,
		"TRUNCATE":   true,
		"REFERENCES": true,
		"TRIGGER":    true,
		"ALL":        true,
	}
	validSequencePrivileges = map[string]bool{
		"USAGE":  true,
		"SELECT": true,
		"UPDATE": true,
		"ALL":    true,
	}
)

// IsRoleExistent reports whether a Postgres role exists.
func (c *Client) IsRoleExistent(role string) (bool, error) {
	if err := validatePGIdent(role); err != nil {
		return false, fmt.Errorf("unable to check role: %w", err)
	}
	var exists bool
	err := c.dbClient.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`,
		role,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("could not check if role %s exists: %w", role, err)
	}
	return exists, nil
}

// EnsureLoginRole creates a LOGIN role if it does not already exist.
func (c *Client) EnsureLoginRole(role string) error {
	quoted, err := quoteIdent(role)
	if err != nil {
		return fmt.Errorf("unable to ensure login role: %w", err)
	}

	exists, err := c.IsRoleExistent(role)
	if err != nil {
		return err
	}
	if exists {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "postgres role %s already exists", role)
		}
		return nil
	}

	query := fmt.Sprintf("CREATE USER %s WITH LOGIN", quoted)
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating postgres login role: %s", query)
	}
	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not create role %s: %w", role, err)
	}
	return nil
}

// GrantRDSIAM grants the RDS-managed rds_iam role so the user can authenticate
// with IAM auth tokens.
func (c *Client) GrantRDSIAM(role string) error {
	quoted, err := quoteIdent(role)
	if err != nil {
		return fmt.Errorf("unable to grant rds_iam: %w", err)
	}
	query := fmt.Sprintf("GRANT rds_iam TO %s", quoted)
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "granting rds_iam: %s", query)
	}
	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not grant rds_iam to %s: %w", role, err)
	}
	return nil
}

// HasRDSIAM reports whether role is a member of rds_iam.
func (c *Client) HasRDSIAM(role string) (bool, error) {
	if err := validatePGIdent(role); err != nil {
		return false, fmt.Errorf("unable to check rds_iam membership: %w", err)
	}
	var ok bool
	err := c.dbClient.QueryRow(`
SELECT EXISTS (
  SELECT 1
  FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles u ON u.oid = m.member
  WHERE r.rolname = 'rds_iam' AND u.rolname = $1
)`, role).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("could not check rds_iam membership for %s: %w", role, err)
	}
	return ok, nil
}

// GrantDatabasePrivileges grants privileges on a database to a role.
func (c *Client) GrantDatabasePrivileges(dbName, role string, privileges []string) error {
	dbQ, err := quoteIdent(dbName)
	if err != nil {
		return fmt.Errorf("unable to grant database privileges: %w", err)
	}
	roleQ, err := quoteIdent(role)
	if err != nil {
		return fmt.Errorf("unable to grant database privileges: %w", err)
	}
	perms, err := normalizePrivileges(privileges, validDatabasePrivileges)
	if err != nil {
		return fmt.Errorf("unable to grant database privileges: %w", err)
	}

	query := fmt.Sprintf(
		"GRANT %s ON DATABASE %s TO %s",
		strings.Join(perms, ", "),
		dbQ,
		roleQ,
	)
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "granting database privileges: %s", query)
	}
	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not grant database privileges on %s to %s: %w", dbName, role, err)
	}
	return nil
}

// GrantSchemaPrivileges grants privileges on a schema to a role.
func (c *Client) GrantSchemaPrivileges(schema, role string, privileges []string) error {
	schemaQ, err := quoteIdent(schema)
	if err != nil {
		return fmt.Errorf("unable to grant schema privileges: %w", err)
	}
	roleQ, err := quoteIdent(role)
	if err != nil {
		return fmt.Errorf("unable to grant schema privileges: %w", err)
	}
	perms, err := normalizePrivileges(privileges, validSchemaPrivileges)
	if err != nil {
		return fmt.Errorf("unable to grant schema privileges: %w", err)
	}

	query := fmt.Sprintf(
		"GRANT %s ON SCHEMA %s TO %s",
		strings.Join(perms, ", "),
		schemaQ,
		roleQ,
	)
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "granting schema privileges: %s", query)
	}
	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not grant schema privileges on %s to %s: %w", schema, role, err)
	}
	return nil
}

// AlterDefaultPrivileges grants default privileges in a schema for future
// objects created by the current session user. objectKind is "TABLES" or "SEQUENCES".
func (c *Client) AlterDefaultPrivileges(schema, grantee, objectKind string, privileges []string) error {
	schemaQ, err := quoteIdent(schema)
	if err != nil {
		return fmt.Errorf("unable to alter default privileges: %w", err)
	}
	granteeQ, err := quoteIdent(grantee)
	if err != nil {
		return fmt.Errorf("unable to alter default privileges: %w", err)
	}

	kind := strings.ToUpper(strings.TrimSpace(objectKind))
	var valid map[string]bool
	switch kind {
	case "TABLES":
		valid = validTablePrivileges
	case "SEQUENCES":
		valid = validSequencePrivileges
	default:
		return fmt.Errorf("unable to alter default privileges: object kind must be TABLES or SEQUENCES, got %q", objectKind)
	}

	perms, err := normalizePrivileges(privileges, valid)
	if err != nil {
		return fmt.Errorf("unable to alter default privileges: %w", err)
	}

	query := fmt.Sprintf(
		"ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON %s TO %s",
		schemaQ,
		strings.Join(perms, ", "),
		kind,
		granteeQ,
	)
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "altering default privileges: %s", query)
	}
	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not alter default privileges for %s: %w", grantee, err)
	}
	return nil
}

// EnsureIAMDBUser creates/aligns one IAM DB user from spec. Requires a connection
// with CREATEROLE / GRANT rights (typically the RDS master user).
func (c *Client) EnsureIAMDBUser(spec IAMDBUserSpec) error {
	if err := validatePGIdent(spec.Name); err != nil {
		return fmt.Errorf("unable to ensure IAM DB user: %w", err)
	}

	schema := spec.Schema
	if schema == "" {
		schema = "public"
	}

	if err := c.EnsureLoginRole(spec.Name); err != nil {
		return err
	}

	if spec.GrantRDSIAM {
		has, err := c.HasRDSIAM(spec.Name)
		if err != nil {
			return err
		}
		if !has {
			if err := c.GrantRDSIAM(spec.Name); err != nil {
				return err
			}
		} else if c.verboseMode {
			log.Log.Debugf(c.ctx, "role %s already has rds_iam", spec.Name)
		}
	}

	if len(spec.DatabasePrivileges) > 0 {
		dbName := c.dbName
		if dbName == "" {
			return fmt.Errorf("unable to grant database privileges: client db name is empty")
		}
		if err := c.GrantDatabasePrivileges(dbName, spec.Name, spec.DatabasePrivileges); err != nil {
			return err
		}
	}

	if len(spec.SchemaPrivileges) > 0 {
		if err := c.GrantSchemaPrivileges(schema, spec.Name, spec.SchemaPrivileges); err != nil {
			return err
		}
	}

	if len(spec.DefaultTablePrivileges) > 0 {
		if err := c.AlterDefaultPrivileges(schema, spec.Name, "TABLES", spec.DefaultTablePrivileges); err != nil {
			return err
		}
	}

	if len(spec.DefaultSequencePrivileges) > 0 {
		if err := c.AlterDefaultPrivileges(schema, spec.Name, "SEQUENCES", spec.DefaultSequencePrivileges); err != nil {
			return err
		}
	}

	for table, perms := range spec.TableGrants {
		exists, err := c.IsTableExistent(&Table{Name: table})
		if err != nil {
			return err
		}
		if !exists {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "skipping table grant on %s (table does not exist yet)", table)
			}
			continue
		}
		if err := c.GrantTablePermissions(table, spec.Name, perms); err != nil {
			return err
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "aligned IAM DB user %s", spec.Name)
	}
	return nil
}

// AlignIAMDBUsers ensures each IAM DB user spec is present and granted.
func (c *Client) AlignIAMDBUsers(specs []IAMDBUserSpec) error {
	for i, spec := range specs {
		if err := c.EnsureIAMDBUser(spec); err != nil {
			return fmt.Errorf("align IAM DB users[%d] (%s): %w", i, spec.Name, err)
		}
	}
	return nil
}
