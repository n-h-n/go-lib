package rds_postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

func (c *Client) AlignTableSchema(table *Table, opts ...func(*alignTableOpts)) error {
	if table == nil {
		return fmt.Errorf("unable to align table schema: table cannot be nil")
	}

	// Check if table exists
	exists, err := c.IsTableExistent(table)
	if err != nil {
		return fmt.Errorf("could not check if table exists: %w", err)
	}
	if !exists {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s does not exist in %s, creating....", table.Name, c.dbName)
		}
		if err = c.CreateTable(table); err != nil {
			return fmt.Errorf("could not create table: %w", err)
		}
	}

	// Check if schema is aligned
	aligned, err := c.IsSchemaAligned(table, opts...)
	if err != nil {
		return fmt.Errorf("could not check if schema is aligned: %w", err)
	}

	if !aligned {
		if err = c.AlterTableDropColumns(table); err != nil {
			return fmt.Errorf("could not drop columns: %w", err)
		}

		if err = c.AlterTableAddColumns(table); err != nil {
			return fmt.Errorf("could not add columns: %w", err)
		}
	}

	// Check if primary key is aligned
	pkColumnName, err := c.GetPrimaryKeyColumnName(table)
	if err != nil {
		return fmt.Errorf("could not get primary key column name: %w", err)
	}

	if pkColumnName != table.PrimaryKeyName {
		if err = c.ChangePrimaryKey(table); err != nil {
			return fmt.Errorf("could not change primary key: %w", err)
		}
	}

	return nil
}

func (c *Client) AlterTableAddColumns(table *Table) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "altering table %s in %s", table.Name, c.dbName)
	}

	if table == nil {
		return fmt.Errorf("unable to alter table: table cannot be nil")
	}

	// Get current columns
	rows, err := c.dbClient.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, table.Name)
	if err != nil {
		return fmt.Errorf("could not get columns: %w", err)
	}
	defer rows.Close()

	currentColumns := make(map[string]struct{})
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return fmt.Errorf("could not scan column name: %w", err)
		}
		currentColumns[columnName] = struct{}{}
	}

	// Check for new columns
	for _, column := range *table.Columns {
		if _, ok := currentColumns[column.Name]; ok {
			continue
		}

		// New column, alter table
		pgType, err := goTypeToPostgresType(column.Type)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table.Name, column.Name, pgType)
		if !column.Nullable {
			defaultValue, err := postgresDefaultValue(column.Type)
			if err != nil {
				return err
			}

			var strForm string
			switch column.Type.Kind() {
			case reflect.Struct:
				if reflect.TypeOf(column.Type) == reflect.TypeOf(time.Time{}) {
					strForm = "'0001-01-01 00:00:00+00'"
				}
			case reflect.Slice:
				strForm = "{}"
			case reflect.Bool:
				strForm = "false"
			case reflect.String:
				strForm = "''"
			default:
				strForm = fmt.Sprintf("%v", defaultValue)
			}

			query += " NOT NULL DEFAULT " + strForm
		}

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "altering table %s in %s with query: %s", table.Name, c.dbName, query)
		}

		if _, err := c.dbClient.Exec(query); err != nil {
			return fmt.Errorf("could not add column: %w", err)
		}

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "altered table %s in %s", table.Name, c.dbName)
		}
	}

	return nil
}

func (c *Client) AlterTableDropColumns(table *Table) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "altering table %s in %s", table.Name, c.dbName)
	}

	if table == nil {
		return fmt.Errorf("unable to alter table: table cannot be nil")
	}

	// Get current columns
	rows, err := c.dbClient.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, table.Name)
	if err != nil {
		return fmt.Errorf("could not get columns: %w", err)
	}
	defer rows.Close()

	currentColumns := make(map[string]struct{})
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return fmt.Errorf("could not scan column name: %w", err)
		}
		currentColumns[columnName] = struct{}{}
	}

	for k := range currentColumns {
		if _, ok := (*table.Columns)[k]; ok {
			continue
		}

		// Undesired column, alter table
		query := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN IF EXISTS "%s"`, table.Name, k)

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "altering table %s in %s with query: %s", table.Name, c.dbName, query)
		}

		if _, err := c.dbClient.Exec(query); err != nil {
			return fmt.Errorf("could not drop column: %w", err)
		}

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "altered table %s in %s", table.Name, c.dbName)
		}
	}

	return nil
}

func (c *Client) GetPrimaryKeyColumnName(table *Table) (string, error) {
	if table == nil {
		return "", fmt.Errorf("unable to get primary key column: table cannot be nil")
	}

	query := `
			SELECT kcu.column_name 
			FROM information_schema.table_constraints tco
			JOIN information_schema.key_column_usage kcu 
					ON kcu.constraint_name = tco.constraint_name
					AND kcu.table_name = tco.table_name
					AND kcu.table_schema = tco.table_schema
			WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1;
	`

	row := c.dbClient.QueryRow(query, table.Name)
	var columnName string
	err := row.Scan(&columnName)
	if err != nil {
		if err == sql.ErrNoRows {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "no primary key column found for table %s in %s", table.Name, c.dbName)
			}
			return "", nil
		}
		return "", fmt.Errorf("could not get primary key column name: %w", err)
	}

	return columnName, nil
}

func (c *Client) CreateView(viewName, query string, replace bool) error {
	if viewName == "" {
		return fmt.Errorf("unable to create view: view name cannot be empty")
	}

	if query == "" {
		return fmt.Errorf("unable to create view: query cannot be empty")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating view %s in %s", viewName, c.dbName)
	}

	if replace {
		query = fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", viewName, query)
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS %s", viewName, query)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating view %s in %s with query: %s", viewName, c.dbName, query)
	}

	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not create view: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created view %s in %s", viewName, c.dbName)
	}

	return nil
}

func (c *Client) DropView(viewName string) error {
	if viewName == "" {
		return fmt.Errorf("unable to drop view: view name cannot be empty")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping view %s in %s", viewName, c.dbName)
	}

	query := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping view %s in %s with query: %s", viewName, c.dbName, query)
	}

	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not drop view: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped view %s in %s", viewName, c.dbName)
	}

	return nil
}

func (c *Client) GetPrimaryKeyConstraintName(table *Table) (string, error) {
	if table == nil {
		return "", fmt.Errorf("unable to get primary key constraint: table cannot be nil")
	}

	query := `
			SELECT conname 
			FROM pg_constraint 
			INNER JOIN pg_class ON conrelid=pg_class.oid 
			INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace 
			WHERE nspname = current_schema() AND relname=$1 AND contype='p';
	`

	row := c.dbClient.QueryRow(query, table.Name)
	var constraintName string
	err := row.Scan(&constraintName)
	if err != nil {
		if err == sql.ErrNoRows {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "no primary key constraint found for table %s in %s", table.Name, c.dbName)
			}
			return "", nil
		}
		return "", fmt.Errorf("could not get primary key constraint: %w", err)
	}

	return constraintName, nil
}

func (c *Client) ChangeColumnToNullable(tableName string, columnName string) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "attempting to change column %s to nullable for table %s in %s", columnName, tableName, c.dbName)
	}

	query := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;`, tableName, columnName)

	_, err := c.dbClient.Exec(query)
	if err != nil {
		return fmt.Errorf("could not change column to nullable: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "changed column %s to nullable for table %s in %s", columnName, tableName, c.dbName)
	}

	return nil
}

func (c *Client) ChangeColumnToNonNullable(tableName string, columnName string) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "attempting to change column %s to non-nullable for table %s in %s", columnName, tableName, c.dbName)
	}

	query := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;`, tableName, columnName)

	_, err := c.dbClient.Exec(query)
	// TODO: handle default values per data type from null to non-null
	if err != nil {
		return fmt.Errorf("could not change column to non-nullable: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "changed column %s to non-nullable for table %s in %s", columnName, tableName, c.dbName)
	}
	return nil
}

func (c *Client) ChangePrimaryKey(table *Table) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "attempting to change primary key for table %s in %s", table.Name, c.dbName)
	}

	if table == nil {
		return fmt.Errorf("unable to change primary key: table cannot be nil")
	}

	// Get current primary key
	constraintName, err := c.GetPrimaryKeyConstraintName(table)
	if err != nil {
		return fmt.Errorf("could not get primary key constraint name: %w", err)
	}

	if constraintName != "" {
		// Drop current primary key
		query := fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s`, table.Name, constraintName)

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "dropping primary key for table %s in %s with query: %s", table.Name, c.dbName, query)
		}

		if _, err := c.dbClient.Exec(query); err != nil {
			return fmt.Errorf("could not drop primary key: %w", err)
		}
	}

	query := fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY (%s)`, table.Name, table.PrimaryKeyName)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating primary key for table %s in %s with query: %s", table.Name, c.dbName, query)
	}

	if _, err := c.dbClient.Exec(query); err != nil {
		return fmt.Errorf("could not create primary key: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "changed primary key for table %s in %s", table.Name, c.dbName)
	}

	return nil
}

func (c *Client) CreateIndex(table *Table, cols *[]Column, opts ...func(*indexOpts)) error {
	o := indexOpts{
		sortDesc: []Column{},
		recreate: false,
		unique:   false,
		typ:      IndexTypeBTREE,
	}
	for _, opt := range opts {
		opt(&o)
	}

	indexName, err := createIndexName(table, cols)
	if err != nil {
		return fmt.Errorf("could not create index name: %w", err)
	}

	// Check if index already exists
	exists, err := c.IsIndexExistent(table, cols)
	if err != nil {
		return fmt.Errorf("could not check if index exists: %w", err)
	}

	if exists && !o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "index %s already exists in %s for table %s, option to recreate not specified, skipping", indexName, c.dbName, table.Name)
		}
		return nil
	}

	if exists && o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "recreating index %s in %s for table %s", indexName, c.dbName, table.Name)
		}
		if err = c.DropIndex(table, cols); err != nil {
			return fmt.Errorf("could not drop index: %w", err)
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating index %s", indexName)
	}

	columnNames := []string{}
	for _, col := range *cols {
		columnNames = append(columnNames, col.Name)
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s USING %s (%s);",
		func() string {
			if o.unique {
				return "UNIQUE "
			}
			return ""
		}(),
		indexName,
		table.Name,
		o.typ,
		func() string {
			if len(o.sortDesc) > 0 {
				sortDescColNames := []string{}
				for _, col := range o.sortDesc {
					sortDescColNames = append(sortDescColNames, col.Name)
				}

				for i, colName := range columnNames {
					if slices.Contains(sortDescColNames, colName) {
						columnNames[i] += " DESC"
					}
				}
			}
			return strings.Join(columnNames, ", ")
		}(),
	)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating index %s in %s for table %s with query: %s", indexName, c.dbName, table.Name, query)
	}

	_, err = c.dbClient.Exec(query)
	if err != nil {
		return fmt.Errorf("could not create index: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created index %s in %s for table %s", indexName, c.dbName, table.Name)
	}

	return nil
}

func (c *Client) GetTableNames() ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
	`

	rows, err := c.dbClient.Query(query)
	if err != nil {
		return nil, err
	}

	tableNames := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}

	return tableNames, nil
}

func (c *Client) DeleteRows(rows ...Row) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to delete")
	}

	for _, row := range rows {
		if row == nil {
			return fmt.Errorf("row cannot be nil")
		}

		table := row.Table()

		// Check if table exists
		exists, err := c.IsTableExistent(table)
		if err != nil {
			return fmt.Errorf("failed to delete: could not check if table %s exists: %w", table.Name, err)
		}
		if !exists {
			return fmt.Errorf("failed to delete: table %s does not exist", table.Name)
		}

		// Check if schema is aligned
		aligned, err := c.IsSchemaAligned(table)
		if err != nil {
			return fmt.Errorf("failed to delete: could not check if schema is aligned: %w", err)
		}
		if !aligned {
			return fmt.Errorf("failed to delete: schema is not aligned")
		}

		var pk string

		cols := row.Columns()
		colNames := utils.GetKeysFromMap(*cols)
		values := []interface{}{}
		for _, colName := range colNames {
			col := (*cols)[colName]
			if col.PrimaryKey {
				pk = col.Name
				values = append(values, col.Value)
			}
		}

		if pk == "" {
			return fmt.Errorf("failed to delete row (%s) in table (%s): no primary key found", strings.Join(colNames, ", "), table.Name)
		}

		query := fmt.Sprintf(
			"DELETE FROM %s WHERE %s = $1",
			table.Name,
			pk,
		)

		_, err = c.dbClient.Exec(query, values...)
		if err != nil {
			return fmt.Errorf("failed to delete row (%s) in table (%s): %w", strings.Join(colNames, ", "), table.Name, err)
		}
	}

	return nil
}

func (c *Client) GetPrimaryKeyConstraint(tableName string) (string, error) {
	query := `
			SELECT conname 
			FROM pg_constraint 
			INNER JOIN pg_class ON conrelid=pg_class.oid 
			INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace 
			WHERE nspname = current_schema() AND relname=$1 AND contype='p';
	`

	row := c.dbClient.QueryRow(query, tableName)
	var constraintName string
	err := row.Scan(&constraintName)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no primary key constraint found for table %s", tableName)
		}
		return "", fmt.Errorf("could not get primary key constraint: %w", err)
	}

	return constraintName, nil
}

func (c *Client) IsSchemaAligned(table *Table, opts ...func(*alignTableOpts)) (bool, error) {
	o := alignTableOpts{
		nullAlignment: false,
	}

	for _, opt := range opts {
		opt(&o)
	}

	if table == nil {
		return false, fmt.Errorf("unable to check schema alignment: table cannot be nil")
	}

	query := `
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = $1
	`
	rows, err := c.dbClient.Query(query, table.Name)
	if err != nil {
		return false, fmt.Errorf("schema alignment check error: %w", err)
	}
	defer rows.Close()

	existentColumns := make(map[string]struct{})
	aligned := true
	for rows.Next() {
		var columnName, dataType, isNullable, columnDefault sql.NullString
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err != nil {
			return false, fmt.Errorf("schema alignment check error: %w", err)
		}

		if !columnName.Valid || !dataType.Valid || !isNullable.Valid {
			return false, fmt.Errorf("schema alignment check error: query scan returned invalid values for %s", columnName.String)
		}

		col, exists := (*table.Columns)[columnName.String]
		if !exists {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema is not aligned for input table %s in %s: column %s does not exist in runtime", table.Name, c.dbName, columnName.String)
			}
			aligned = false
		}
		existentColumns[columnName.String] = struct{}{}

		if exists {
			convertedType, err := goTypeToPostgresType(col.Type)
			if err != nil {
				return false, fmt.Errorf("schema alignment check error upon converting Go type to PG type: %w", err)
			}
			if !strings.EqualFold(convertedType, dataType.String) {
				if strings.EqualFold(dataType.String, "array") && strings.HasSuffix(convertedType, "[]") {
					// do nothing
				} else {
					if c.verboseMode {
						log.Log.Debugf(c.ctx, "schema alignment check error: column %s type mismatch; have %s in db, %s (%s Go type) in runtime", col.Name, dataType.String, convertedType, col.Type.String())
					}
					aligned = false
				}
			}
			if !strings.EqualFold(utils.BoolToYesNo(col.Nullable), isNullable.String) {
				if c.verboseMode {
					log.Log.Debugf(c.ctx, "schema alignment check error: column %s nullable mismatch; have %s in db, %s in runtime", col.Name, isNullable.String, utils.BoolToYesNo(col.Nullable))
				}
				if o.nullAlignment {
					if isNullable.String == "YES" && !col.Nullable {
						if err = c.ChangeColumnToNonNullable(table.Name, col.Name); err != nil {
							return false, fmt.Errorf("schema alignment check error: %w", err)
						}
					} else if isNullable.String == "NO" && col.Nullable {
						if err = c.ChangeColumnToNullable(table.Name, col.Name); err != nil {
							return false, fmt.Errorf("schema alignment check error: %w", err)
						}
					}
				} else {
					aligned = false
				}
			}
		}
	}

	// check input table columns
	for _, col := range *table.Columns {
		if _, exists := existentColumns[col.Name]; exists {
			continue
		} else {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema is not aligned for input table %s in %s: column %s does not exist in db", table.Name, c.dbName, col.Name)
			}
			aligned = false
		}
	}

	return aligned, nil
}

func (c *Client) CreateTable(table *Table, opts ...func(*createTableOpts)) error {
	if table == nil {
		return fmt.Errorf("unable to create table: table cannot be nil")
	}

	createOpts := createTableOpts{
		overwrite: false,
	}

	// Check if table already exists
	exists, err := c.IsTableExistent(table)
	if err != nil {
		return err
	}

	if exists && !createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s already exists in database %s, overwrite option is false, skipping", table.Name, c.dbName)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating table %s in database %s", table.Name, c.dbName)
	}

	for _, opt := range opts {
		opt(&createOpts)
	}

	if createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "overwrite mode enabled; dropping table %s in %s", table.Name, c.dbName)
		}
		err := c.DropTable(table.Name)
		if err != nil {
			return err
		}
	}

	columnNames := []string{}
	for _, column := range *table.Columns {
		pgType, err := goTypeToPostgresType(column.Type)
		if err != nil {
			return err
		}
		pgCol := fmt.Sprintf(`"%s" %s`, column.Name, pgType)
		if column.PrimaryKey {
			pgCol += " PRIMARY KEY"
		}
		columnNames = append(columnNames, pgCol)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table.Name, strings.Join(columnNames, ", "))

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating table %s in %s with query: %s", table.Name, c.dbName, query)
	}

	_, err = c.dbClient.ExecContext(c.ctx, query)
	if err != nil {
		return err
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created table %s in %s", table.Name, c.dbName)
	}
	return nil
}

func (c *Client) DropIndex(table *Table, cols *[]Column) error {
	if table == nil || table.Name == "" {
		return fmt.Errorf("unable to drop index: table and table.Name cannot be zero values")
	}

	if len(*cols) == 0 {
		return fmt.Errorf("unable to drop index: no columns to index provided")
	}

	indexName, err := createIndexName(table, cols)
	if err != nil {
		return fmt.Errorf("could not create index name: %w", err)
	}

	query := fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping index %s in %s for table %s", indexName, c.dbName, table.Name)
	}

	_, err = c.dbClient.ExecContext(c.ctx, query)
	if err != nil {
		return err
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped index %s in %s for table %s", indexName, c.dbName, table.Name)
	}

	return nil
}

func (c *Client) DropTable(tableName string) error {
	_, err := c.dbClient.ExecContext(c.ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return err
	}
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped table %s in %s", tableName, c.dbName)
	}
	return nil
}

func (c *Client) IsIndexExistent(table *Table, cols *[]Column) (bool, error) {
	if table == nil || table.Name == "" {
		return false, fmt.Errorf("unable to check if index exists: table and table.Name cannot be zero values")
	}

	if len(*cols) == 0 {
		return false, fmt.Errorf("unable to check if index exists: no columns to index provided")
	}

	indexName, err := createIndexName(table, cols)
	if err != nil {
		return false, fmt.Errorf("could not create index name: %w", err)
	}

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE tablename = $1 AND indexname = $2
		)
	`

	var exists bool
	err = c.dbClient.QueryRow(query, table.Name, indexName).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Client) GrantTablePermissions(
	tableName string,
	user string,
	permissions []string,
) error {
	if tableName == "" {
		return fmt.Errorf("unable to grant permissions: table name cannot be empty")
	}
	if user == "" {
		return fmt.Errorf("unable to grant permissions: user cannot be empty")
	}
	if len(permissions) == 0 {
		return fmt.Errorf("unable to grant permissions: no permissions specified")
	}

	// Validate permissions
	validPerms := map[string]bool{
		"SELECT":     true,
		"INSERT":     true,
		"UPDATE":     true,
		"DELETE":     true,
		"TRUNCATE":   true,
		"REFERENCES": true,
		"TRIGGER":    true,
		"ALL":        true,
	}

	for _, perm := range permissions {
		if !validPerms[strings.ToUpper(perm)] {
			return fmt.Errorf("invalid permission: %s", perm)
		}
	}

	query := fmt.Sprintf(
		"GRANT %s ON TABLE %s TO %s",
		strings.Join(permissions, ", "),
		tableName,
		user,
	)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "granting permissions on table %s to user %s with query: %s", tableName, user, query)
	}

	_, err := c.dbClient.Exec(query)
	if err != nil {
		return fmt.Errorf("could not grant permissions: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "granted permissions %v on table %s to user %s", permissions, tableName, user)
	}

	return nil
}

func (c *Client) IsTableExistent(table *Table) (bool, error) {
	if table == nil {
		return false, fmt.Errorf("unable to check if table exists: table cannot be nil")
	}

	var exists bool
	err := c.dbClient.QueryRow("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1)", table.Name).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (c *Client) LeftJoin(leftTable *Table, rightTable *Table, joinColumns []JoinColumnPair) (*sql.Rows, *JoinResultantColumns, error) {
	if leftTable == nil || rightTable == nil {
		return nil, nil, fmt.Errorf("unable to left join: tables cannot be nil")
	}

	for _, pair := range joinColumns {
		if pair.LeftColumnName == "" || pair.RightColumnName == "" {
			return nil, nil, fmt.Errorf("unable to left join: join column pair names cannot be empty")
		}
	}

	// Check if tables exist and schema align
	for _, t := range []*Table{leftTable, rightTable} {
		exists, err := c.IsTableExistent(t)
		if err != nil {
			return nil, nil, fmt.Errorf("could not check if table %s exists: %w", t.Name, err)
		}
		if !exists {
			return nil, nil, fmt.Errorf("table %s does not exist", t.Name)
		}

		aligned, err := c.IsSchemaAligned(t)
		if err != nil {
			return nil, nil, fmt.Errorf("could not check if schema is aligned: %w", err)
		}
		if !aligned {
			return nil, nil, fmt.Errorf("schema is not aligned for table %s", t.Name)
		}
	}

	// Get columns for tables
	columns := &JoinResultantColumns{
		Columns: make(map[string]reflect.Type),
	}
	for i, t := range []*Table{leftTable, rightTable} {
		rows, err := c.dbClient.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`, t.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get columns: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var columnName, dataType string

			if err := rows.Scan(&columnName, &dataType); err != nil {
				return nil, nil, fmt.Errorf("could not scan column name: %w", err)
			}

			// check if column name already exists
			if i == 1 {
				if _, ok := columns.Columns[columnName]; ok {
					columnName = fmt.Sprintf("%s.%s", t.Name, columnName)
				}
			}

			goType, err := postgresTypeToGoType(dataType)
			if err != nil {
				return nil, nil, fmt.Errorf("could not convert postgres type to Go type: %w", err)
			}

			columns.Columns[columnName] = goType
		}
	}

	// Create join query
	query := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON ",
		leftTable.Name,
		rightTable.Name,
	)

	for i, pair := range joinColumns {
		if i > 0 {
			query += " AND "
		}

		query += fmt.Sprintf("%s.%s = %s.%s", leftTable.Name, pair.LeftColumnName, rightTable.Name, pair.RightColumnName)
	}

	rows, err := c.dbClient.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("could not execute join query: %w", err)
	}

	return rows, columns, nil
}

func (c *Client) UpsertRows(rows ...Row) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to upsert")
	}

	for _, row := range rows {
		if row == nil {
			return fmt.Errorf("row cannot be nil")
		}

		table := row.Table()

		// Check if table exists
		exists, err := c.IsTableExistent(table)
		if err != nil {
			return fmt.Errorf("failed to upsert: could not check if table %s exists: %w", table.Name, err)
		}
		if !exists {
			return fmt.Errorf("failed to upsert: table %s does not exist", table.Name)
		}

		// Check if schema is aligned
		aligned, err := c.IsSchemaAligned(table)
		if err != nil {
			return fmt.Errorf("failed to upsert: could not check if schema is aligned: %w", err)
		}
		if !aligned {
			return fmt.Errorf("failed to upsert: schema is not aligned")
		}

		var pk string

		cols := row.Columns()
		colNames := utils.GetKeysFromMap(*cols)
		excludedCols := make([]string, len(colNames))
		for i, col := range colNames {
			excludedCols[i] = fmt.Sprintf("EXCLUDED.%s", col)
		}
		values := []interface{}{}
		placeholders := make([]string, len(colNames))
		for i, colName := range colNames {
			col := (*cols)[colName]
			placeholders[i] = fmt.Sprintf("$%d", i+1)

			// general value handling
			switch col.Type.String() {
			case "[]string":
				val := pq.StringArray(col.Value.([]string))
				values = append(values, val)
			case "[]int":
				converted := make([]int64, len(col.Value.([]int)))
				for _, v := range col.Value.([]int) {
					converted = append(converted, int64(v))
				}
				val := pq.Int64Array(converted)
				values = append(values, val)
			case "[]int32":
				val := pq.Int32Array(col.Value.([]int32))
				values = append(values, val)
			case "[]int64":
				val := pq.Int64Array(col.Value.([]int64))
				values = append(values, val)
			case "[]float32":
				val := pq.Float32Array(col.Value.([]float32))
				values = append(values, val)
			case "[]float64":
				val := pq.Float64Array(col.Value.([]float64))
				values = append(values, val)
			case "[]bool":
				val := pq.BoolArray(col.Value.([]bool))
				values = append(values, val)
			case "[]time.Time":
				val := pq.Array(col.Value.([]time.Time))
				values = append(values, val)
			default:
				values = append(values, col.Value)
			}

			if col.PrimaryKey {
				pk = col.Name
			}
		}

		if pk == "" {
			return fmt.Errorf("failed to upsert row (%s) in table (%s): no primary key found", strings.Join(colNames, ", "), table.Name)
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET (%s) = (%s)",
			table.Name,
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
			pk,
			strings.Join(colNames, ", "),
			strings.Join(excludedCols, ", "),
		)

		_, err = c.dbClient.Exec(query, values...)
		if err != nil {
			return fmt.Errorf("failed to upsert row (%s) in table (%s) with query %s: %w", strings.Join(colNames, ", "), table.Name, query, err)
		}
	}

	return nil
}

func (c *Client) ViewExists(viewName string) (bool, error) {
	if viewName == "" {
		return false, fmt.Errorf("unable to check if view exists: view name cannot be empty")
	}

	var exists bool
	err := c.dbClient.QueryRow("SELECT EXISTS (SELECT true FROM information_schema.views WHERE table_name = $1 AND table_schema = 'public')", viewName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ########################################
// # Options
// ########################################

type alignTableOpts struct {
	nullAlignment bool
}

func WithAlignTableNullability(v bool) func(*alignTableOpts) {
	return func(opts *alignTableOpts) {
		opts.nullAlignment = v
	}
}

type createTableOpts struct {
	overwrite bool
}

func WithCreateTableOverwrite(v bool) func(*createTableOpts) {
	return func(opts *createTableOpts) {
		opts.overwrite = v
	}
}

type indexOpts struct {
	sortDesc []Column
	unique   bool
	recreate bool
	typ      indexType
}

// Specify descending sorting for certain columns. Columns are sorted in ascending order by default.
func WithSortDescending(cols []Column) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.sortDesc = cols
	}
}

func WithUniqueConstraint() func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.unique = true
	}
}

func WithRecreateIndex() func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.recreate = true
	}
}

func WithIndexType(typ indexType) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = typ
	}
}
