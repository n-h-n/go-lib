package bigquery

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

// rowValueSaver implements bigquery.ValueSaver for Row interface
type rowValueSaver struct {
	row Row
}

// Save implements bigquery.ValueSaver interface
func (r *rowValueSaver) Save() (map[string]bigquery.Value, string, error) {
	cols := r.row.Columns()
	values := make(map[string]bigquery.Value)

	for colName, col := range *cols {
		// Use default value if column value is nil
		value := col.Value
		if value == nil {
			// Convert BigQuery type to Go type first
			if goType, err := bigQueryTypeToGoType(col.Type); err == nil {
				if defaultValue, err := bigQueryDefaultValue(goType); err == nil {
					value = defaultValue
				}
			}
		}
		values[colName] = value
	}

	return values, "", nil
}

// CreateTable creates a BigQuery table
func (c *Client) CreateTable(table *Table, opts ...func(*createTableOpts)) error {
	if table == nil {
		return fmt.Errorf("unable to create table: table cannot be nil")
	}

	// Validate table name
	if err := validateTableName(table.Name); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	createOpts := createTableOpts{
		overwrite: false,
	}

	for _, opt := range opts {
		opt(&createOpts)
	}

	// Check if table already exists
	exists, err := c.IsTableExistent(table)
	if err != nil {
		return err
	}

	if exists && !createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s already exists in dataset %s, overwrite option is false, skipping", table.Name, c.datasetID)
		}
		return nil
	}

	if exists && createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "overwrite mode enabled; dropping table %s in %s", table.Name, c.datasetID)
		}
		err := c.DropTable(table.Name)
		if err != nil {
			return err
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating table %s in dataset %s", table.Name, c.datasetID)
	}

	// Convert to BigQuery schema
	schema, err := convertToBigQuerySchema(table)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	// Create table metadata
	tableMetadata := &bigquery.TableMetadata{
		Name:        table.Name,
		Description: table.Description,
		Schema:      convertToBigQuerySchemaFields(schema),
		Labels:      table.Labels,
	}

	// Apply partitioning if specified
	if createOpts.timePartitioning != nil {
		tableMetadata.TimePartitioning = &bigquery.TimePartitioning{
			Type:       bigquery.TimePartitioningType(createOpts.timePartitioning.Type),
			Field:      createOpts.timePartitioning.Field,
			Expiration: time.Duration(createOpts.timePartitioning.ExpirationMs) * time.Millisecond,
		}
	}

	// Apply clustering if specified
	if createOpts.clustering != nil && len(createOpts.clustering.Fields) > 0 {
		tableMetadata.Clustering = &bigquery.Clustering{
			Fields: createOpts.clustering.Fields,
		}
	}

	// Create the table
	tableRef := c.GetTableReference(table.Name)
	err = tableRef.Create(c.ctx, tableMetadata)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created table %s in dataset %s", table.Name, c.datasetID)
	}

	return nil
}

// DropTable drops a BigQuery table
func (c *Client) DropTable(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	tableRef := c.GetTableReference(tableName)
	err := tableRef.Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped table %s in dataset %s", tableName, c.datasetID)
	}

	return nil
}

// IsTableExistent checks if a table exists
func (c *Client) IsTableExistent(table *Table) (bool, error) {
	if table == nil {
		return false, fmt.Errorf("unable to check if table exists: table cannot be nil")
	}

	tableRef := c.GetTableReference(table.Name)
	_, err := tableRef.Metadata(c.ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// GetTableNames returns a list of table names in the dataset
func (c *Client) GetTableNames() ([]string, error) {
	datasetRef := c.GetDatasetReference()
	it := datasetRef.Tables(c.ctx)

	var tableNames []string
	for {
		table, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		if table == nil {
			break
		}
		tableNames = append(tableNames, table.TableID)
	}

	return tableNames, nil
}

// GetTableSchema returns the schema of a table
func (c *Client) GetTableSchema(tableName string) (*Schema, error) {
	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	tableRef := c.GetTableReference(tableName)
	metadata, err := tableRef.Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	schema := &Schema{
		Fields: make([]*SchemaField, len(metadata.Schema)),
	}

	for i, field := range metadata.Schema {
		mode := "NULLABLE"
		if field.Repeated {
			mode = "REPEATED"
		} else if field.Required {
			mode = "REQUIRED"
		}

		schema.Fields[i] = &SchemaField{
			Name:        field.Name,
			Type:        bigqueryType(field.Type),
			Mode:        mode,
			Description: field.Description,
		}
	}

	return schema, nil
}

// AlignTableSchema aligns the table schema with the provided table definition
func (c *Client) AlignTableSchema(table *Table, opts ...func(*alignTableOpts)) error {
	if table == nil {
		return fmt.Errorf("unable to align table schema: table cannot be nil")
	}

	o := alignTableOpts{
		nullAlignment:   false,
		allowRecreation: false, // Default to false for safety
	}

	for _, opt := range opts {
		opt(&o)
	}

	// Check if table exists
	exists, err := c.IsTableExistent(table)
	if err != nil {
		return fmt.Errorf("could not check if table exists: %w", err)
	}
	if !exists {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s does not exist in %s, creating....", table.Name, c.datasetID)
		}
		if err = c.CreateTable(table); err != nil {
			return fmt.Errorf("could not create table: %w", err)
		}
		return nil
	}

	// Check if schema is aligned
	aligned, err := c.IsSchemaAligned(table, opts...)
	if err != nil {
		return fmt.Errorf("could not check if schema is aligned: %w", err)
	}

	if !aligned {
		// Check if we need to recreate the table (column removal/type changes)
		needsRecreation, err := c.needsTableRecreation(table)
		if err != nil {
			return fmt.Errorf("could not check if table needs recreation: %w", err)
		}

		if needsRecreation {
			if !o.allowRecreation {
				return fmt.Errorf("table %s requires recreation (column removal or type changes detected) but recreation is not allowed. Use WithAlignTableRecreation(true) to enable", table.Name)
			}

			if c.verboseMode {
				log.Log.Debugf(c.ctx, "table %s needs recreation due to column removal or type changes", table.Name)
			}
			// Recreate the table with new schema
			if err = c.CreateTable(table, WithCreateTableOverwrite(true)); err != nil {
				return fmt.Errorf("could not recreate table: %w", err)
			}
		} else {
			// Only adding new columns
			if err = c.AlterTableAddColumns(table); err != nil {
				return fmt.Errorf("could not add columns: %w", err)
			}
		}
	}

	return nil
}

// IsSchemaAligned checks if the table schema is aligned with the provided table definition
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

	// Get current schema
	currentSchema, err := c.GetTableSchema(table.Name)
	if err != nil {
		return false, fmt.Errorf("schema alignment check error: %w", err)
	}

	// Convert current schema to our table format
	currentTable, err := convertFromBigQuerySchema(currentSchema)
	if err != nil {
		return false, fmt.Errorf("schema alignment check error: %w", err)
	}

	// Check if all columns exist and match
	aligned := true
	for colName, expectedCol := range *table.Columns {
		currentCol, exists := (*currentTable.Columns)[colName]
		if !exists {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema is not aligned for input table %s in %s: column %s does not exist in runtime", table.Name, c.datasetID, colName)
			}
			aligned = false
			continue
		}

		// Check type alignment (normalize types for comparison)
		if normalizeBigQueryType(expectedCol.Type) != normalizeBigQueryType(currentCol.Type) {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema alignment check error: column %s type mismatch; have %s in db, %s in runtime", colName, currentCol.Type, expectedCol.Type)
			}
			aligned = false
		}

		// Check mode alignment
		if expectedCol.Mode != currentCol.Mode {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema alignment check error: column %s mode mismatch; have %s in db, %s in runtime", colName, currentCol.Mode, expectedCol.Mode)
			}
			aligned = false
		}
	}

	// Check for extra columns in current schema
	for colName := range *currentTable.Columns {
		if _, exists := (*table.Columns)[colName]; !exists {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "schema is not aligned for input table %s in %s: column %s exists in db but not in runtime", table.Name, c.datasetID, colName)
			}
			aligned = false
		}
	}

	return aligned, nil
}

// needsTableRecreation checks if the table needs to be recreated due to column removal or type changes
func (c *Client) needsTableRecreation(table *Table) (bool, error) {
	// Get current schema
	currentSchema, err := c.GetTableSchema(table.Name)
	if err != nil {
		return false, fmt.Errorf("could not get current schema: %w", err)
	}

	// Convert current schema to our table format
	currentTable, err := convertFromBigQuerySchema(currentSchema)
	if err != nil {
		return false, fmt.Errorf("could not convert current schema: %w", err)
	}

	// Check if any current columns are missing in the new schema (column removal)
	for colName := range *currentTable.Columns {
		if _, exists := (*table.Columns)[colName]; !exists {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "column %s exists in current table but not in new schema - recreation needed", colName)
			}
			return true, nil
		}
	}

	// Check if any existing columns have type changes
	for colName, newCol := range *table.Columns {
		if currentCol, exists := (*currentTable.Columns)[colName]; exists {
			if currentCol.Type != newCol.Type {
				if c.verboseMode {
					log.Log.Debugf(c.ctx, "column %s type changed from %s to %s - recreation needed", colName, currentCol.Type, newCol.Type)
				}
				return true, nil
			}
			// Check mode changes that require recreation (e.g., REQUIRED to NULLABLE)
			if currentCol.Mode != newCol.Mode {
				if c.verboseMode {
					log.Log.Debugf(c.ctx, "column %s mode changed from %s to %s - recreation needed", colName, currentCol.Mode, newCol.Mode)
				}
				return true, nil
			}
		}
	}

	return false, nil
}

// AlterTableAddColumns adds columns to an existing table
func (c *Client) AlterTableAddColumns(table *Table) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "altering table %s in %s", table.Name, c.datasetID)
	}

	if table == nil {
		return fmt.Errorf("unable to alter table: table cannot be nil")
	}

	// Get current schema
	currentSchema, err := c.GetTableSchema(table.Name)
	if err != nil {
		return fmt.Errorf("could not get current schema: %w", err)
	}

	// Find new columns to add
	currentColumns := make(map[string]bool)
	for _, field := range currentSchema.Fields {
		currentColumns[field.Name] = true
	}

	// Build complete schema: existing fields + new fields
	var allFields []*bigquery.FieldSchema

	// Add existing fields first
	for _, field := range currentSchema.Fields {
		existingField := &bigquery.FieldSchema{
			Name:        field.Name,
			Type:        bigquery.FieldType(field.Type),
			Repeated:    field.Mode == "REPEATED",
			Required:    field.Mode == "REQUIRED",
			Description: field.Description,
		}
		allFields = append(allFields, existingField)
	}

	// Add new fields
	var newFieldsCount int
	for _, column := range *table.Columns {
		if !currentColumns[column.Name] {
			// New column, add to schema
			field := &bigquery.FieldSchema{
				Name:        column.Name,
				Type:        bigquery.FieldType(column.Type),
				Repeated:    column.Mode == "REPEATED",
				Required:    column.Mode == "REQUIRED",
				Description: column.Description,
			}
			allFields = append(allFields, field)
			newFieldsCount++
		}
	}

	if newFieldsCount == 0 {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "no new columns to add to table %s", table.Name)
		}
		return nil
	}

	// Update table schema with complete schema
	tableRef := c.GetTableReference(table.Name)
	update := bigquery.TableMetadataToUpdate{
		Schema: allFields,
	}

	_, err = tableRef.Update(c.ctx, update, "")
	if err != nil {
		return fmt.Errorf("could not add columns: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "altered table %s in %s", table.Name, c.datasetID)
	}

	return nil
}

// UpsertRows upserts rows into a BigQuery table
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

		// Insert data using ValueSaver wrapper
		tableRef := c.GetTableReference(table.Name)
		inserter := tableRef.Inserter()

		if c.verboseMode {
			// Debug: show formatted values being inserted
			cols := row.Columns()
			var formattedValues []string
			for colName, col := range *cols {
				formattedValues = append(formattedValues, fmt.Sprintf("%s=%s", colName, formatBigQueryValue(col.Value)))
			}
			log.Log.Debugf(c.ctx, "upserting row in table %s: %s", table.Name, strings.Join(formattedValues, ", "))
		}

		// Create a ValueSaver wrapper for the row
		rowSaver := &rowValueSaver{row: row}
		err = inserter.Put(c.ctx, rowSaver)
		if err != nil {
			return fmt.Errorf("failed to upsert row in table %s: %w", table.Name, err)
		}
	}

	return nil
}

// DeleteRows deletes rows from a BigQuery table
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

		// Build DELETE query
		cols := row.Columns()
		colNames := utils.GetKeysFromMap(*cols)
		whereClause := ""
		params := make(map[string]interface{})

		for _, colName := range colNames {
			col := (*cols)[colName]
			if col.Mode == "REQUIRED" { // Primary key equivalent
				if whereClause != "" {
					whereClause += " AND "
				}
				whereClause += fmt.Sprintf("%s = @%s", colName, colName)
				params[colName] = col.Value
			}
		}

		if whereClause == "" {
			return fmt.Errorf("failed to delete row (%s) in table (%s): no primary key found", strings.Join(colNames, ", "), table.Name)
		}

		query := fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE %s", c.projectID, c.datasetID, table.Name, whereClause)

		if c.verboseMode {
			// Debug: show formatted parameter values
			var formattedParams []string
			for paramName, paramValue := range params {
				formattedParams = append(formattedParams, fmt.Sprintf("@%s=%s", paramName, formatBigQueryValue(paramValue)))
			}
			log.Log.Debugf(c.ctx, "deleting row from table %s with params: %s", table.Name, strings.Join(formattedParams, ", "))
		}

		// Execute DELETE query with parameters
		_, err = c.QueryWithParams(query, params)
		if err != nil {
			return fmt.Errorf("failed to delete row (%s) in table (%s): %w", strings.Join(colNames, ", "), table.Name, err)
		}
	}

	return nil
}

// Query executes a SELECT query and returns results
func (c *Client) Query(query string) (*bigquery.RowIterator, error) {
	return c.ExecuteQuery(c.ctx, query)
}

// QueryWithParams executes a parameterized query
func (c *Client) QueryWithParams(query string, params map[string]interface{}) (*bigquery.RowIterator, error) {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "executing parameterized query: %s", query)
		// Debug: show formatted parameter values
		var formattedParams []string
		for paramName, paramValue := range params {
			formattedParams = append(formattedParams, fmt.Sprintf("@%s=%s", paramName, formatBigQueryValue(paramValue)))
		}
		log.Log.Debugf(c.ctx, "query parameters: %s", strings.Join(formattedParams, ", "))
	}

	q := c.client.Query(query)
	q.UseLegacySQL = c.useLegacySQL
	q.DryRun = c.dryRun

	// Set parameters
	for key, value := range params {
		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  key,
			Value: value,
		})
	}

	job, err := q.Run(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	if !c.dryRun {
		err = c.WaitForJob(c.ctx, job)
		if err != nil {
			return nil, err
		}

		it, err := job.Read(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read query results: %w", err)
		}

		return it, nil
	}

	return nil, nil
}

// SelectRows selects rows from a table based on WHERE conditions
func (c *Client) SelectRows(tableName string, opts ...func(*selectRowsOpts)) (*bigquery.RowIterator, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	selectOpts := selectRowsOpts{
		columns: []string{"*"}, // Default to select all columns
		limit:   0,             // No limit by default
	}

	for _, opt := range opts {
		opt(&selectOpts)
	}

	// Check if table exists
	exists, err := c.IsTableExistent(&Table{Name: tableName})
	if err != nil {
		return nil, fmt.Errorf("failed to check if table exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Build SELECT query
	columnsClause := strings.Join(selectOpts.columns, ", ")
	query := fmt.Sprintf("SELECT %s FROM `%s.%s.%s`", columnsClause, c.projectID, c.datasetID, tableName)

	// Add WHERE clause if conditions are provided
	if len(selectOpts.whereConditionsMap) > 0 {
		// Use buildWhereClause for map-based conditions
		whereClause := buildWhereClause(selectOpts.whereConditionsMap)
		query += " " + whereClause
	} else if len(selectOpts.whereConditions) > 0 {
		// Use string-based conditions
		whereClause := strings.Join(selectOpts.whereConditions, " AND ")
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add GROUP BY clause if specified
	if len(selectOpts.groupBy) > 0 {
		groupClause := strings.Join(selectOpts.groupBy, ", ")
		query += fmt.Sprintf(" GROUP BY %s", groupClause)
	}

	// Add HAVING clause if specified
	if len(selectOpts.havingConditions) > 0 {
		havingClause := strings.Join(selectOpts.havingConditions, " AND ")
		query += fmt.Sprintf(" HAVING %s", havingClause)
	}

	// Add WINDOW clause if specified (for window functions with PARTITION BY)
	if len(selectOpts.windowDefinitions) > 0 {
		windowClause := strings.Join(selectOpts.windowDefinitions, ", ")
		query += fmt.Sprintf(" WINDOW %s", windowClause)
	}

	// Add ORDER BY clause if specified
	if len(selectOpts.orderBy) > 0 {
		query += " " + buildOrderByClause(selectOpts.orderBy)
	}

	// Add LIMIT clause if specified
	if selectOpts.limit > 0 {
		query += " " + buildLimitClause(selectOpts.limit)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "executing SELECT query: %s", query)
	}

	// Execute query with parameters if any
	if len(selectOpts.params) > 0 {
		return c.QueryWithParams(query, selectOpts.params)
	}

	return c.Query(query)
}

// LeftJoin performs a LEFT JOIN between two tables
func (c *Client) LeftJoin(leftTable *Table, rightTable *Table, joinColumns []JoinColumnPair) (*bigquery.RowIterator, *JoinResultantColumns, error) {
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
		schema, err := c.GetTableSchema(t.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get schema for table %s: %w", t.Name, err)
		}

		for _, field := range schema.Fields {
			columnName := field.Name

			// Check if column name already exists (for right table)
			if i == 1 {
				if _, ok := columns.Columns[columnName]; ok {
					columnName = fmt.Sprintf("%s_%s", t.Name, field.Name)
				}
			}

			goType, err := bigQueryTypeToGoType(field.Type)
			if err != nil {
				return nil, nil, fmt.Errorf("could not convert BigQuery type %s to Go type: %w", field.Type, err)
			}

			columns.Columns[columnName] = goType
		}
	}

	// Create join query
	query := fmt.Sprintf(
		"SELECT * FROM `%s.%s.%s` LEFT JOIN `%s.%s.%s` ON ",
		c.projectID, c.datasetID, leftTable.Name,
		c.projectID, c.datasetID, rightTable.Name,
	)

	for i, pair := range joinColumns {
		if i > 0 {
			query += " AND "
		}

		query += fmt.Sprintf("`%s.%s.%s`.%s = `%s.%s.%s`.%s",
			c.projectID, c.datasetID, leftTable.Name, escapeIdentifier(pair.LeftColumnName),
			c.projectID, c.datasetID, rightTable.Name, escapeIdentifier(pair.RightColumnName))
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "executing LEFT JOIN query: %s", query)
	}

	// Execute the join query
	it, err := c.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("could not execute join query: %w", err)
	}

	return it, columns, nil
}

// CreateDataset creates a BigQuery dataset
func (c *Client) CreateDataset(datasetID string, opts ...func(*createDatasetOpts)) error {
	if datasetID == "" {
		return fmt.Errorf("dataset ID cannot be empty")
	}

	createOpts := createDatasetOpts{
		overwrite: false,
	}

	for _, opt := range opts {
		opt(&createOpts)
	}

	// Check if dataset already exists
	datasetRef := c.client.Dataset(datasetID)
	_, err := datasetRef.Metadata(c.ctx)
	exists := (err == nil)

	if exists && !createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "dataset %s already exists, overwrite option is false, skipping", datasetID)
		}
		return nil
	}

	if exists && createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "overwrite mode enabled; dropping dataset %s", datasetID)
		}
		err := c.DropDataset(datasetID, true) // Delete with contents
		if err != nil {
			return fmt.Errorf("failed to drop existing dataset: %w", err)
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating dataset %s", datasetID)
	}

	// Create dataset metadata
	datasetMetadata := &bigquery.DatasetMetadata{
		Location:    c.location,
		Labels:      createOpts.labels,
		Description: createOpts.description,
	}

	err = datasetRef.Create(c.ctx, datasetMetadata)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created dataset %s", datasetID)
	}

	return nil
}

// DropDataset drops a BigQuery dataset
func (c *Client) DropDataset(datasetID string, deleteContents bool) error {
	if datasetID == "" {
		return fmt.Errorf("dataset ID cannot be empty")
	}

	datasetRef := c.client.Dataset(datasetID)
	var err error
	if deleteContents {
		err = datasetRef.DeleteWithContents(c.ctx)
	} else {
		err = datasetRef.Delete(c.ctx)
	}
	if err != nil {
		return fmt.Errorf("failed to drop dataset: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped dataset %s", datasetID)
	}

	return nil
}

// CreateView creates a BigQuery view
func (c *Client) CreateView(viewName, query string, replace bool) error {
	if viewName == "" {
		return fmt.Errorf("unable to create view: view name cannot be empty")
	}

	if query == "" {
		return fmt.Errorf("unable to create view: query cannot be empty")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating view %s in %s", viewName, c.datasetID)
	}

	viewRef := c.client.Dataset(c.datasetID).Table(viewName)

	// Check if view already exists
	exists, err := c.ViewExists(viewName)
	if err != nil {
		return fmt.Errorf("failed to check if view exists: %w", err)
	}

	if exists && !replace {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "view %s already exists in %s, replace option is false, skipping", viewName, c.datasetID)
		}
		return nil
	}

	if exists && replace {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "replace mode enabled; dropping view %s in %s", viewName, c.datasetID)
		}
		err := viewRef.Delete(c.ctx)
		if err != nil && !strings.Contains(err.Error(), "notFound") {
			return fmt.Errorf("failed to delete existing view: %w", err)
		}
	}

	// Create view metadata
	viewMetadata := &bigquery.TableMetadata{
		Name:      viewName,
		ViewQuery: query,
	}

	err = viewRef.Create(c.ctx, viewMetadata)
	if err != nil {
		return fmt.Errorf("could not create view: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created view %s in %s", viewName, c.datasetID)
	}

	return nil
}

// DropView drops a BigQuery view
func (c *Client) DropView(viewName string) error {
	if viewName == "" {
		return fmt.Errorf("unable to drop view: view name cannot be empty")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping view %s in %s", viewName, c.datasetID)
	}

	viewRef := c.client.Dataset(c.datasetID).Table(viewName)
	err := viewRef.Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("could not drop view: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped view %s in %s", viewName, c.datasetID)
	}

	return nil
}

// ViewExists checks if a view exists
func (c *Client) ViewExists(viewName string) (bool, error) {
	if viewName == "" {
		return false, fmt.Errorf("unable to check if view exists: view name cannot be empty")
	}

	viewRef := c.client.Dataset(c.datasetID).Table(viewName)
	_, err := viewRef.Metadata(c.ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// CreateIndex creates clustering or partitioning for a BigQuery table
func (c *Client) CreateIndex(table *Table, opts ...func(*indexOpts)) error {
	o := indexOpts{
		recreate:          false,
		typ:               IndexTypeClustering,
		clusteringFields:  []string{},
		timePartitioning:  nil,
		rangePartitioning: nil,
	}

	for _, opt := range opts {
		opt(&o)
	}

	if table == nil || table.Name == "" {
		return fmt.Errorf("unable to create index: table and table.Name cannot be zero values")
	}

	// Validate table name
	if err := validateTableName(table.Name); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Validate that we have something to create
	if o.typ == IndexTypeClustering && len(o.clusteringFields) == 0 {
		return fmt.Errorf("unable to create clustering index: no clustering fields provided")
	}
	if o.typ == IndexTypePartition && o.timePartitioning == nil && o.rangePartitioning == nil {
		return fmt.Errorf("unable to create partition index: no partitioning configuration provided")
	}
	if o.typ == IndexTypeSearch && o.searchIndex == nil {
		return fmt.Errorf("unable to create search index: no search index configuration provided")
	}
	if o.typ == IndexTypeVector && o.vectorIndex == nil {
		return fmt.Errorf("unable to create vector index: no vector index configuration provided")
	}

	// Check if index already exists
	exists, err := c.IsIndexExistent(table, &o)
	if err != nil {
		return fmt.Errorf("could not check if index exists: %w", err)
	}

	if exists && !o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "%s already exists for table %s, option to recreate not specified, skipping", o.typ, table.Name)
		}
		return nil
	}

	if exists && o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "recreating %s for table %s", o.typ, table.Name)
		}
		if err = c.DropIndex(table, &o); err != nil {
			return fmt.Errorf("could not drop index: %w", err)
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating %s for table %s", o.typ, table.Name)
	}

	// Create the index based on type
	switch o.typ {
	case IndexTypeClustering:
		// Handle clustering via table metadata update
		tableRef := c.GetTableReference(table.Name)
		update := bigquery.TableMetadataToUpdate{
			Clustering: &bigquery.Clustering{
				Fields: o.clusteringFields,
			},
		}
		_, err = tableRef.Update(c.ctx, update, "")
		if err != nil {
			return fmt.Errorf("could not create %s: %w", o.typ, err)
		}

	case IndexTypePartition:
		// Handle partitioning via table metadata update
		tableRef := c.GetTableReference(table.Name)
		update := bigquery.TableMetadataToUpdate{}
		if o.timePartitioning != nil {
			update.TimePartitioning = &bigquery.TimePartitioning{
				Type:       bigquery.TimePartitioningType(o.timePartitioning.Type),
				Field:      o.timePartitioning.Field,
				Expiration: time.Duration(o.timePartitioning.ExpirationMs) * time.Millisecond,
			}
		}
		// Note: BigQuery Go client doesn't support RangePartitioning in TableMetadataToUpdate
		// Range partitioning must be set during table creation
		if o.rangePartitioning != nil {
			return fmt.Errorf("range partitioning cannot be added to existing tables - must be set during table creation")
		}
		_, err = tableRef.Update(c.ctx, update, "")
		if err != nil {
			return fmt.Errorf("could not create %s: %w", o.typ, err)
		}

	case IndexTypeSearch:
		// Handle search index via DDL
		err = c.createSearchIndex(table, o.searchIndex)
		if err != nil {
			return fmt.Errorf("could not create search index: %w", err)
		}

	case IndexTypeVector:
		// Handle vector index via DDL
		err = c.createVectorIndex(table, o.vectorIndex)
		if err != nil {
			return fmt.Errorf("could not create vector index: %w", err)
		}

	default:
		return fmt.Errorf("unsupported index type: %s", o.typ)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created %s for table %s", o.typ, table.Name)
	}

	return nil
}

// createSearchIndex creates a search index using DDL
func (c *Client) createSearchIndex(table *Table, config *SearchIndexConfig) error {
	// Generate index name if not provided
	indexName := config.Name
	if indexName == "" {
		// Create columns for name generation
		var cols []Column
		if config.IndexAllColumns {
			// For ALL COLUMNS, we can't generate a specific name, so use a generic one
			indexName = fmt.Sprintf("%s_search_index", table.Name)
		} else if len(config.Columns) > 0 {
			// Create column objects for name generation
			for _, colName := range config.Columns {
				cols = append(cols, Column{Name: colName})
			}
			if generatedName, err := createIndexName(table, &cols); err == nil {
				indexName = generatedName
			} else {
				indexName = fmt.Sprintf("%s_search_index", table.Name)
			}
		} else {
			return fmt.Errorf("search index must specify either columns or IndexAllColumns=true")
		}
	}

	var columnsClause string
	if config.IndexAllColumns {
		columnsClause = "ALL COLUMNS"
	} else if len(config.Columns) > 0 {
		columnsClause = fmt.Sprintf("(%s)", strings.Join(config.Columns, ", "))
	} else {
		return fmt.Errorf("search index must specify either columns or IndexAllColumns=true")
	}

	query := fmt.Sprintf(`
		CREATE SEARCH INDEX %s
		ON %s.%s.%s %s
	`, escapeIdentifier(indexName), c.projectID, c.datasetID, table.Name, columnsClause)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "executing search index DDL: %s", query)
	}

	err := c.ExecuteDML(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	return nil
}

// createVectorIndex creates a vector index using DDL
func (c *Client) createVectorIndex(table *Table, config *VectorIndexConfig) error {
	// Generate index name if not provided
	indexName := config.Name
	if indexName == "" {
		// Create column for name generation
		cols := []Column{{Name: config.Column}}
		if generatedName, err := createIndexName(table, &cols); err == nil {
			indexName = generatedName
		} else {
			indexName = fmt.Sprintf("%s_vector_index", table.Name)
		}
	}

	query := fmt.Sprintf(`
		CREATE VECTOR INDEX %s
		ON %s.%s.%s (%s)
	`, escapeIdentifier(indexName), c.projectID, c.datasetID, table.Name, escapeIdentifier(config.Column))

	// Add options if specified
	if len(config.Options) > 0 || config.DistanceType != "" || config.Dimensions > 0 {
		options := []string{}

		if config.DistanceType != "" {
			options = append(options, fmt.Sprintf("distance_type='%s'", config.DistanceType))
		}

		if config.Dimensions > 0 {
			options = append(options, fmt.Sprintf("dimensions=%d", config.Dimensions))
		}

		// Add any additional options
		for key, value := range config.Options {
			options = append(options, fmt.Sprintf("%s='%v'", key, value))
		}

		if len(options) > 0 {
			query += fmt.Sprintf("\nOPTIONS (%s)", strings.Join(options, ", "))
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "executing vector index DDL: %s", query)
	}

	err := c.ExecuteDML(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create vector index: %w", err)
	}

	return nil
}

// DropIndex drops clustering or partitioning from a BigQuery table
func (c *Client) DropIndex(table *Table, opts *indexOpts) error {
	if table == nil || table.Name == "" {
		return fmt.Errorf("unable to drop index: table and table.Name cannot be zero values")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping %s for table %s", opts.typ, table.Name)
	}

	// Remove index based on type
	switch opts.typ {
	case IndexTypeClustering:
		tableRef := c.GetTableReference(table.Name)
		update := bigquery.TableMetadataToUpdate{
			Clustering: &bigquery.Clustering{}, // Empty clustering removes it
		}
		_, err := tableRef.Update(c.ctx, update, "")
		if err != nil {
			return fmt.Errorf("could not drop %s: %w", opts.typ, err)
		}

	case IndexTypePartition:
		// Note: BigQuery doesn't allow removing partitioning from existing tables
		// This would require table recreation
		return fmt.Errorf("cannot drop partitioning from existing table %s - partitioning cannot be removed once set", table.Name)

	case IndexTypeSearch:
		if opts.searchIndex == nil || opts.searchIndex.Name == "" {
			return fmt.Errorf("search index name is required for dropping")
		}
		query := fmt.Sprintf("DROP SEARCH INDEX %s ON %s.%s.%s",
			escapeIdentifier(opts.searchIndex.Name), c.projectID, c.datasetID, table.Name)

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "executing drop search index DDL: %s", query)
		}

		err := c.ExecuteDML(c.ctx, query)
		if err != nil {
			return fmt.Errorf("could not drop search index: %w", err)
		}

	case IndexTypeVector:
		if opts.vectorIndex == nil || opts.vectorIndex.Name == "" {
			return fmt.Errorf("vector index name is required for dropping")
		}
		query := fmt.Sprintf("DROP VECTOR INDEX %s ON %s.%s.%s",
			escapeIdentifier(opts.vectorIndex.Name), c.projectID, c.datasetID, table.Name)

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "executing drop vector index DDL: %s", query)
		}

		err := c.ExecuteDML(c.ctx, query)
		if err != nil {
			return fmt.Errorf("could not drop vector index: %w", err)
		}

	default:
		return fmt.Errorf("unsupported index type: %s", opts.typ)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped %s for table %s", opts.typ, table.Name)
	}

	return nil
}

// IsIndexExistent checks if clustering or partitioning exists for a table
func (c *Client) IsIndexExistent(table *Table, opts *indexOpts) (bool, error) {
	if table == nil || table.Name == "" {
		return false, fmt.Errorf("unable to check if index exists: table and table.Name cannot be zero values")
	}

	tableRef := c.GetTableReference(table.Name)
	metadata, err := tableRef.Metadata(c.ctx)
	if err != nil {
		return false, err
	}

	switch opts.typ {
	case IndexTypeClustering:
		// Check if clustering exists and matches the requested fields
		if metadata.Clustering == nil || len(metadata.Clustering.Fields) == 0 {
			return false, nil
		}

		// Check if the clustering fields exactly match the requested fields
		if len(metadata.Clustering.Fields) != len(opts.clusteringFields) {
			return false, nil
		}

		requestedFields := make(map[string]bool)
		for _, field := range opts.clusteringFields {
			requestedFields[field] = true
		}

		for _, field := range metadata.Clustering.Fields {
			if !requestedFields[field] {
				return false, nil
			}
		}

		return true, nil

	case IndexTypePartition:
		// Check time partitioning
		if opts.timePartitioning != nil {
			if metadata.TimePartitioning == nil {
				return false, nil
			}
			if string(metadata.TimePartitioning.Type) != opts.timePartitioning.Type {
				return false, nil
			}
			if metadata.TimePartitioning.Field != opts.timePartitioning.Field {
				return false, nil
			}
			return true, nil
		}

		// Check range partitioning
		if opts.rangePartitioning != nil {
			if metadata.RangePartitioning == nil {
				return false, nil
			}
			if metadata.RangePartitioning.Field != opts.rangePartitioning.Field {
				return false, nil
			}
			// Could add more detailed range comparison here if needed
			return true, nil
		}

		return false, nil

	case IndexTypeSearch:
		// Check if search index exists by querying INFORMATION_SCHEMA
		if opts.searchIndex == nil || opts.searchIndex.Name == "" {
			return false, fmt.Errorf("search index name is required for existence check")
		}

		query := fmt.Sprintf(`
			SELECT COUNT(*) as count
			FROM %s.%s.INFORMATION_SCHEMA.SEARCH_INDEXES
			WHERE table_name = %s AND index_name = %s
		`, c.projectID, c.datasetID, escapeIdentifier(table.Name), escapeIdentifier(opts.searchIndex.Name))

		it, err := c.ExecuteQuery(c.ctx, query)
		if err != nil {
			return false, fmt.Errorf("failed to check search index existence: %w", err)
		}

		var row struct {
			Count int64 `bigquery:"count"`
		}
		err = it.Next(&row)
		if err != nil {
			return false, fmt.Errorf("failed to read search index count: %w", err)
		}

		return row.Count > 0, nil

	case IndexTypeVector:
		// Check if vector index exists by querying INFORMATION_SCHEMA
		if opts.vectorIndex == nil || opts.vectorIndex.Name == "" {
			return false, fmt.Errorf("vector index name is required for existence check")
		}

		query := fmt.Sprintf(`
			SELECT COUNT(*) as count
			FROM %s.%s.INFORMATION_SCHEMA.VECTOR_INDEXES
			WHERE table_name = %s AND index_name = %s
		`, c.projectID, c.datasetID, escapeIdentifier(table.Name), escapeIdentifier(opts.vectorIndex.Name))

		it, err := c.ExecuteQuery(c.ctx, query)
		if err != nil {
			return false, fmt.Errorf("failed to check vector index existence: %w", err)
		}

		var row struct {
			Count int64 `bigquery:"count"`
		}
		err = it.Next(&row)
		if err != nil {
			return false, fmt.Errorf("failed to read vector index count: %w", err)
		}

		return row.Count > 0, nil

	default:
		return false, fmt.Errorf("unsupported index type: %s", opts.typ)
	}
}

// ########################################
// # Options
// ########################################

type alignTableOpts struct {
	nullAlignment   bool
	allowRecreation bool
}

func WithAlignTableNullability(v bool) func(*alignTableOpts) {
	return func(opts *alignTableOpts) {
		opts.nullAlignment = v
	}
}

// WARNING: This option is dangerous and should only be used if you know what you are doing. It will drop and recreate the table.
func WithAlignTableRecreation(v bool) func(*alignTableOpts) {
	return func(opts *alignTableOpts) {
		opts.allowRecreation = v
	}
}

type createTableOpts struct {
	overwrite        bool
	timePartitioning *TimePartitioning
	clustering       *ClusteringConfig
}

func WithCreateTableOverwrite(v bool) func(*createTableOpts) {
	return func(opts *createTableOpts) {
		opts.overwrite = v
	}
}

func WithTimePartitioning(tp *TimePartitioning) func(*createTableOpts) {
	return func(opts *createTableOpts) {
		opts.timePartitioning = tp
	}
}

func WithClustering(clustering *ClusteringConfig) func(*createTableOpts) {
	return func(opts *createTableOpts) {
		opts.clustering = clustering
	}
}

type createDatasetOpts struct {
	overwrite   bool
	labels      map[string]string
	description string
}

func WithCreateDatasetOverwrite(v bool) func(*createDatasetOpts) {
	return func(opts *createDatasetOpts) {
		opts.overwrite = v
	}
}

func WithDatasetLabels(labels map[string]string) func(*createDatasetOpts) {
	return func(opts *createDatasetOpts) {
		opts.labels = labels
	}
}

func WithDatasetDescription(description string) func(*createDatasetOpts) {
	return func(opts *createDatasetOpts) {
		opts.description = description
	}
}

type indexOpts struct {
	recreate          bool
	typ               indexType
	clusteringFields  []string
	timePartitioning  *TimePartitioning
	rangePartitioning *RangePartitioning
	searchIndex       *SearchIndexConfig
	vectorIndex       *VectorIndexConfig
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

func WithClusteringFields(fields []string) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = IndexTypeClustering
		opts.clusteringFields = fields
	}
}

func WithIndexTimePartitioning(tp *TimePartitioning) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = IndexTypePartition
		opts.timePartitioning = tp
	}
}

func WithIndexRangePartitioning(rp *RangePartitioning) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = IndexTypePartition
		opts.rangePartitioning = rp
	}
}

func WithSearchIndex(config *SearchIndexConfig) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = IndexTypeSearch
		opts.searchIndex = config
	}
}

func WithVectorIndex(config *VectorIndexConfig) func(*indexOpts) {
	return func(opts *indexOpts) {
		opts.typ = IndexTypeVector
		opts.vectorIndex = config
	}
}

type selectRowsOpts struct {
	columns            []string
	whereConditions    []string
	whereConditionsMap map[string]interface{} // For buildWhereClause
	groupBy            []string
	havingConditions   []string
	windowDefinitions  []string
	orderBy            []string
	limit              int
	params             map[string]interface{}
}

func WithSelectColumns(columns []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.columns = columns
	}
}

func WithWhereCondition(condition string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.whereConditions = append(opts.whereConditions, condition)
	}
}

func WithWhereConditions(conditions []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.whereConditions = append(opts.whereConditions, conditions...)
	}
}

func WithWhereConditionsMap(conditions map[string]interface{}) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.whereConditionsMap = conditions
	}
}

func WithGroupBy(groupBy []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.groupBy = groupBy
	}
}

func WithHavingCondition(condition string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.havingConditions = append(opts.havingConditions, condition)
	}
}

func WithHavingConditions(conditions []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.havingConditions = append(opts.havingConditions, conditions...)
	}
}

func WithWindowDefinition(windowName, definition string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.windowDefinitions = append(opts.windowDefinitions, fmt.Sprintf("%s AS (%s)", windowName, definition))
	}
}

func WithWindowDefinitions(definitions []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.windowDefinitions = append(opts.windowDefinitions, definitions...)
	}
}

func WithOrderBy(orderBy []string) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.orderBy = orderBy
	}
}

func WithLimit(limit int) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.limit = limit
	}
}

func WithSelectParams(params map[string]interface{}) func(*selectRowsOpts) {
	return func(opts *selectRowsOpts) {
		opts.params = params
	}
}

// Helper function to convert our Schema to BigQuery FieldSchema slice
func convertToBigQuerySchemaFields(schema *Schema) []*bigquery.FieldSchema {
	if schema == nil {
		return nil
	}

	fields := make([]*bigquery.FieldSchema, len(schema.Fields))
	for i, field := range schema.Fields {
		fields[i] = &bigquery.FieldSchema{
			Name:        field.Name,
			Type:        bigquery.FieldType(field.Type),
			Repeated:    field.Mode == "REPEATED",
			Required:    field.Mode == "REQUIRED",
			Description: field.Description,
		}
	}

	return fields
}
