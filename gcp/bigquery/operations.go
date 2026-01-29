package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
	"google.golang.org/api/iterator"
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

		// Handle slices for JSON fields - serialize as JSON or convert empty slices to null
		if col.Type == TypeJSON && value != nil {
			if reflect.TypeOf(value).Kind() == reflect.Slice {
				sliceValue := reflect.ValueOf(value)
				if sliceValue.Len() == 0 {
					// Empty slice for JSON field should be null, not empty array
					value = nil
				} else {
					// Non-empty slice needs to be serialized as JSON
					jsonBytes, err := json.Marshal(value)
					if err != nil {
						return nil, "", fmt.Errorf("failed to marshal JSON field %s: %w", colName, err)
					}
					value = string(jsonBytes)
				}
			} else if reflect.TypeOf(value).Kind() == reflect.Ptr {
				// Handle pointers to structs - serialize as JSON
				jsonBytes, err := json.Marshal(value)
				if err != nil {
					return nil, "", fmt.Errorf("failed to marshal JSON field %s: %w", colName, err)
				}
				value = string(jsonBytes)
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

	// Ensure primary keys are set from table columns
	if len(table.PrimaryKeyNames) == 0 {
		table.PrimaryKeyNames = c.detectPrimaryKeysFromColumns(table)
	}

	createOpts := createTableOpts{
		overwrite: false,
	}

	for _, opt := range opts {
		opt(&createOpts)
	}

	// Auto-optimize for primary key clustering if enabled and not explicitly set
	if createOpts.autoOptimize && createOpts.clustering == nil && len(table.PrimaryKeyNames) > 0 {
		createOpts.clustering = &ClusteringConfig{
			Fields: table.PrimaryKeyNames,
		}
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "auto-optimizing table %s with clustering on primary keys: %v", table.Name, table.PrimaryKeyNames)
		}
	}

	// Check if table already exists
	exists, err := c.IsTableExistent(table)
	if err != nil {
		return err
	}

	if exists && !createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s already exists in dataset %s, overwrite option is false, ensuring primary key constraints are set", table.Name, c.datasetID)
		}

		// Even if table exists, ensure primary key constraints are set
		if len(table.PrimaryKeyNames) > 0 {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "setting primary key constraints on existing table %s for columns %v", table.Name, table.PrimaryKeyNames)
			}
			if err = c.SetPrimaryKeyConstraints(table.Name, table.PrimaryKeyNames); err != nil {
				// Log the error but don't fail the operation
				if c.verboseMode {
					log.Log.Debugf(c.ctx, "warning: could not set primary key constraints on existing table %s: %v", table.Name, err)
				}
			}
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

	// Set primary key constraints if we have primary keys
	if len(table.PrimaryKeyNames) > 0 {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "setting primary key constraints on newly created table %s for columns %v", table.Name, table.PrimaryKeyNames)
		}
		if err = c.SetPrimaryKeyConstraints(table.Name, table.PrimaryKeyNames); err != nil {
			// Log the error but don't fail table creation
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "warning: could not set primary key constraints on table %s: %v", table.Name, err)
			}
		}
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

	// Ensure primary keys are set from table columns
	if len(table.PrimaryKeyNames) == 0 {
		table.PrimaryKeyNames = c.detectPrimaryKeysFromColumns(table)
	}

	// // Check if table exists
	// exists, err := c.IsTableExistent(table)
	// if err != nil {
	// 	return fmt.Errorf("could not check if table exists: %w", err)
	// }
	// if !exists {
	// 	if c.verboseMode {
	// 		log.Log.Debugf(c.ctx, "table %s does not exist in %s, creating....", table.Name, c.datasetID)
	// 	}
	// 	if err = c.CreateTable(table); err != nil {
	// 		return fmt.Errorf("could not create table: %w", err)
	// 	}
	// 	return nil
	// }

	// Set primary key constraints if we have primary keys and they're not already set
	if len(table.PrimaryKeyNames) > 0 {
		hasConstraint, err := c.HasPrimaryKeyConstraint(table.Name)
		if err != nil {
			return fmt.Errorf("could not check if primary key constraint exists: %w", err)
		}
		if !hasConstraint {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "setting primary key constraints on table %s for columns %v", table.Name, table.PrimaryKeyNames)
			}
			if err = c.SetPrimaryKeyConstraints(table.Name, table.PrimaryKeyNames); err != nil {
				return fmt.Errorf("could not set primary key constraints: %w", err)
			}
		}

		// Optimize table for upserts by adding clustering on primary keys
		if err = c.AddClusteringToTable(table); err != nil {
			// Log the error but don't fail schema alignment
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "warning: could not optimize table %s for upserts: %v", table.Name, err)
			}
		}
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

// detectPrimaryKeysFromColumns detects all primary keys from table columns
func (c *Client) detectPrimaryKeysFromColumns(table *Table) []string {
	if table == nil || table.Columns == nil {
		return nil
	}

	var pkeys []string
	// Look for columns with REQUIRED mode (primary key candidates)
	for colName, col := range *table.Columns {
		if col.Mode == "REQUIRED" {
			pkeys = append(pkeys, colName)
		}
	}

	// If no REQUIRED columns found, fallback to "id" column if it exists
	if len(pkeys) == 0 {
		if _, exists := (*table.Columns)["id"]; exists {
			pkeys = append(pkeys, "id")
		}
	}

	return pkeys
}

// SetPrimaryKeyConstraint sets a primary key constraint on an existing BigQuery table
func (c *Client) SetPrimaryKeyConstraint(tableName, primaryKeyColumn string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if primaryKeyColumn == "" {
		return fmt.Errorf("primary key column cannot be empty")
	}

	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Check if primary key constraint already exists
	hasConstraint, err := c.HasPrimaryKeyConstraint(tableName)
	if err != nil {
		return fmt.Errorf("could not check if primary key constraint exists: %w", err)
	}
	if hasConstraint {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "primary key constraint already exists on table %s", tableName)
		}
		return nil
	}

	// Build ALTER TABLE statement to add primary key constraint
	// Note: BigQuery doesn't support named primary key constraints
	alterQuery := fmt.Sprintf(`
		ALTER TABLE %s.%s.%s
		ADD PRIMARY KEY (%s) NOT ENFORCED
	`, c.projectID, c.datasetID, tableName, escapeIdentifier(primaryKeyColumn))

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "setting primary key constraint on table %s: %s", tableName, alterQuery)
	}

	// Execute ALTER TABLE statement
	err = c.ExecuteDML(c.ctx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to set primary key constraint: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully set primary key constraint on table %s", tableName)
	}

	return nil
}

// SetPrimaryKeyConstraints sets primary key constraints on an existing BigQuery table
func (c *Client) SetPrimaryKeyConstraints(tableName string, primaryKeyColumns []string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("primary key columns cannot be empty")
	}

	// Check if primary key constraint already exists
	hasConstraint, err := c.HasPrimaryKeyConstraint(tableName)
	if err != nil {
		return fmt.Errorf("could not check if primary key constraint exists: %w", err)
	}
	if hasConstraint {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "primary key constraint already exists on table %s, updating to new primary keys", tableName)
		}
		// Use UpdatePrimaryKeyConstraints to modify existing constraint
		return c.UpdatePrimaryKeyConstraints(tableName, primaryKeyColumns)
	}

	// Build ALTER TABLE statement to add primary key constraint with multiple columns
	// Note: BigQuery doesn't support named primary key constraints
	escapedColumns := make([]string, len(primaryKeyColumns))
	for i, col := range primaryKeyColumns {
		escapedColumns[i] = escapeIdentifier(col)
	}

	alterQuery := fmt.Sprintf(`
		ALTER TABLE %s.%s.%s
		ADD PRIMARY KEY (%s) NOT ENFORCED
	`, c.projectID, c.datasetID, tableName, strings.Join(escapedColumns, ", "))

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "setting primary key constraints on table %s: %s", tableName, alterQuery)
	}

	// Execute ALTER TABLE statement
	err = c.ExecuteDML(c.ctx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to set primary key constraints: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully set primary key constraints on table %s", tableName)
	}

	return nil
}

// UpdatePrimaryKeyConstraints updates primary key constraints on an existing BigQuery table
func (c *Client) UpdatePrimaryKeyConstraints(tableName string, primaryKeyColumns []string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("primary key columns cannot be empty")
	}

	// Check if primary key constraint exists
	hasConstraint, err := c.HasPrimaryKeyConstraint(tableName)
	if err != nil {
		return fmt.Errorf("could not check if primary key constraint exists: %w", err)
	}

	// Build ALTER TABLE statement to modify primary key constraint
	escapedColumns := make([]string, len(primaryKeyColumns))
	for i, col := range primaryKeyColumns {
		escapedColumns[i] = escapeIdentifier(col)
	}

	var alterQuery string
	if hasConstraint {
		// Get current primary key columns to compare
		currentPrimaryKeys, err := c.GetPrimaryKeyColumns(tableName)
		if err != nil {
			return fmt.Errorf("could not get current primary key columns: %w", err)
		}

		// Only drop and recreate if the primary keys don't match
		if !comparePrimaryKeyColumns(currentPrimaryKeys, primaryKeyColumns) {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "primary key columns don't match (current: %v, desired: %v), updating", currentPrimaryKeys, primaryKeyColumns)
			}
			// Drop existing primary key constraint first, then add new one
			alterQuery = fmt.Sprintf(`
				ALTER TABLE %s.%s.%s
				DROP PRIMARY KEY;
				
				ALTER TABLE %s.%s.%s
				ADD PRIMARY KEY (%s) NOT ENFORCED
			`, c.projectID, c.datasetID, tableName,
				c.projectID, c.datasetID, tableName, strings.Join(escapedColumns, ", "))
		} else {
			if c.verboseMode {
				log.Log.Debugf(c.ctx, "primary key columns already match (current: %v), no update needed", currentPrimaryKeys)
			}
			return nil // No update needed
		}
	} else {
		// Just add the primary key constraint
		alterQuery = fmt.Sprintf(`
			ALTER TABLE %s.%s.%s
			ADD PRIMARY KEY (%s) NOT ENFORCED
		`, c.projectID, c.datasetID, tableName, strings.Join(escapedColumns, ", "))
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "updating primary key constraints on table %s: %s", tableName, alterQuery)
	}

	// Execute ALTER TABLE statement(s)
	err = c.ExecuteDML(c.ctx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to update primary key constraints: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully updated primary key constraints on table %s", tableName)
	}

	return nil
}

// GetPrimaryKeyColumns gets the current primary key columns from a BigQuery table
func (c *Client) GetPrimaryKeyColumns(tableName string) ([]string, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Query INFORMATION_SCHEMA to get primary key columns
	query := fmt.Sprintf(`
		SELECT column_name
		FROM %s.%s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE table_name = '%s' AND constraint_name IN (
			SELECT constraint_name
			FROM %s.%s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
			WHERE table_name = '%s' AND constraint_type = 'PRIMARY KEY'
		)
		ORDER BY ordinal_position
	`, c.projectID, c.datasetID, tableName, c.projectID, c.datasetID, tableName)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "getting primary key columns: %s", query)
	}

	it, err := c.ExecuteQuery(c.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key columns: %w", err)
	}

	var primaryKeyColumns []string
	for {
		var result struct {
			ColumnName string `bigquery:"column_name"`
		}
		err := it.Next(&result)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key column: %w", err)
		}
		primaryKeyColumns = append(primaryKeyColumns, result.ColumnName)
	}

	return primaryKeyColumns, nil
}

// comparePrimaryKeyColumns compares two slices of primary key column names
func comparePrimaryKeyColumns(current, desired []string) bool {
	if len(current) != len(desired) {
		return false
	}

	for i, col := range current {
		if col != desired[i] {
			return false
		}
	}

	return true
}

// HasPrimaryKeyConstraint checks if a table has a primary key constraint
func (c *Client) HasPrimaryKeyConstraint(tableName string) (bool, error) {
	if tableName == "" {
		return false, fmt.Errorf("table name cannot be empty")
	}

	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return false, fmt.Errorf("invalid table name: %w", err)
	}

	// Query INFORMATION_SCHEMA to check for primary key constraints
	query := fmt.Sprintf(`
		SELECT COUNT(*) as constraint_count
		FROM %s.%s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
		WHERE table_name = '%s' AND constraint_type = 'PRIMARY KEY'
	`, c.projectID, c.datasetID, tableName)

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "checking for primary key constraints: %s", query)
	}

	it, err := c.ExecuteQuery(c.ctx, query)
	if err != nil {
		return false, fmt.Errorf("failed to check primary key constraints: %w", err)
	}

	var result struct {
		ConstraintCount int64 `bigquery:"constraint_count"`
	}
	err = it.Next(&result)
	if err != nil {
		return false, fmt.Errorf("failed to read constraint count: %w", err)
	}

	return result.ConstraintCount > 0, nil
}

// AddClusteringToTable adds clustering to a table with flexible configuration options
func (c *Client) AddClusteringToTable(table *Table, opts ...func(*clusteringOpts)) error {
	if table == nil {
		return fmt.Errorf("table cannot be nil")
	}

	// Default clustering options
	clusteringOpts := clusteringOpts{
		clusteringFields: nil, // Will default to primary key if not specified
		forceRecreate:    false,
	}

	for _, opt := range opts {
		opt(&clusteringOpts)
	}

	// Ensure primary keys are set from table columns
	if len(table.PrimaryKeyNames) == 0 {
		table.PrimaryKeyNames = c.detectPrimaryKeysFromColumns(table)
	}

	// Determine clustering fields
	var clusteringFields []string
	if len(clusteringOpts.clusteringFields) > 0 {
		clusteringFields = clusteringOpts.clusteringFields
	} else if len(table.PrimaryKeyNames) > 0 {
		clusteringFields = table.PrimaryKeyNames
	} else {
		return fmt.Errorf("no clustering fields specified and no primary keys found for table %s", table.Name)
	}

	// Check if clustering already exists
	hasClustering, err := c.HasClustering(table.Name)
	if err != nil {
		return fmt.Errorf("could not check if clustering exists: %w", err)
	}

	if hasClustering && !clusteringOpts.forceRecreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "table %s already has clustering, skipping (use WithForceRecreateClustering(true) to override)", table.Name)
		}
		return nil
	}

	// Add clustering
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "adding clustering to table %s on fields: %v", table.Name, clusteringFields)
	}

	err = c.CreateIndex(table, WithClusteringFields(clusteringFields), WithRecreateIndex())
	if err != nil {
		return fmt.Errorf("failed to add clustering: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully added clustering to table %s", table.Name)
	}

	return nil
}

// HasClustering checks if a table has clustering configured
func (c *Client) HasClustering(tableName string) (bool, error) {
	if tableName == "" {
		return false, fmt.Errorf("table name cannot be empty")
	}

	// Validate table name
	if err := validateTableName(tableName); err != nil {
		return false, fmt.Errorf("invalid table name: %w", err)
	}

	// Get table metadata to check for clustering
	tableRef := c.GetTableReference(tableName)
	metadata, err := tableRef.Metadata(c.ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get table metadata: %w", err)
	}

	return metadata.Clustering != nil && len(metadata.Clustering.Fields) > 0, nil
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
			log.Log.Infof(c.ctx, "column %s exists in table %s but not in new schema - recreation needed", colName, table.Name)
			return true, nil
		}
	}

	// Check if any existing columns have type changes
	for colName, newCol := range *table.Columns {
		if currentCol, exists := (*currentTable.Columns)[colName]; exists {
			if normalizeBigQueryType(currentCol.Type) != normalizeBigQueryType(newCol.Type) {
				log.Log.Infof(c.ctx, "column %s type in table %s changed from %s to %s - recreation needed", colName, table.Name, currentCol.Type, newCol.Type)
				return true, nil
			}
			// Check mode changes that require recreation (e.g., REQUIRED to NULLABLE)
			if currentCol.Mode != newCol.Mode {
				log.Log.Infof(c.ctx, "column %s mode in table %s changed from %s to %s - recreation needed", colName, table.Name, currentCol.Mode, newCol.Mode)
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

	// Ensure primary keys are set from table columns
	if len(table.PrimaryKeyNames) == 0 {
		table.PrimaryKeyNames = c.detectPrimaryKeysFromColumns(table)
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

// UpsertRows upserts rows into a BigQuery table using a MERGE statement with retry logic
func (c *Client) UpsertRows(rows ...Row) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to upsert")
	}

	for _, row := range rows {
		if row == nil {
			return fmt.Errorf("row cannot be nil")
		}

		err := c.upsertRowWithRetry(row)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetPrimaryKeyValues extracts the primary key values from a Row as a map
func (c *Client) GetPrimaryKeyValues(row Row) map[string]interface{} {
	table := row.Table()
	cols := row.Columns()
	pkValues := make(map[string]interface{})

	// Get primary key column names
	var primaryKeys []string
	if len(table.PrimaryKeyNames) > 0 {
		primaryKeys = table.PrimaryKeyNames
	} else {
		// Fallback: look for a column named "id" (common convention)
		if _, exists := (*cols)["id"]; exists {
			primaryKeys = []string{"id"}
		} else {
			// Last resort: use all REQUIRED columns
			for colName, col := range *cols {
				if col.Mode == "REQUIRED" {
					primaryKeys = append(primaryKeys, colName)
				}
			}
		}
	}

	// Extract values for each primary key
	for _, pkName := range primaryKeys {
		if col, exists := (*cols)[pkName]; exists {
			pkValues[pkName] = col.Value
		}
	}

	return pkValues
}

// GetPrimaryKeyValuesString returns primary key values as a formatted string for logging
func (c *Client) GetPrimaryKeyValuesString(row Row) string {
	pkValues := c.GetPrimaryKeyValues(row)
	if len(pkValues) == 0 {
		return "no-pk"
	}

	var parts []string
	for pkName, pkValue := range pkValues {
		parts = append(parts, fmt.Sprintf("%s=%v", pkName, pkValue))
	}

	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

// upsertRowWithRetry performs a single row upsert with exponential backoff retry logic
func (c *Client) upsertRowWithRetry(row Row) error {
	maxRetries := c.maxRetries
	if maxRetries <= 0 {
		maxRetries = 3 // Default fallback
	}

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		table := row.Table()

		// Build MERGE statement for upsert
		mergeQuery, err := c.buildMergeQuery(row)
		if err != nil {
			return fmt.Errorf("failed to build merge query: %w", err)
		}

		if c.verboseMode {
			log.Log.Debugf(c.ctx, "executing MERGE query for upsert in table %s: %s", table.Name, mergeQuery)
		}

		// Execute MERGE statement
		err = c.ExecuteDML(c.ctx, mergeQuery)
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Check if this is a concurrency error that we should retry
		if strings.Contains(err.Error(), "concurrent update") ||
			strings.Contains(err.Error(), "Could not serialize access") {

			if attempt < maxRetries {
				// Exponential backoff: 100ms, 200ms, 400ms, 800ms, etc.
				backoffDuration := time.Duration(100*(1<<attempt)) * time.Millisecond
				log.Log.Warnf(c.ctx, "concurrent update error, retrying in %v (attempt %d/%d), pk columns: %v, pk values: %v",
					backoffDuration, attempt+1, maxRetries+1, row.Table().PrimaryKeyNames, c.GetPrimaryKeyValuesString(row))

				select {
				case <-c.ctx.Done():
					return c.ctx.Err()
				case <-time.After(backoffDuration):
					continue
				}
			}
		} else {
			// Non-retryable error, return immediately
			return fmt.Errorf("failed to upsert row in table %s: %w, query: %s", table.Name, err, mergeQuery)
		}
	}

	return fmt.Errorf("failed to upsert row after %d retries: %w", maxRetries+1, lastErr)
}

// buildMergeQuery builds a MERGE statement for upserting a row
func (c *Client) buildMergeQuery(row Row) (string, error) {
	table := row.Table()
	cols := row.Columns()

	// Find primary key columns
	var primaryKeys []string

	// First try to use the table's PrimaryKeyNames if available
	if len(table.PrimaryKeyNames) > 0 {
		primaryKeys = table.PrimaryKeyNames
	} else {
		// Fallback: look for a column named "id" (common convention)
		if _, exists := (*cols)["id"]; exists {
			primaryKeys = []string{"id"}
		} else {
			// Last resort: use all REQUIRED columns
			for colName, col := range *cols {
				if col.Mode == "REQUIRED" {
					primaryKeys = append(primaryKeys, colName)
				}
			}
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("no primary keys found for upsert operation")
	}

	// Build column lists
	var insertColumns []string
	var insertValues []string
	var updateSet []string
	var sourceSelect []string
	var onConditions []string

	for colName, col := range *cols {
		// Format value for SQL
		formattedValue := c.formatValueForSQL(col.Value, col.Type)

		insertColumns = append(insertColumns, escapeIdentifier(colName))
		insertValues = append(insertValues, formattedValue)

		// Build source SELECT clause: value AS column_name
		sourceSelect = append(sourceSelect, fmt.Sprintf("%s AS %s", formattedValue, escapeIdentifier(colName)))

		// For UPDATE SET, exclude primary key columns
		isPrimaryKey := false
		for _, pk := range primaryKeys {
			if colName == pk {
				isPrimaryKey = true
				break
			}
		}
		if !isPrimaryKey {
			updateSet = append(updateSet, fmt.Sprintf("%s = %s", escapeIdentifier(colName), formattedValue))
		}
	}

	// Build ON conditions for multiple primary keys
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", escapeIdentifier(pk), escapeIdentifier(pk)))
	}

	// Build the MERGE statement
	mergeQuery := fmt.Sprintf(`
		MERGE %s.%s.%s AS target
		USING (
			SELECT %s
		) AS source
		ON %s
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s)
			VALUES (%s)`,
		c.projectID, c.datasetID, table.Name,
		strings.Join(sourceSelect, ", "),
		strings.Join(onConditions, " AND "),
		strings.Join(updateSet, ", "),
		strings.Join(insertColumns, ", "),
		strings.Join(insertValues, ", "))

	// Debug logging
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "MERGE query components - PrimaryKeys: %v, SourceSelect: %v, InsertColumns: %v, InsertValues: %v, UpdateSet: %v",
			primaryKeys, sourceSelect, insertColumns, insertValues, updateSet)
	}

	return mergeQuery, nil
}

// formatValueForSQL formats a value for use in SQL statements
func (c *Client) formatValueForSQL(value interface{}, colType bigqueryType) string {
	if value == nil {
		return "NULL"
	}

	// Handle slices for REPEATED fields
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		sliceValue := reflect.ValueOf(value)
		if sliceValue.Len() == 0 {
			return "NULL"
		}

		// For JSON type columns, serialize the entire slice as JSON
		if colType == TypeJSON {
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				// Fallback to array format if JSON marshaling fails
				var elements []string
				for i := 0; i < sliceValue.Len(); i++ {
					elem := sliceValue.Index(i).Interface()
					elemStr := c.formatValueForSQL(elem, colType)
					elements = append(elements, elemStr)
				}
				return fmt.Sprintf("[%s]", strings.Join(elements, ", "))
			}
			// Return as PARSE_JSON for JSON type with wide_number_mode to handle float precision
			jsonStr := string(jsonBytes)
			escaped := strings.ReplaceAll(jsonStr, "\\", "\\\\") // Escape backslashes first
			escaped = strings.ReplaceAll(escaped, "'", "\\'")
			return fmt.Sprintf("PARSE_JSON('%s', wide_number_mode=>'round')", escaped)
		}

		// For REPEATED fields, format each element individually
		var elements []string
		for i := 0; i < sliceValue.Len(); i++ {
			elem := sliceValue.Index(i).Interface()
			// Format each element based on its type
			elemStr := c.formatValueForSQL(elem, colType)
			elements = append(elements, elemStr)
		}
		return fmt.Sprintf("[%s]", strings.Join(elements, ", "))
	}

	switch colType {
	case TypeString:
		// Escape single quotes and newlines, wrap in quotes
		str := fmt.Sprintf("%v", value)
		escaped := strings.ReplaceAll(str, "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\n", "\\n")
		escaped = strings.ReplaceAll(escaped, "\r", "\\r")
		escaped = strings.ReplaceAll(escaped, "\t", "\\t")
		return fmt.Sprintf("'%s'", escaped)
	case TypeJSON:
		// For JSON type, serialize structs/pointers to JSON
		if reflect.TypeOf(value).Kind() == reflect.Ptr || reflect.TypeOf(value).Kind() == reflect.Struct {
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				// Fallback to string representation if JSON marshaling fails
				str := fmt.Sprintf("%v", value)
				escaped := strings.ReplaceAll(str, "\\", "\\\\") // Escape backslashes first
				escaped = strings.ReplaceAll(escaped, "'", "\\'")
				escaped = strings.ReplaceAll(escaped, "\n", "\\n")
				escaped = strings.ReplaceAll(escaped, "\r", "\\r")
				escaped = strings.ReplaceAll(escaped, "\t", "\\t")
				return fmt.Sprintf("'%s'", escaped)
			}
			// Wrap JSON in quotes and cast to JSON type for BigQuery MERGE
			// Use wide_number_mode to handle float precision issues
			jsonStr := string(jsonBytes)
			// Properly escape for SQL: escape backslashes first, then single quotes
			escaped := strings.ReplaceAll(jsonStr, "\\", "\\\\")
			escaped = strings.ReplaceAll(escaped, "'", "\\'")
			return fmt.Sprintf("PARSE_JSON('%s', wide_number_mode=>'round')", escaped)
		}
		// For non-struct values, treat as string
		str := fmt.Sprintf("%v", value)
		escaped := strings.ReplaceAll(str, "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\n", "\\n")
		escaped = strings.ReplaceAll(escaped, "\r", "\\r")
		escaped = strings.ReplaceAll(escaped, "\t", "\\t")
		return fmt.Sprintf("'%s'", escaped)
	case TypeInt64, TypeInteger:
		return fmt.Sprintf("%d", value)
	case TypeFloat64, TypeFloat:
		return fmt.Sprintf("%f", value)
	case TypeBoolean:
		return fmt.Sprintf("%t", value)
	case TypeTimestamp:
		if t, ok := value.(time.Time); ok {
			if t.IsZero() {
				return "NULL"
			}
			return fmt.Sprintf("TIMESTAMP('%s')", t.Format(time.RFC3339))
		}
		// Check if this is a struct that embeds time.Time (e.g., UnixTime)
		if t, ok := extractTimeFromStruct(value); ok {
			if t.IsZero() {
				return "NULL"
			}
			return fmt.Sprintf("TIMESTAMP('%s')", t.Format(time.RFC3339))
		}
		return fmt.Sprintf("TIMESTAMP('%v')", value)
	case TypeBytes:
		// For bytes, convert to hex string
		if b, ok := value.([]byte); ok {
			return fmt.Sprintf("B'%x'", b)
		}
		return fmt.Sprintf("B'%v'", value)
	default:
		// Default to string representation
		str := fmt.Sprintf("%v", value)
		escaped := strings.ReplaceAll(str, "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\n", "\\n")
		escaped = strings.ReplaceAll(escaped, "\r", "\\r")
		escaped = strings.ReplaceAll(escaped, "\t", "\\t")
		return fmt.Sprintf("'%s'", escaped)
	}
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
		_, err := c.QueryWithParams(query, params)
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

// QueryWithCustomUnmarshaling executes a SELECT query and unmarshals results using custom JSON handling
func (c *Client) QueryWithCustomUnmarshaling(query string, targetStruct interface{}, jsonFieldTypes map[string]reflect.Type) error {
	// Execute the query
	it, err := c.ExecuteQuery(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Create custom unmarshaling
	unmarshaler := NewCustomJSONUnmarshaler()

	// Register JSON field types
	for fieldName, fieldType := range jsonFieldTypes {
		unmarshaler.RegisterType(fieldName, fieldType)
	}

	// Process each row
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err != nil {
			if err.Error() == "iterator done" {
				break
			}
			return fmt.Errorf("failed to read row: %w", err)
		}

		// Unmarshal the row into the target struct
		err = unmarshaler.UnmarshalBigQueryRow(row, targetStruct)
		if err != nil {
			return fmt.Errorf("failed to unmarshal row: %w", err)
		}
	}

	return nil
}

// QueryRowsWithCustomUnmarshaling executes a SELECT query and returns all rows with custom JSON handling
func (c *Client) QueryRowsWithCustomUnmarshaling(query string, targetSlice interface{}, jsonFieldTypes map[string]reflect.Type) error {
	// Execute the query
	it, err := c.ExecuteQuery(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Validate target slice
	targetValue := reflect.ValueOf(targetSlice)
	if targetValue.Kind() != reflect.Ptr || targetValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("target must be a pointer to a slice")
	}

	sliceValue := targetValue.Elem()
	elementType := sliceValue.Type().Elem()

	// Create custom unmarshaling
	unmarshaler := NewCustomJSONUnmarshaler()

	// Register JSON field types
	for fieldName, fieldType := range jsonFieldTypes {
		unmarshaler.RegisterType(fieldName, fieldType)
	}

	// Process each row
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err != nil {
			if err.Error() == "iterator done" {
				break
			}
			return fmt.Errorf("failed to read row: %w", err)
		}

		// Create a new element
		elementValue := reflect.New(elementType)

		// Unmarshal the row into the element
		err = unmarshaler.UnmarshalBigQueryRow(row, elementValue.Interface())
		if err != nil {
			return fmt.Errorf("failed to unmarshal row: %w", err)
		}

		// Append to slice
		if elementType.Kind() == reflect.Ptr {
			sliceValue.Set(reflect.Append(sliceValue, elementValue))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, elementValue.Elem()))
		}
	}

	return nil
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

	// // Check if table exists
	// exists, err := c.IsTableExistent(&Table{Name: tableName})
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to check if table exists: %w", err)
	// }
	// if !exists {
	// 	return nil, fmt.Errorf("table %s does not exist", tableName)
	// }

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

// CopyDatasetTables copies all tables from the source dataset to the destination dataset.
// This is useful as a workaround for BigQuery's immutable dataset names - you cannot rename
// a dataset, but you can copy all its tables to a new dataset.
func (c *Client) CopyDatasetTables(sourceDatasetID, destDatasetID string, opts ...func(*copyDatasetOpts)) error {
	if sourceDatasetID == "" {
		return fmt.Errorf("source dataset ID cannot be empty")
	}
	if destDatasetID == "" {
		return fmt.Errorf("destination dataset ID cannot be empty")
	}
	if sourceDatasetID == destDatasetID {
		return fmt.Errorf("source and destination dataset IDs cannot be the same")
	}

	copyOpts := copyDatasetOpts{
		overwriteExisting: false,
		skipExisting:      true,
		concurrency:       4,
	}

	for _, opt := range opts {
		opt(&copyOpts)
	}

	// Validate source dataset exists
	sourceDatasetRef := c.client.Dataset(sourceDatasetID)
	_, err := sourceDatasetRef.Metadata(c.ctx)
	if err != nil {
		return fmt.Errorf("source dataset %s does not exist or is not accessible: %w", sourceDatasetID, err)
	}

	// Create destination dataset if it doesn't exist
	destDatasetRef := c.client.Dataset(destDatasetID)
	_, err = destDatasetRef.Metadata(c.ctx)
	if err != nil {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "destination dataset %s does not exist, creating it", destDatasetID)
		}
		err = c.CreateDataset(destDatasetID)
		if err != nil {
			return fmt.Errorf("failed to create destination dataset %s: %w", destDatasetID, err)
		}
	}

	// List all tables in the source dataset
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "listing tables in source dataset %s", sourceDatasetID)
	}

	it := sourceDatasetRef.Tables(c.ctx)
	var tableNames []string
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list tables in source dataset: %w", err)
		}
		tableNames = append(tableNames, table.TableID)
	}

	if len(tableNames) == 0 {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "no tables found in source dataset %s", sourceDatasetID)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "found %d tables to copy from %s to %s", len(tableNames), sourceDatasetID, destDatasetID)
	}

	// Use channels to collect results from goroutines
	type result struct {
		tableName string
		err       error
		skipped   bool
	}

	resultChan := make(chan result, len(tableNames))
	semaphore := make(chan struct{}, copyOpts.concurrency)

	// Copy each table concurrently
	for _, tableName := range tableNames {
		go func(tblName string) {
			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Check if table already exists in destination
			destTableRef := destDatasetRef.Table(tblName)
			_, err := destTableRef.Metadata(c.ctx)
			tableExists := (err == nil)

			if tableExists {
				if copyOpts.skipExisting && !copyOpts.overwriteExisting {
					if c.verboseMode {
						log.Log.Debugf(c.ctx, "skipping table %s as it already exists in destination dataset", tblName)
					}
					resultChan <- result{tableName: tblName, skipped: true}
					return
				}
				if copyOpts.overwriteExisting {
					if c.verboseMode {
						log.Log.Debugf(c.ctx, "overwriting existing table %s in destination dataset", tblName)
					}
				}
			}

			// Get source table reference
			sourceTableRef := sourceDatasetRef.Table(tblName)

			// Create copier
			copier := destTableRef.CopierFrom(sourceTableRef)
			if copyOpts.overwriteExisting {
				copier.WriteDisposition = bigquery.WriteTruncate
			} else {
				copier.WriteDisposition = bigquery.WriteEmpty
			}

			if c.verboseMode {
				log.Log.Debugf(c.ctx, "copying table %s from %s to %s", tblName, sourceDatasetID, destDatasetID)
			}

			// Run copy job
			job, err := copier.Run(c.ctx)
			if err != nil {
				resultChan <- result{tableName: tblName, err: fmt.Errorf("failed to start copy job for table %s: %w", tblName, err)}
				return
			}

			// Wait for job to complete
			status, err := job.Wait(c.ctx)
			if err != nil {
				resultChan <- result{tableName: tblName, err: fmt.Errorf("copy job failed for table %s: %w", tblName, err)}
				return
			}

			if status.Err() != nil {
				resultChan <- result{tableName: tblName, err: fmt.Errorf("copy job completed with error for table %s: %w", tblName, status.Err())}
				return
			}

			if c.verboseMode {
				log.Log.Debugf(c.ctx, "successfully copied table %s", tblName)
			}

			resultChan <- result{tableName: tblName}
		}(tableName)
	}

	// Collect results and check for errors
	var errors []error
	var copiedCount, skippedCount int
	for i := 0; i < len(tableNames); i++ {
		res := <-resultChan
		if res.err != nil {
			errors = append(errors, res.err)
		} else if res.skipped {
			skippedCount++
		} else {
			copiedCount++
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "copy dataset completed: %d tables copied, %d tables skipped, %d errors", copiedCount, skippedCount, len(errors))
	}

	// Return combined error if any occurred
	if len(errors) > 0 {
		return fmt.Errorf("failed to copy %d table(s): %v", len(errors), errors)
	}

	return nil
}

// CopyTable copies a single table from source to destination.
// The source and destination can be in different datasets or even different projects.
func (c *Client) CopyTable(sourceDatasetID, sourceTableName, destDatasetID, destTableName string, opts ...func(*copyTableOpts)) error {
	if sourceDatasetID == "" {
		return fmt.Errorf("source dataset ID cannot be empty")
	}
	if sourceTableName == "" {
		return fmt.Errorf("source table name cannot be empty")
	}
	if destDatasetID == "" {
		return fmt.Errorf("destination dataset ID cannot be empty")
	}
	if destTableName == "" {
		destTableName = sourceTableName
	}

	copyOpts := copyTableOpts{
		overwrite: false,
	}

	for _, opt := range opts {
		opt(&copyOpts)
	}

	// Get source table reference
	sourceTableRef := c.client.Dataset(sourceDatasetID).Table(sourceTableName)

	// Verify source table exists
	_, err := sourceTableRef.Metadata(c.ctx)
	if err != nil {
		return fmt.Errorf("source table %s.%s does not exist or is not accessible: %w", sourceDatasetID, sourceTableName, err)
	}

	// Create destination dataset if it doesn't exist
	destDatasetRef := c.client.Dataset(destDatasetID)
	_, err = destDatasetRef.Metadata(c.ctx)
	if err != nil {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "destination dataset %s does not exist, creating it", destDatasetID)
		}
		err = c.CreateDataset(destDatasetID)
		if err != nil {
			return fmt.Errorf("failed to create destination dataset %s: %w", destDatasetID, err)
		}
	}

	// Get destination table reference
	destTableRef := destDatasetRef.Table(destTableName)

	// Check if destination table exists
	_, err = destTableRef.Metadata(c.ctx)
	destExists := (err == nil)

	if destExists && !copyOpts.overwrite {
		return fmt.Errorf("destination table %s.%s already exists (use WithCopyTableOverwrite(true) to overwrite)", destDatasetID, destTableName)
	}

	// Create copier
	copier := destTableRef.CopierFrom(sourceTableRef)
	if copyOpts.overwrite {
		copier.WriteDisposition = bigquery.WriteTruncate
	} else {
		copier.WriteDisposition = bigquery.WriteEmpty
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "copying table %s.%s to %s.%s", sourceDatasetID, sourceTableName, destDatasetID, destTableName)
	}

	// Run copy job
	job, err := copier.Run(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to start copy job: %w", err)
	}

	// Wait for job to complete
	status, err := job.Wait(c.ctx)
	if err != nil {
		return fmt.Errorf("copy job failed: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("copy job completed with error: %w", status.Err())
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully copied table %s.%s to %s.%s", sourceDatasetID, sourceTableName, destDatasetID, destTableName)
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
	autoOptimize     bool // Auto-optimize for upserts by clustering on primary key
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

func WithAutoOptimizeForUpserts(v bool) func(*createTableOpts) {
	return func(opts *createTableOpts) {
		opts.autoOptimize = v
	}
}

type createDatasetOpts struct {
	overwrite   bool
	labels      map[string]string
	description string
}

type copyDatasetOpts struct {
	overwriteExisting bool
	skipExisting      bool
	concurrency       int
}

// WithCopyDatasetOverwrite overwrites existing tables in the destination dataset
func WithCopyDatasetOverwrite(v bool) func(*copyDatasetOpts) {
	return func(opts *copyDatasetOpts) {
		opts.overwriteExisting = v
		if v {
			opts.skipExisting = false
		}
	}
}

// WithCopyDatasetSkipExisting skips tables that already exist in the destination (default: true)
func WithCopyDatasetSkipExisting(v bool) func(*copyDatasetOpts) {
	return func(opts *copyDatasetOpts) {
		opts.skipExisting = v
	}
}

// WithCopyDatasetConcurrency sets the number of concurrent table copy operations (default: 4)
func WithCopyDatasetConcurrency(n int) func(*copyDatasetOpts) {
	return func(opts *copyDatasetOpts) {
		if n > 0 {
			opts.concurrency = n
		}
	}
}

type copyTableOpts struct {
	overwrite bool
}

// WithCopyTableOverwrite overwrites the destination table if it exists
func WithCopyTableOverwrite(v bool) func(*copyTableOpts) {
	return func(opts *copyTableOpts) {
		opts.overwrite = v
	}
}

type clusteringOpts struct {
	clusteringFields []string
	forceRecreate    bool
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

// Clustering option functions
func WithClusteringFieldsForTable(fields []string) func(*clusteringOpts) {
	return func(opts *clusteringOpts) {
		opts.clusteringFields = fields
	}
}

func WithForceRecreateClustering(v bool) func(*clusteringOpts) {
	return func(opts *clusteringOpts) {
		opts.forceRecreate = v
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

// SetupTables ensures all provided tables exist with proper schema, primary keys, and clustering.
// This is a reusable function that replaces duplicated BigQueryStartup logic across clients.
func (c *Client) SetupTables(tables []*Table) error {
	log.Log.Infof(c.ctx, "ensuring BQ tables exist...")

	if len(tables) == 0 {
		log.Log.Infof(c.ctx, "no tables to setup")
		return nil
	}

	// Use channels to collect results from goroutines
	type result struct {
		tableName string
		err       error
	}

	resultChan := make(chan result, len(tables))

	// Process each table in a separate goroutine
	for _, table := range tables {
		go func(t *Table) {
			tableName := t.Name
			log.Log.Infof(c.ctx, "starting BigQuery setup for table: %s", tableName)

			// Create table
			err := c.CreateTable(t)
			if err != nil {
				log.Log.Errorf(c.ctx, "failed to create BigQuery table %s: %v", tableName, err)
				resultChan <- result{tableName: tableName, err: err}
				return
			}
			log.Log.Infof(c.ctx, "BigQuery table %s exists", tableName)

			// Align table schema
			err = c.AlignTableSchema(t, WithAlignTableRecreation(false))
			if err != nil {
				log.Log.Errorf(c.ctx, "failed to align BigQuery table schema for %s: %v", tableName, err)
				resultChan <- result{tableName: tableName, err: err}
				return
			}
			log.Log.Infof(c.ctx, "BigQuery table schema aligned for %s", tableName)

			// Ensure clustering indices are created
			err = c.AddClusteringToTable(t, WithForceRecreateClustering(false))
			if err != nil {
				log.Log.Errorf(c.ctx, "failed to create BigQuery clustering indices for %s: %v", tableName, err)
				resultChan <- result{tableName: tableName, err: err}
				return
			}
			log.Log.Infof(c.ctx, "BigQuery clustering indices created for %s", tableName)

			// Success - send nil error
			resultChan <- result{tableName: tableName, err: nil}
		}(table)
	}

	// Collect results and check for errors
	var errors []error
	for i := 0; i < len(tables); i++ {
		res := <-resultChan
		if res.err != nil {
			errors = append(errors, fmt.Errorf("table %s: %w", res.tableName, res.err))
		}
	}

	// Return combined error if any occurred
	if len(errors) > 0 {
		return fmt.Errorf("BigQuery startup failed for %d table(s): %v", len(errors), errors)
	}

	log.Log.Infof(c.ctx, "all BigQuery tables setup completed successfully")
	return nil
}

// WriteRowsToBigQuery is a generic function that writes rows to BigQuery with concurrency control and progress tracking.
// T must implement the bigquery.Row interface.
// logStartMsg and logResultMsg are functions that generate log messages for each row.
func WriteRowsToBigQuery[T Row](
	ctx context.Context,
	bigqueryClient *Client,
	rows []T,
	logStartMsg func(row T, percentage float64) string,
	logResultMsg func(row T, percentage float64, isError bool) string,
) error {
	// Create a semaphore to limit concurrent DML operations to 2
	semaphore := make(chan struct{}, 2)

	// Use atomic counter to track completed items
	var completed int64
	total := int64(len(rows))

	// Use channels to collect results from goroutines
	type result struct {
		index int
		err   error
	}

	resultChan := make(chan result, len(rows))

	// Process each row in a separate goroutine with concurrency limit
	for idx, row := range rows {
		go func(index int, r T) {
			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }() // Release semaphore

			// Calculate percentage before processing
			currentCompleted := atomic.AddInt64(&completed, 0)
			percentage := float64(currentCompleted) / float64(total) * 100
			log.Log.Infof(ctx, logStartMsg(r, percentage))

			r.SetMetadata()
			err := bigqueryClient.UpsertRows(r) // Now has built-in retry logic

			// Increment completed counter and log completion
			newCompleted := atomic.AddInt64(&completed, 1)
			newPercentage := float64(newCompleted) / float64(total) * 100

			if err != nil {
				log.Log.Errorf(ctx, logResultMsg(r, newPercentage, true))
			} else {
				log.Log.Infof(ctx, logResultMsg(r, newPercentage, false))
			}
			resultChan <- result{index: index, err: err}
		}(idx, row)
	}

	// Collect results and check for errors
	for i := 0; i < len(rows); i++ {
		res := <-resultChan
		if res.err != nil {
			return res.err
		}
	}

	return nil
}
