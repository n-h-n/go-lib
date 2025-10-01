package bigquery

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

// CreateTable creates a BigQuery table
func (c *Client) CreateTable(table *Table, opts ...func(*createTableOpts)) error {
	if table == nil {
		return fmt.Errorf("unable to create table: table cannot be nil")
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

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating table %s in dataset %s", table.Name, c.datasetID)
	}

	if createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "overwrite mode enabled; dropping table %s in %s", table.Name, c.datasetID)
		}
		err := c.DropTable(table.Name)
		if err != nil {
			return err
		}
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
	}

	// Check if schema is aligned
	aligned, err := c.IsSchemaAligned(table, opts...)
	if err != nil {
		return fmt.Errorf("could not check if schema is aligned: %w", err)
	}

	if !aligned {
		if err = c.AlterTableAddColumns(table); err != nil {
			return fmt.Errorf("could not add columns: %w", err)
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

		// Check type alignment
		if expectedCol.Type != currentCol.Type {
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

	var newFields []*bigquery.FieldSchema
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
			newFields = append(newFields, field)
		}
	}

	if len(newFields) == 0 {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "no new columns to add to table %s", table.Name)
		}
		return nil
	}

	// Update table schema
	tableRef := c.GetTableReference(table.Name)
	update := bigquery.TableMetadataToUpdate{
		Schema: newFields,
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

		// Prepare data for insertion
		cols := row.Columns()
		colNames := utils.GetKeysFromMap(*cols)
		values := make([]bigquery.Value, len(colNames))

		for i, colName := range colNames {
			col := (*cols)[colName]
			values[i] = col.Value
		}

		// Insert data
		tableRef := c.GetTableReference(table.Name)
		inserter := tableRef.Inserter()

		// Create a map for the row data
		rowData := make(map[string]bigquery.Value)
		for colName, col := range *cols {
			rowData[colName] = col.Value
		}

		err = inserter.Put(c.ctx, []map[string]bigquery.Value{rowData})
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
		values := []interface{}{}
		whereClause := ""

		for _, colName := range colNames {
			col := (*cols)[colName]
			if col.Mode == "REQUIRED" { // Primary key equivalent
				if whereClause != "" {
					whereClause += " AND "
				}
				whereClause += fmt.Sprintf("%s = @%s", colName, colName)
				values = append(values, col.Value)
			}
		}

		if whereClause == "" {
			return fmt.Errorf("failed to delete row (%s) in table (%s): no primary key found", strings.Join(colNames, ", "), table.Name)
		}

		query := fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE %s", c.projectID, c.datasetID, table.Name, whereClause)

		// Execute DELETE query
		err = c.ExecuteDML(c.ctx, query)
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
	if err == nil && !createOpts.overwrite {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "dataset %s already exists, overwrite option is false, skipping", datasetID)
		}
		return nil
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

	if replace {
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

	err := viewRef.Create(c.ctx, viewMetadata)
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

// CreateIndex creates clustering for a BigQuery table
func (c *Client) CreateIndex(table *Table, cols *[]Column, opts ...func(*indexOpts)) error {
	o := indexOpts{
		recreate: false,
		typ:      IndexTypeClustering,
	}

	for _, opt := range opts {
		opt(&o)
	}

	if table == nil || table.Name == "" {
		return fmt.Errorf("unable to create index: table and table.Name cannot be zero values")
	}

	if len(*cols) == 0 {
		return fmt.Errorf("unable to create index: no columns to index provided")
	}

	// Check if clustering already exists
	exists, err := c.IsIndexExistent(table, cols)
	if err != nil {
		return fmt.Errorf("could not check if index exists: %w", err)
	}

	if exists && !o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "clustering already exists for table %s, option to recreate not specified, skipping", table.Name)
		}
		return nil
	}

	if exists && o.recreate {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "recreating clustering for table %s", table.Name)
		}
		if err = c.DropIndex(table, cols); err != nil {
			return fmt.Errorf("could not drop index: %w", err)
		}
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "creating clustering for table %s", table.Name)
	}

	// Get column names for clustering
	columnNames := []string{}
	for _, col := range *cols {
		columnNames = append(columnNames, col.Name)
	}

	// Update table with clustering
	tableRef := c.GetTableReference(table.Name)
	update := bigquery.TableMetadataToUpdate{
		Clustering: &bigquery.Clustering{
			Fields: columnNames,
		},
	}

	_, err = tableRef.Update(c.ctx, update, "")
	if err != nil {
		return fmt.Errorf("could not create clustering: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "created clustering for table %s", table.Name)
	}

	return nil
}

// DropIndex drops clustering from a BigQuery table
func (c *Client) DropIndex(table *Table, cols *[]Column) error {
	if table == nil || table.Name == "" {
		return fmt.Errorf("unable to drop index: table and table.Name cannot be zero values")
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropping clustering for table %s", table.Name)
	}

	// Remove clustering by setting it to nil
	tableRef := c.GetTableReference(table.Name)
	update := bigquery.TableMetadataToUpdate{
		Clustering: &bigquery.Clustering{}, // Empty clustering removes it
	}

	_, err := tableRef.Update(c.ctx, update, "")
	if err != nil {
		return fmt.Errorf("could not drop clustering: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "dropped clustering for table %s", table.Name)
	}

	return nil
}

// IsIndexExistent checks if clustering exists for a table
func (c *Client) IsIndexExistent(table *Table, cols *[]Column) (bool, error) {
	if table == nil || table.Name == "" {
		return false, fmt.Errorf("unable to check if index exists: table and table.Name cannot be zero values")
	}

	tableRef := c.GetTableReference(table.Name)
	metadata, err := tableRef.Metadata(c.ctx)
	if err != nil {
		return false, err
	}

	// Check if clustering exists and matches the requested columns
	if metadata.Clustering == nil || len(metadata.Clustering.Fields) == 0 {
		return false, nil
	}

	// Check if all requested columns are in the clustering
	requestedColumns := make(map[string]bool)
	for _, col := range *cols {
		requestedColumns[col.Name] = true
	}

	for _, field := range metadata.Clustering.Fields {
		if !requestedColumns[field] {
			return false, nil
		}
	}

	return true, nil
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
	recreate bool
	typ      indexType
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
