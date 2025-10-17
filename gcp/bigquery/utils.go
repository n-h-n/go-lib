package bigquery

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
)

// normalizeBigQueryType normalizes BigQuery type names for comparison
// BigQuery uses different type names in different contexts (schema vs API)
func normalizeBigQueryType(t bigqueryType) bigqueryType {
	switch t {
	case TypeInteger:
		return TypeInt64 // INTEGER -> INT64
	case TypeFloat:
		return TypeFloat64 // FLOAT -> FLOAT64
	default:
		return t // No normalization needed
	}
}

// hasJSONTags checks if a struct has any fields with JSON tags
func hasJSONTags(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if _, ok := field.Tag.Lookup("json"); ok {
			return true
		}
	}
	return false
}

// deserializeJSONToStruct attempts to deserialize JSON data into a struct with JSON tags
func deserializeJSONToStruct(data interface{}, targetType reflect.Type) (interface{}, error) {
	if targetType.Kind() != reflect.Struct {
		return data, nil
	}

	// Convert data to JSON bytes
	var jsonBytes []byte
	var err error

	switch v := data.(type) {
	case string:
		jsonBytes = []byte(v)
	case []byte:
		jsonBytes = v
	default:
		jsonBytes, err = json.Marshal(data)
		if err != nil {
			return data, fmt.Errorf("failed to marshal data to JSON: %w", err)
		}
	}

	// Create a new instance of the target type
	targetValue := reflect.New(targetType).Interface()

	// Unmarshal JSON into the struct
	err = json.Unmarshal(jsonBytes, targetValue)
	if err != nil {
		return data, fmt.Errorf("failed to unmarshal JSON to struct: %w", err)
	}

	// Return the value (not the pointer)
	return reflect.ValueOf(targetValue).Elem().Interface(), nil
}

// DeserializeJSONField attempts to deserialize a JSON field from BigQuery into a Go struct
// This is useful when reading JSON columns back into structs with JSON tags
func DeserializeJSONField(data interface{}, targetType reflect.Type) (interface{}, error) {
	return deserializeJSONToStruct(data, targetType)
}

// CustomJSONUnmarshaler provides custom unmarshaling for BigQuery JSON fields
type CustomJSONUnmarshaler struct {
	// TypeRegistry maps field names to their expected Go types
	TypeRegistry map[string]reflect.Type
}

// NewCustomJSONUnmarshaler creates a new CustomJSONUnmarshaler
func NewCustomJSONUnmarshaler() *CustomJSONUnmarshaler {
	return &CustomJSONUnmarshaler{
		TypeRegistry: make(map[string]reflect.Type),
	}
}

// RegisterType registers a field name with its expected Go type
func (c *CustomJSONUnmarshaler) RegisterType(fieldName string, goType reflect.Type) {
	c.TypeRegistry[fieldName] = goType
}

// UnmarshalJSONField unmarshals a JSON field to the appropriate Go type
func (c *CustomJSONUnmarshaler) UnmarshalJSONField(fieldName string, data interface{}) (interface{}, error) {
	// Check if we have a registered type for this field
	if targetType, exists := c.TypeRegistry[fieldName]; exists {
		return c.unmarshalToType(data, targetType)
	}

	// If no specific type registered, return as-is
	return data, nil
}

// unmarshalToType handles the actual unmarshaling to a specific type
func (c *CustomJSONUnmarshaler) unmarshalToType(data interface{}, targetType reflect.Type) (interface{}, error) {
	if data == nil {
		return reflect.Zero(targetType).Interface(), nil
	}

	// Handle different target types
	switch targetType.Kind() {
	case reflect.Slice:
		return c.unmarshalToSlice(data, targetType)
	case reflect.Ptr:
		return c.unmarshalToPointer(data, targetType)
	case reflect.Struct:
		return c.unmarshalToStruct(data, targetType)
	case reflect.Map:
		return c.unmarshalToMap(data, targetType)
	case reflect.Interface:
		return data, nil // Keep as interface{}
	default:
		return c.unmarshalToBasicType(data, targetType)
	}
}

// unmarshalToSlice handles unmarshaling to slice types (including []*struct)
func (c *CustomJSONUnmarshaler) unmarshalToSlice(data interface{}, targetType reflect.Type) (interface{}, error) {
	// Convert data to JSON bytes first
	jsonBytes, err := c.dataToJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to JSON bytes: %w", err)
	}

	// Create a slice of the target type
	sliceType := targetType
	sliceValue := reflect.MakeSlice(sliceType, 0, 0)

	// Unmarshal JSON array into the slice
	err = json.Unmarshal(jsonBytes, sliceValue.Addr().Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to slice: %w", err)
	}

	return sliceValue.Interface(), nil
}

// unmarshalToPointer handles unmarshaling to pointer types
func (c *CustomJSONUnmarshaler) unmarshalToPointer(data interface{}, targetType reflect.Type) (interface{}, error) {
	// Convert data to JSON bytes first
	jsonBytes, err := c.dataToJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to JSON bytes: %w", err)
	}

	// Create a new instance of the target type
	targetValue := reflect.New(targetType.Elem())

	// Unmarshal JSON into the struct
	err = json.Unmarshal(jsonBytes, targetValue.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to pointer: %w", err)
	}

	return targetValue.Interface(), nil
}

// unmarshalToStruct handles unmarshaling to struct types
func (c *CustomJSONUnmarshaler) unmarshalToStruct(data interface{}, targetType reflect.Type) (interface{}, error) {
	// Convert data to JSON bytes first
	jsonBytes, err := c.dataToJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to JSON bytes: %w", err)
	}

	// Create a new instance of the target type
	targetValue := reflect.New(targetType)

	// Unmarshal JSON into the struct
	err = json.Unmarshal(jsonBytes, targetValue.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to struct: %w", err)
	}

	// Return the value (not the pointer)
	return targetValue.Elem().Interface(), nil
}

// unmarshalToMap handles unmarshaling to map types
func (c *CustomJSONUnmarshaler) unmarshalToMap(data interface{}, targetType reflect.Type) (interface{}, error) {
	// Convert data to JSON bytes first
	jsonBytes, err := c.dataToJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to JSON bytes: %w", err)
	}

	// Create a new map of the target type
	mapValue := reflect.MakeMap(targetType)

	// Unmarshal JSON into the map
	err = json.Unmarshal(jsonBytes, mapValue.Addr().Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	return mapValue.Interface(), nil
}

// unmarshalToBasicType handles unmarshaling to basic types
func (c *CustomJSONUnmarshaler) unmarshalToBasicType(data interface{}, targetType reflect.Type) (interface{}, error) {
	// For basic types, try to convert directly
	value := reflect.ValueOf(data)
	if value.Type().ConvertibleTo(targetType) {
		return value.Convert(targetType).Interface(), nil
	}

	// If direct conversion fails, try JSON unmarshaling
	jsonBytes, err := c.dataToJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to JSON bytes: %w", err)
	}

	targetValue := reflect.New(targetType)
	err = json.Unmarshal(jsonBytes, targetValue.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to basic type: %w", err)
	}

	return targetValue.Elem().Interface(), nil
}

// dataToJSONBytes converts various data types to JSON bytes
func (c *CustomJSONUnmarshaler) dataToJSONBytes(data interface{}) ([]byte, error) {
	switch v := data.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case nil:
		return []byte("null"), nil
	default:
		return json.Marshal(data)
	}
}

// UnmarshalBigQueryRow unmarshals a BigQuery row with custom JSON field handling
func (c *CustomJSONUnmarshaler) UnmarshalBigQueryRow(row map[string]bigquery.Value, targetStruct interface{}) error {
	targetValue := reflect.ValueOf(targetStruct)
	if targetValue.Kind() != reflect.Ptr || targetValue.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to a struct")
	}

	targetValue = targetValue.Elem()
	targetType := targetValue.Type()

	// Iterate through struct fields
	for i := 0; i < targetType.NumField(); i++ {
		field := targetType.Field(i)
		fieldValue := targetValue.Field(i)

		// Get the field name from BigQuery tag or use field name
		fieldName := c.getFieldName(field)
		if fieldName == "" {
			continue
		}

		// Get the value from BigQuery row
		bigqueryValue, exists := row[fieldName]
		if !exists {
			continue
		}

		// Handle JSON fields specially
		if c.isJSONField(field) {
			unmarshaledValue, err := c.UnmarshalJSONField(fieldName, bigqueryValue)
			if err != nil {
				return fmt.Errorf("failed to unmarshal JSON field %s: %w", fieldName, err)
			}

			// Set the unmarshaled value
			if err := c.setFieldValue(fieldValue, unmarshaledValue); err != nil {
				return fmt.Errorf("failed to set field %s: %w", fieldName, err)
			}
		} else {
			// Handle non-JSON fields normally
			if err := c.setFieldValue(fieldValue, bigqueryValue); err != nil {
				return fmt.Errorf("failed to set field %s: %w", fieldName, err)
			}
		}
	}

	return nil
}

// getFieldName extracts the field name from struct tags
func (c *CustomJSONUnmarshaler) getFieldName(field reflect.StructField) string {
	// Check for bigquery tag first
	if bigqueryTag := field.Tag.Get("bigquery"); bigqueryTag != "" {
		return strings.Split(bigqueryTag, ",")[0]
	}

	// Check for json tag
	if jsonTag := field.Tag.Get("json"); jsonTag != "" {
		jsonName := strings.Split(jsonTag, ",")[0]
		if jsonName != "-" {
			return jsonName
		}
	}

	// Use field name as fallback
	return field.Name
}

// isJSONField checks if a field should be treated as JSON
func (c *CustomJSONUnmarshaler) isJSONField(field reflect.StructField) bool {
	// Check if field type is JSON-compatible
	fieldType := field.Type

	// Check for JSON tag
	if jsonTag := field.Tag.Get("json"); jsonTag != "" {
		return true
	}

	// Check if type is complex (struct, slice, map, interface)
	switch fieldType.Kind() {
	case reflect.Struct:
		// Check if struct has JSON tags
		return hasJSONTags(fieldType)
	case reflect.Slice, reflect.Map, reflect.Interface:
		return true
	case reflect.Ptr:
		// Check if pointer points to a struct with JSON tags
		if fieldType.Elem().Kind() == reflect.Struct {
			return hasJSONTags(fieldType.Elem())
		}
	}

	return false
}

// setFieldValue sets a value to a struct field
func (c *CustomJSONUnmarshaler) setFieldValue(fieldValue reflect.Value, value interface{}) error {
	if !fieldValue.CanSet() {
		return fmt.Errorf("field is not settable")
	}

	if value == nil {
		fieldValue.Set(reflect.Zero(fieldValue.Type()))
		return nil
	}

	valueReflect := reflect.ValueOf(value)

	// Try direct assignment first
	if valueReflect.Type().AssignableTo(fieldValue.Type()) {
		fieldValue.Set(valueReflect)
		return nil
	}

	// Try conversion
	if valueReflect.Type().ConvertibleTo(fieldValue.Type()) {
		fieldValue.Set(valueReflect.Convert(fieldValue.Type()))
		return nil
	}

	return fmt.Errorf("cannot assign value of type %s to field of type %s", valueReflect.Type(), fieldValue.Type())
}

// Convenience functions for common JSON field types

// RegisterDisputesType registers the disputes field type for []*struct patterns
func (c *CustomJSONUnmarshaler) RegisterDisputesType(disputeStructType reflect.Type) {
	c.RegisterType("disputes", reflect.SliceOf(reflect.PointerTo(disputeStructType)))
}

// RegisterJSONFieldType is a generic function to register any JSON field type
func (c *CustomJSONUnmarshaler) RegisterJSONFieldType(fieldName string, fieldType reflect.Type) {
	c.RegisterType(fieldName, fieldType)
}

// UnmarshalJSONToSlice unmarshals JSON data directly to a slice type
func UnmarshalJSONToSlice(data interface{}, sliceType reflect.Type) (interface{}, error) {
	unmarshaler := NewCustomJSONUnmarshaler()
	return unmarshaler.unmarshalToSlice(data, sliceType)
}

// UnmarshalJSONToStruct unmarshals JSON data directly to a struct type
func UnmarshalJSONToStruct(data interface{}, structType reflect.Type) (interface{}, error) {
	unmarshaler := NewCustomJSONUnmarshaler()
	return unmarshaler.unmarshalToStruct(data, structType)
}

// UnmarshalJSONToPointer unmarshals JSON data directly to a pointer type
func UnmarshalJSONToPointer(data interface{}, pointerType reflect.Type) (interface{}, error) {
	unmarshaler := NewCustomJSONUnmarshaler()
	return unmarshaler.unmarshalToPointer(data, pointerType)
}

// CreateJSONFieldTypeMap creates a map of field names to types for common patterns
func CreateJSONFieldTypeMap(fieldTypes map[string]reflect.Type) map[string]reflect.Type {
	return fieldTypes
}

// Example usage functions for common patterns

// ExampleDisputeStruct represents a typical dispute struct that might be used in JSON fields
type ExampleDisputeStruct struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	ID        string    `json:"id,omitempty"`
	Memo      string    `json:"memo,omitempty"`
	Type      string    `json:"type,omitempty"`
}

// GetDisputesType returns the reflect.Type for []*ExampleDisputeStruct
func GetDisputesType() reflect.Type {
	return reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(ExampleDisputeStruct{})))
}

// CreateDisputesUnmarshaler creates a pre-configured unmarshaler for disputes fields
func CreateDisputesUnmarshaler(disputeStructType reflect.Type) *CustomJSONUnmarshaler {
	unmarshaler := NewCustomJSONUnmarshaler()
	unmarshaler.RegisterDisputesType(disputeStructType)
	return unmarshaler
}

// goTypeToBigQueryType converts Go types to BigQuery types
func goTypeToBigQueryType(t reflect.Type) (bigqueryType, error) {
	switch t.Kind() {
	case reflect.Bool:
		return TypeBoolean, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return TypeInt64, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return TypeInt64, nil
	case reflect.Float32, reflect.Float64:
		return TypeFloat64, nil
	case reflect.String:
		return TypeString, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return TypeBytes, nil
		}
		// Check if this is a slice of pointers to structs (e.g., []*MyStruct)
		if t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct {
			return TypeJSON, nil
		}
		// For other slice types, return the element type (Repeated flag will be set separately)
		elemType, err := goTypeToBigQueryType(t.Elem())
		if err != nil {
			return "", err
		}
		return elemType, nil
	case reflect.Map, reflect.Interface:
		return TypeJSON, nil
	case reflect.Ptr:
		// Handle pointers to structs as JSON
		if t.Elem().Kind() == reflect.Struct {
			return TypeJSON, nil
		}
		// For other pointer types, dereference and recurse
		return goTypeToBigQueryType(t.Elem())
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return TypeTimestamp, nil
		}
		// Check if struct has JSON tags - if so, treat as JSON
		if hasJSONTags(t) {
			return TypeJSON, nil
		}
		// For other structs, treat as RECORD
		return TypeRecord, nil
	}

	return "", fmt.Errorf("unable to map Go type to BigQuery type: %s", t.String())
}

// bigQueryTypeToGoType converts BigQuery types to Go types
func bigQueryTypeToGoType(t bigqueryType) (reflect.Type, error) {
	if strings.HasSuffix(string(t), "[]") {
		// This is an old array type format (for backward compatibility)
		elementType := t[:len(t)-2]
		elementGoType, err := bigQueryTypeToGoType(elementType)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elementGoType), nil
	}

	switch t {
	case TypeBoolean:
		return reflect.TypeOf(false), nil
	case TypeInt64, TypeInteger:
		return reflect.TypeOf(int64(0)), nil
	case TypeFloat64, TypeFloat:
		return reflect.TypeOf(float64(0)), nil
	case TypeString:
		return reflect.TypeOf(""), nil
	case TypeBytes:
		return reflect.TypeOf([]byte{}), nil
	case TypeTimestamp:
		return reflect.TypeOf(time.Time{}), nil
	case TypeDate:
		return reflect.TypeOf(time.Time{}), nil
	case TypeTime:
		return reflect.TypeOf(time.Time{}), nil
	case TypeDatetime:
		return reflect.TypeOf(time.Time{}), nil
	case TypeJSON:
		return reflect.TypeOf(map[string]interface{}{}), nil
	case TypeRecord:
		return reflect.TypeOf(map[string]interface{}{}), nil
	}

	return nil, fmt.Errorf("unable to map BigQuery type to Go type: %s", t)
}

// bigQueryDefaultValue returns default values for BigQuery types
func bigQueryDefaultValue(t reflect.Type) (interface{}, error) {
	switch t.Kind() {
	case reflect.Bool:
		return false, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int64(0), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(0), nil
	case reflect.Float32, reflect.Float64:
		return float64(0), nil
	case reflect.String:
		return "", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return []byte{}, nil
		}
		return []interface{}{}, nil
	case reflect.Map, reflect.Interface:
		return map[string]interface{}{}, nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return time.Time{}, nil
		}
		return map[string]interface{}{}, nil
	}

	return nil, fmt.Errorf("unable to map Go type to BigQuery default value: %s", t.String())
}

// GetColumns extracts columns from a struct using db tags, similar to PostgreSQL version
func GetColumns(s interface{}) (*map[string]Column, string) {
	columns := make(map[string]Column)
	pkey := ""

	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	} else if v.Kind() == reflect.Ptr && v.IsNil() {
		return &columns, pkey
	}

	if v.Kind() != reflect.Struct {
		return &columns, pkey
	}

	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbTag, tagOk := field.Tag.Lookup("db")
		if !tagOk || dbTag == "" {
			continue
		}

		var c Column

		tagParts := strings.Split(dbTag, ",")
		if tagParts[0] == "" {
			continue
		}

		c.Name = tagParts[0]
		c.Type, _ = goTypeToBigQueryType(field.Type)
		c.Mode = "NULLABLE"
		c.Value = v.Field(i).Interface()

		if len(tagParts) > 1 {
			for _, tagPart := range tagParts[1:] {
				if tagPart == "primarykey" {
					c.Mode = "REQUIRED"
					pkey = c.Name
				}
				if tagPart == "nonnullable" || tagPart == "required" {
					c.Mode = "REQUIRED"
				}
				if tagPart == "nullable" {
					c.Mode = "NULLABLE"
				}
				if tagPart == "repeated" {
					c.Mode = "REPEATED"
				}
			}
		}

		// Override mode for slice types - slices should always be REPEATED unless they contain complex types
		if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() != reflect.Uint8 {
			elemType := field.Type.Elem()

			// Check if element type is a basic type (not complex)
			isBasicType := elemType.Kind() == reflect.String ||
				elemType.Kind() == reflect.Int || elemType.Kind() == reflect.Int64 ||
				elemType.Kind() == reflect.Float32 || elemType.Kind() == reflect.Float64 ||
				elemType.Kind() == reflect.Bool ||
				(elemType.Kind() == reflect.Ptr &&
					(elemType.Elem().Kind() == reflect.String ||
						elemType.Elem().Kind() == reflect.Int || elemType.Elem().Kind() == reflect.Int64 ||
						elemType.Elem().Kind() == reflect.Float32 || elemType.Elem().Kind() == reflect.Float64 ||
						elemType.Elem().Kind() == reflect.Bool)) // *string, *int, *int64, *float32, *float64, *bool

			if isBasicType {
				c.Mode = "REPEATED"
			}
			// Otherwise, leave it as JSON (for complex types like []*struct{}, [][]string, etc.)
		}

		columns[tagParts[0]] = c
	}

	err := ValidateColumns(&columns)
	if err != nil {
		panic(err)
	}

	return &columns, pkey
}

// ValidateColumns validates column definitions
func ValidateColumns(columns *map[string]Column) error {
	pkey := ""
	for _, c := range *columns {
		if c.Mode == "REQUIRED" && strings.Contains(string(c.Type), "primarykey") {
			if pkey != "" {
				return fmt.Errorf("multiple primary keys defined on both %s and %s", pkey, c.Name)
			}
			pkey = c.Name
		}
	}

	return nil
}

// createIndexName creates a unique index name for BigQuery clustering
func createIndexName(table *Table, cols *[]Column) (string, error) {
	if table == nil || table.Name == "" || cols == nil {
		return "", fmt.Errorf("to create an index name, the table name and columns must be provided and not zero values")
	}

	colNames := []string{}
	for _, col := range *cols {
		colNames = append(colNames, col.Name)
	}
	slices.Sort(colNames)

	// create a unique hash of the column names
	h := sha256.New()
	h.Write([]byte(strings.Join(colNames, "_")))
	hash := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(h.Sum(nil))[:30]

	// shorten to 63 characters max
	indexName := fmt.Sprintf("%s_%s_cluster", table.Name, hash)

	if len(indexName) > 63 {
		indexName = indexName[:63]
	}
	return indexName, nil
}

// convertToBigQuerySchema converts our Table structure to BigQuery Schema
func convertToBigQuerySchema(table *Table) (*Schema, error) {
	if table == nil || table.Columns == nil {
		return nil, fmt.Errorf("table and columns cannot be nil")
	}

	schema := &Schema{
		Fields: make([]*SchemaField, 0, len(*table.Columns)),
	}

	for _, column := range *table.Columns {
		field := &SchemaField{
			Name:        column.Name,
			Type:        column.Type,
			Mode:        column.Mode,
			Description: column.Description,
		}

		// Handle nested fields for RECORD types
		if column.Type == TypeRecord {
			// This would need to be expanded based on your specific needs
			field.Fields = []*SchemaField{}
		}

		schema.Fields = append(schema.Fields, field)
	}

	return schema, nil
}

// convertFromBigQuerySchema converts BigQuery Schema to our Table structure
func convertFromBigQuerySchema(schema *Schema) (*Table, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	columns := make(map[string]Column)
	pkey := ""

	for _, field := range schema.Fields {
		column := Column{
			Name:        field.Name,
			Type:        field.Type,
			Mode:        field.Mode,
			Description: field.Description,
		}

		// Determine if this is a primary key (REQUIRED mode in BigQuery)
		if field.Mode == "REQUIRED" {
			pkey = field.Name
		}

		columns[field.Name] = column
	}

	table := &Table{
		Name:           "", // Will be set by caller
		Columns:        &columns,
		PrimaryKeyName: pkey,
	}

	return table, nil
}

// escapeIdentifier escapes BigQuery identifiers
func escapeIdentifier(identifier string) string {
	// BigQuery uses backticks for identifiers
	return "`" + strings.ReplaceAll(identifier, "`", "\\`") + "`"
}

// buildWhereClause builds a WHERE clause from conditions
func buildWhereClause(conditions map[string]interface{}) string {
	if len(conditions) == 0 {
		return ""
	}

	var clauses []string
	for key := range conditions {
		clause := fmt.Sprintf("%s = @%s", escapeIdentifier(key), key)
		clauses = append(clauses, clause)
	}

	return "WHERE " + strings.Join(clauses, " AND ")
}

// buildOrderByClause builds an ORDER BY clause
func buildOrderByClause(orderBy []string) string {
	if len(orderBy) == 0 {
		return ""
	}

	var clauses []string
	for _, field := range orderBy {
		clauses = append(clauses, escapeIdentifier(field))
	}

	return "ORDER BY " + strings.Join(clauses, ", ")
}

// buildLimitClause builds a LIMIT clause
func buildLimitClause(limit int) string {
	if limit <= 0 {
		return ""
	}

	return fmt.Sprintf("LIMIT %d", limit)
}

// validateTableName validates BigQuery table names
func validateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// BigQuery table names must be 1-1024 characters and contain only letters, numbers, and underscores
	if len(tableName) > 1024 {
		return fmt.Errorf("table name too long: %d characters (max 1024)", len(tableName))
	}

	// Check for valid characters (letters, numbers, underscores)
	for _, char := range tableName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return fmt.Errorf("invalid character in table name: %c", char)
		}
	}

	return nil
}

// validateDatasetName validates BigQuery dataset names
func validateDatasetName(datasetName string) error {
	if datasetName == "" {
		return fmt.Errorf("dataset name cannot be empty")
	}

	// BigQuery dataset names must be 1-1024 characters and contain only letters, numbers, and underscores
	if len(datasetName) > 1024 {
		return fmt.Errorf("dataset name too long: %d characters (max 1024)", len(datasetName))
	}

	// Check for valid characters (letters, numbers, underscores)
	for _, char := range datasetName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return fmt.Errorf("invalid character in dataset name: %c", char)
		}
	}

	return nil
}

// formatBigQueryValue formats a value for BigQuery SQL
func formatBigQueryValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "\\'"))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case time.Time:
		return fmt.Sprintf("TIMESTAMP('%s')", v.Format(time.RFC3339))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}
