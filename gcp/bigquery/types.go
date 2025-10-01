package bigquery

import (
	"reflect"
	"time"
)

const (
	// BigQuery data types
	TypeString     bigqueryType = "STRING"
	TypeBytes      bigqueryType = "BYTES"
	TypeInteger    bigqueryType = "INTEGER"
	TypeInt64      bigqueryType = "INT64"
	TypeFloat      bigqueryType = "FLOAT"
	TypeFloat64    bigqueryType = "FLOAT64"
	TypeNumeric    bigqueryType = "NUMERIC"
	TypeBignumeric bigqueryType = "BIGNUMERIC"
	TypeBoolean    bigqueryType = "BOOLEAN"
	TypeTimestamp  bigqueryType = "TIMESTAMP"
	TypeDate       bigqueryType = "DATE"
	TypeTime       bigqueryType = "TIME"
	TypeDatetime   bigqueryType = "DATETIME"
	TypeGeography  bigqueryType = "GEOGRAPHY"
	TypeJSON       bigqueryType = "JSON"
	TypeRecord     bigqueryType = "RECORD"
	TypeArray      bigqueryType = "ARRAY"
)

// Row interface for BigQuery rows - similar to PostgreSQL Row interface
type Row interface {
	Table() *Table
	Columns() *map[string]Column
	SetMetadata()
}

// Table represents a BigQuery table
type Table struct {
	Name           string
	Columns        *map[string]Column
	PrimaryKeyName string
	Description    string
	Labels         map[string]string
}

// Column represents a BigQuery column
type Column struct {
	Name        string
	Type        bigqueryType
	Mode        string // NULLABLE, REQUIRED, REPEATED
	Description string
	Value       interface{}
}

// JoinColumnPair represents columns used in joins
type JoinColumnPair struct {
	LeftColumnName  string
	RightColumnName string
}

// JoinResultantColumns represents the result of a join operation
type JoinResultantColumns struct {
	Columns map[string]reflect.Type
}

// QueryJob represents a BigQuery query job
type QueryJob struct {
	JobID      string
	Query      string
	JobType    string
	State      string
	CreateTime time.Time
	StartTime  time.Time
	EndTime    time.Time
}

// Dataset represents a BigQuery dataset
type Dataset struct {
	DatasetID   string
	ProjectID   string
	Location    string
	Labels      map[string]string
	Created     time.Time
	Modified    time.Time
	Description string
}

// PartitioningConfig represents table partitioning configuration
type PartitioningConfig struct {
	Type         string // TIME, RANGE, INTEGER_RANGE
	Field        string
	ExpirationMs int64
}

// ClusteringConfig represents table clustering configuration
type ClusteringConfig struct {
	Fields []string
}

// TimePartitioning represents time-based partitioning
type TimePartitioning struct {
	Type         string
	Field        string
	ExpirationMs int64
}

// RangePartitioning represents range-based partitioning
type RangePartitioning struct {
	Field string
	Range *RangeValue
}

// RangeValue represents a range of values for partitioning
type RangeValue struct {
	Start    string
	End      string
	Interval string
}

// EncryptionConfig represents encryption configuration
type EncryptionConfig struct {
	KMSKeyName string
}

// ExternalDataConfig represents external data source configuration
type ExternalDataConfig struct {
	SourceFormat string
	SourceURIs   []string
	Schema       *Schema
	Options      map[string]interface{}
}

// Schema represents BigQuery table schema
type Schema struct {
	Fields []*SchemaField
}

// SchemaField represents a field in BigQuery schema
type SchemaField struct {
	Name        string
	Type        bigqueryType
	Mode        string
	Description string
	Fields      []*SchemaField // For nested fields
}

// JobConfiguration represents BigQuery job configuration
type JobConfiguration struct {
	Query      *QueryConfiguration
	Load       *LoadConfiguration
	Extract    *ExtractConfiguration
	Copy       *CopyConfiguration
	DryRun     bool
	JobTimeout int
}

// QueryConfiguration represents query job configuration
type QueryConfiguration struct {
	Query              string
	UseLegacySQL       bool
	UseQueryCache      bool
	WriteDisposition   string
	CreateDisposition  string
	DestinationTable   *TableReference
	DefaultDataset     *DatasetReference
	Priority           string
	MaximumBytesBilled int64
}

// LoadConfiguration represents load job configuration
type LoadConfiguration struct {
	SourceURIs        []string
	DestinationTable  *TableReference
	Schema            *Schema
	WriteDisposition  string
	CreateDisposition string
	SourceFormat      string
	SkipLeadingRows   int64
	MaxBadRecords     int64
}

// ExtractConfiguration represents extract job configuration
type ExtractConfiguration struct {
	SourceTable       *TableReference
	DestinationURIs   []string
	DestinationFormat string
	Compression       string
	FieldDelimiter    string
	PrintHeader       bool
}

// CopyConfiguration represents copy job configuration
type CopyConfiguration struct {
	SourceTables      []*TableReference
	DestinationTable  *TableReference
	WriteDisposition  string
	CreateDisposition string
}

// TableReference represents a reference to a BigQuery table
type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

// DatasetReference represents a reference to a BigQuery dataset
type DatasetReference struct {
	ProjectID string
	DatasetID string
}

// JobStatus represents the status of a BigQuery job
type JobStatus struct {
	State      string
	Error      *ErrorProto
	Errors     []*ErrorProto
	Statistics *JobStatistics
}

// ErrorProto represents a BigQuery error
type ErrorProto struct {
	Location  string
	Message   string
	Reason    string
	DebugInfo string
}

// JobStatistics represents job statistics
type JobStatistics struct {
	CreationTime        time.Time
	StartTime           time.Time
	EndTime             time.Time
	TotalBytesProcessed int64
	TotalSlotMs         int64
	Query               *QueryStatistics
}

// QueryStatistics represents query-specific statistics
type QueryStatistics struct {
	TotalBytesProcessed int64
	TotalSlotMs         int64
	CacheHit            bool
	StatementType       string
}

// IndexType represents BigQuery clustering/indexing types
type indexType string

const (
	IndexTypeClustering indexType = "CLUSTERING"
	IndexTypePartition  indexType = "PARTITION"
)

type bigqueryType string
