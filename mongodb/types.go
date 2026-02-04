package mongodb

import (
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type SortDirection int
type EqualityOperator string
type EqualityFilter struct {
	FieldName string
	Value     float64
	Operator  EqualityOperator
}

const (
	SortDirectionAsc                   SortDirection    = 1
	SortDirectionDesc                  SortDirection    = -1
	EqualityOperatorEqual              EqualityOperator = "$eq"
	EqualityOperatorNotEqual           EqualityOperator = "$ne"
	EqualityOperatorGreaterThanOrEqual EqualityOperator = "$gte"
	EqualityOperatorLessThanOrEqual    EqualityOperator = "$lte"
)

// Document is an interface for all documents which can be written to a collection.
type Document interface {
	Collection() Collection

	CollectionName() string

	Database() Database

	DatabaseName() string

	TimestampField() string

	GetID() string

	SetMetadata()
}

type Collection string
type Database string

type SearchIndexType string

const (
	SearchIndexTypeVectorSearch SearchIndexType = "vectorSearch"
	SearchIndexTypeTextSearch   SearchIndexType = "textSearch"
)

type SearchIndexModel struct {
	SearchIndex mongo.SearchIndexModel
	Type        SearchIndexType
	Name        string
	Reindex     bool
}

// String returns the string representation of the collection type, i.e. the collection name.
func (c Collection) String() string {
	return string(c)
}

// String returns the string representation of the database type, i.e. the database name.
func (d Database) String() string {
	return string(d)
}

type Metadata struct {
	SchemaVersion  int       `bson:"schema_version" json:"schema_version"`
	InstanceID     string    `bson:"instance_id" json:"instance_id"`
	UpdatedAt      time.Time `bson:"updated_at" json:"updated_at"`
	AthenaLastSync time.Time `bson:"athena_last_sync" json:"athena_last_sync"`
}

func (r *Metadata) Set(ver int) {
	r.SchemaVersion = ver
	r.InstanceID = os.Getenv("HOSTNAME")
	r.UpdatedAt = time.Now()
}

type Embeddings struct {
	FullContextEmbedding []float32 `json:"full_context_embedding,omitempty" bson:"full_context_embedding,omitempty"`
	EmbeddingModifiedAt  time.Time `json:"embedding_modified_at,omitempty" bson:"embedding_modified_at,omitempty"`
	LastSearchScore      float64   `json:"last_search_score,omitempty" bson:"last_search_score,omitempty"`
	LastSearchString     string    `json:"last_search_string,omitempty" bson:"last_search_string,omitempty"`
	LastSearchVector     []float32 `json:"last_search_vector,omitempty" bson:"last_search_vector,omitempty"`
	LastSearchTime       time.Time `json:"last_search_time,omitempty" bson:"last_search_time,omitempty"`
}

type SearchOptions struct {
	EmbeddingPath string
	Limit         int
	NumCandidates int
	Exact         bool
	Sort          bson.D
	ScoreCutoff   *EqualityFilter
}

type SearchOption func(*SearchOptions)

func WithLimit(limit int) SearchOption {
	return func(opts *SearchOptions) {
		opts.Limit = limit
	}
}

func WithNumCandidates(numCandidates int) SearchOption {
	return func(opts *SearchOptions) {
		opts.NumCandidates = numCandidates
	}
}

func WithExact(exact bool) SearchOption {
	return func(opts *SearchOptions) {
		opts.Exact = exact
	}
}

func WithEmbeddingPath(embeddingPath string) SearchOption {
	return func(opts *SearchOptions) {
		opts.EmbeddingPath = embeddingPath
	}
}

func WithScoreCutoff(scoreField string, scoreCutoff float64, op EqualityOperator) SearchOption {
	return func(opts *SearchOptions) {
		opts.ScoreCutoff = &EqualityFilter{
			FieldName: scoreField,
			Value:     scoreCutoff,
			Operator:  op,
		}
	}
}

func WithSort(sortSpec ...bson.E) SearchOption {
	return func(opts *SearchOptions) {
		for _, spec := range sortSpec {
			opts.Sort = append(opts.Sort, spec)
		}
	}
}
