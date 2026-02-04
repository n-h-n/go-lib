package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/exp/slices"
)

func hasSearchStage(pipeline []bson.D) bool {
	for _, stage := range pipeline {
		for _, elem := range stage {
			if slices.Contains([]string{"$search", "$vectorSearch"}, elem.Key) {
				return true
			}

			// Also check nested documents
			if subDoc, ok := elem.Value.(bson.D); ok {
				if hasSearchInDoc(subDoc) {
					return true
				}
			}
			// Check for arrays of documents
			if subArray, ok := elem.Value.(bson.A); ok {
				if hasSearchInArray(subArray) {
					return true
				}
			}
		}
	}
	return false
}

// Helper for nested documents
func hasSearchInDoc(doc bson.D) bool {
	for _, elem := range doc {
		if elem.Key == "$search" {
			return true
		}
		// Recursive check for nested documents
		if subDoc, ok := elem.Value.(bson.D); ok {
			if hasSearchInDoc(subDoc) {
				return true
			}
		}
		// Check arrays
		if subArray, ok := elem.Value.(bson.A); ok {
			if hasSearchInArray(subArray) {
				return true
			}
		}
	}
	return false
}

// Helper for arrays
func hasSearchInArray(arr bson.A) bool {
	for _, item := range arr {
		if doc, ok := item.(bson.D); ok {
			if hasSearchInDoc(doc) {
				return true
			}
		}
	}
	return false
}

// buildVectorSearchPipeline creates a MongoDB aggregation pipeline for vector search
func BuildVectorSearchPipeline(
	queryVector []float32,
	searchIndexModel *SearchIndexModel,
	opts ...SearchOption,
) []bson.D {
	// Apply options
	searchOpts := &SearchOptions{
		Exact:         false,
		Limit:         10,
		NumCandidates: 100,
		EmbeddingPath: "_embeddings.full_context_embedding",
		ScoreCutoff: &EqualityFilter{
			FieldName: "_embeddings.last_search_score",
			Value:     0.5,
			Operator:  EqualityOperatorGreaterThanOrEqual,
		},
	}
	for _, opt := range opts {
		opt(searchOpts)
	}

	// Build $vectorSearch stage
	// See: https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/
	vectorSearchStage := bson.D{
		{Key: "$vectorSearch", Value: bson.D{
			{Key: "exact", Value: searchOpts.Exact},
			{Key: "index", Value: searchIndexModel.Name},
			{Key: "path", Value: searchOpts.EmbeddingPath},
			{Key: "queryVector", Value: queryVector},
			{Key: "numCandidates", Value: searchOpts.NumCandidates},
			{Key: "limit", Value: searchOpts.Limit},
		}},
	}

	// Build pipeline with $addFields to include search score
	pipeline := mongo.Pipeline{
		vectorSearchStage,
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "_embeddings.last_search_score", Value: bson.D{
					{Key: "$meta", Value: "vectorSearchScore"},
				}},
			}},
		},
		bson.D{
			{Key: "$match", Value: bson.D{
				{Key: searchOpts.ScoreCutoff.FieldName, Value: bson.D{
					{Key: string(searchOpts.ScoreCutoff.Operator), Value: searchOpts.ScoreCutoff.Value},
				}},
			}},
		},
		bson.D{
			{Key: "$sort", Value: searchOpts.Sort},
		},
		bson.D{
			{Key: "$limit", Value: searchOpts.Limit},
		},
	}

	return pipeline
}
