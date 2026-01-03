package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
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
