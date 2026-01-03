package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/slices"
)

type commandOpt func() (interface{}, string)

func WithCustomFilter(filter bson.D) commandOpt {
	return func() (interface{}, string) {
		return filter, "filter"
	}
}

func WithIgnoreFields(fields []string) commandOpt {
	return func() (interface{}, string) {
		if !slices.Contains(fields, "_id") {
			fields = append(fields, "_id")
		}
		return fields, "ignoreFields"
	}
}

func WithCustomOperator(customOperators bson.M) commandOpt {
	return func() (interface{}, string) {
		return customOperators, "customOperators"
	}
}
