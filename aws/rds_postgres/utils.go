package rds_postgres

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

func goTypeToPostgresType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "integer", nil
	case reflect.Int64:
		return "bigint", nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "bigint", nil
	case reflect.Float32:
		return "real", nil
	case reflect.Float64:
		return "double precision", nil
	case reflect.Interface, reflect.Map:
		return "jsonb", nil
	case reflect.String:
		return "text", nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return "integer[]", nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return "bigint[]", nil
		case reflect.Float32:
			return "real[]", nil
		case reflect.Float64:
			return "double precision[]", nil
		case reflect.String:
			return "text[]", nil
		case reflect.Bool:
			return "boolean[]", nil
		case reflect.Map, reflect.Interface:
			return "jsonb[]", nil
		case reflect.Slice:
			ptype, err := goTypeToPostgresType(t.Elem())
			if err != nil {
				return "", err
			}
			return ptype + "[]", nil
		case reflect.Struct:
			if t.Elem() == reflect.TypeOf(time.Time{}) {
				return "timestamp with time zone[]", nil
			}
		}
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "timestamp with time zone", nil
		}
	}

	return "", fmt.Errorf("unable to map Go type to PostgreSQL type: %s", t.String())
}

func postgresTypeToGoType(t string) (reflect.Type, error) {
	if strings.HasSuffix(t, "[]") {
		// This is an array type. Get the element type by removing the "[]".
		elementType := t[:len(t)-2]
		// Recursively call postgresTypeToGoType to get the Go type for the element type.
		elementGoType, err := postgresTypeToGoType(elementType)
		if err != nil {
			return nil, err
		}
		// Return a slice type with the element type.
		return reflect.SliceOf(elementGoType), nil
	}

	switch t {
	case "boolean":
		return reflect.TypeOf(false), nil
	case "integer":
		return reflect.TypeOf(int(0)), nil
	case "bigint":
		return reflect.TypeOf(int64(0)), nil
	case "real":
		return reflect.TypeOf(float32(0)), nil
	case "double precision":
		return reflect.TypeOf(float64(0)), nil
	case "jsonb":
		return reflect.TypeOf(map[string]interface{}{}), nil
	case "text":
		return reflect.TypeOf(""), nil
	case "timestamptz":
		return reflect.TypeOf(time.Time{}), nil
	case "timestamp with time zone":
		return reflect.TypeOf(time.Time{}), nil
	}

	return nil, fmt.Errorf("unable to map PostgreSQL type to Go type: %s", t)
}

func postgresDefaultValue(t reflect.Type) (interface{}, error) {
	switch t.Kind() {
	case reflect.Bool:
		return false, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int64(0), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(0), nil
	case reflect.Float32:
		return float32(0), nil
	case reflect.Float64:
		return float64(0), nil
	case reflect.Interface, reflect.Map:
		return map[string]interface{}{}, nil
	case reflect.String:
		return "", nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return []int64{}, nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return []int64{}, nil
		case reflect.Float32:
			return []float32{}, nil
		case reflect.Float64:
			return []float64{}, nil
		case reflect.String:
			return []string{}, nil
		case reflect.Bool:
			return []bool{}, nil
		case reflect.Map, reflect.Interface:
			return []map[string]interface{}{}, nil
		case reflect.Slice:
			ptype, err := postgresDefaultValue(t.Elem())
			if err != nil {
				return nil, err
			}
			return []interface{}{ptype}, nil
		case reflect.Struct:
			if t.Elem() == reflect.TypeOf(time.Time{}) {
				return []time.Time{}, nil
			}
		}
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return time.Time{}, nil
		}
	}

	return nil, fmt.Errorf("unable to map Go type to PostgreSQL default value: %s", t.String())
}

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
		c.Type = field.Type
		c.Nullable = true
		c.Value = v.Field(i).Interface()

		if len(tagParts) > 1 {
			for _, tagPart := range tagParts[1:] {
				if tagPart == "primarykey" {
					c.PrimaryKey = true
					c.Nullable = false
					pkey = c.Name
				}
				if tagPart == "nonnullable" {
					c.Nullable = false
				}
				if tagPart == "nullable" {
					c.Nullable = true
				}
			}
		}

		columns[tagParts[0]] = c
	}

	err := ValidateColumns(&columns)
	if err != nil {
		panic(err)
	}

	return &columns, pkey
}

func ValidateColumns(columns *map[string]Column) error {
	pkey := ""
	for _, c := range *columns {
		if c.PrimaryKey {
			if pkey != "" {
				return fmt.Errorf("multiple primary keys defined on both %s and %s", pkey, c.Name)
			}
			pkey = c.Name

			if c.Nullable {
				return fmt.Errorf("primary key column %s cannot be nullable", c.Name)
			}
		}
	}

	return nil
}

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
	indexName := fmt.Sprintf("%s_%s_idx", table.Name, hash)

	if len(indexName) > 63 {
		indexName = indexName[:63]
	}
	return indexName, nil
}
