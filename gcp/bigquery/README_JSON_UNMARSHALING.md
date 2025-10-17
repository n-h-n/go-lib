# BigQuery Custom JSON Unmarshaling

This document explains how to use the custom JSON unmarshaling functionality for BigQuery JSON fields, specifically to handle the error where BigQuery JSON fields cannot be directly assigned to Go struct fields like `[]*struct`.

## Problem

When running SELECT queries on BigQuery tables with JSON fields, you may encounter errors like:

```
bigquery: schema field disputes of type JSON is not assignable to struct field Disputes of type []*struct { CreatedAt time.Time "json:\"created_at,omitempty\""; ID string "json:\"id,omitempty\""; Memo string "json:\"memo,omitempty\""; Type string "json:\"type,omitempty\"" }
```

This happens because BigQuery stores JSON data as strings, but Go's BigQuery client cannot automatically unmarshal JSON strings into complex Go types like slices of structs.

## Solution

The custom JSON unmarshaling functionality provides several ways to handle JSON fields:

1. **CustomJSONUnmarshaler** - A flexible unmarshaler that can handle various JSON field types
2. **Enhanced Query Methods** - New query methods that use custom unmarshaling
3. **Convenience Functions** - Helper functions for common patterns

## Usage Examples

### 1. Basic Usage with CustomJSONUnmarshaler

```go
package main

import (
    "fmt"
    "reflect"
    "time"
    "github.com/n-h-n/go-lib/gcp/bigquery"
)

// Define your struct
type Transaction struct {
    ID       string     `bigquery:"id" json:"id"`
    Amount   float64    `bigquery:"amount" json:"amount"`
    Disputes []*Dispute `bigquery:"disputes" json:"disputes"`
    Created  time.Time  `bigquery:"created" json:"created"`
}

type Dispute struct {
    CreatedAt time.Time `json:"created_at,omitempty"`
    ID        string    `json:"id,omitempty"`
    Memo      string    `json:"memo,omitempty"`
    Type      string    `json:"type,omitempty"`
}

func main() {
    // Create BigQuery client
    client, err := bigquery.NewClient(ctx, projectID, datasetID)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Define JSON field types
    jsonFieldTypes := map[string]reflect.Type{
        "disputes": reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))),
    }

    // Query with custom unmarshaling
    query := "SELECT id, amount, disputes, created FROM transactions WHERE id = @transaction_id"
    params := map[string]interface{}{
        "transaction_id": "txn_123",
    }

    it, err := client.QueryWithParams(query, params)
    if err != nil {
        log.Fatal(err)
    }

    // Process results
    unmarshaler := bigquery.NewCustomJSONUnmarshaler()
    for fieldName, fieldType := range jsonFieldTypes {
        unmarshaler.RegisterType(fieldName, fieldType)
    }

    for {
        var row map[string]bigquery.Value
        err := it.Next(&row)
        if err != nil {
            if err.Error() == "iterator done" {
                break
            }
            log.Fatal(err)
        }

        var transaction Transaction
        err = unmarshaler.UnmarshalBigQueryRow(row, &transaction)
        if err != nil {
            log.Fatal(err)
        }

        fmt.Printf("Transaction: %+v\n", transaction)
    }
}
```

### 2. Using Enhanced Query Methods

```go
// Query single row with custom unmarshaling
var transaction Transaction
jsonFieldTypes := map[string]reflect.Type{
    "disputes": reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))),
}

err := client.QueryWithCustomUnmarshaling(query, &transaction, jsonFieldTypes)
if err != nil {
    log.Fatal(err)
}

// Query multiple rows with custom unmarshaling
var transactions []Transaction
err := client.QueryRowsWithCustomUnmarshaling(query, &transactions, jsonFieldTypes)
if err != nil {
    log.Fatal(err)
}
```

### 3. Using Convenience Functions

```go
// Create a pre-configured unmarshaler for disputes
disputeType := reflect.TypeOf(Dispute{})
unmarshaler := bigquery.CreateDisputesUnmarshaler(disputeType)

// Unmarshal JSON data directly
jsonData := `[{"id": "dispute_1", "memo": "Payment dispute", "type": "chargeback"}]`
disputes, err := unmarshaler.UnmarshalJSONField("disputes", jsonData)
if err != nil {
    log.Fatal(err)
}

// Or use direct unmarshaling functions
disputesType := reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{})))
disputes, err := bigquery.UnmarshalJSONToSlice(jsonData, disputesType)
if err != nil {
    log.Fatal(err)
}
```

## Supported Types

The custom unmarshaling supports:

- **Slices**: `[]*struct`, `[]struct`, `[]string`, `[]int`, etc.
- **Pointers**: `*struct`, `*string`, `*int`, etc.
- **Structs**: Any struct with JSON tags
- **Maps**: `map[string]interface{}`, `map[string]string`, etc.
- **Basic Types**: `string`, `int`, `float64`, `bool`, etc.

## API Reference

### CustomJSONUnmarshaler

```go
type CustomJSONUnmarshaler struct {
    TypeRegistry map[string]reflect.Type
}

// Create a new unmarshaler
func NewCustomJSONUnmarshaler() *CustomJSONUnmarshaler

// Register a field type
func (c *CustomJSONUnmarshaler) RegisterType(fieldName string, goType reflect.Type)

// Unmarshal a JSON field
func (c *CustomJSONUnmarshaler) UnmarshalJSONField(fieldName string, data interface{}) (interface{}, error)

// Unmarshal an entire BigQuery row
func (c *CustomJSONUnmarshaler) UnmarshalBigQueryRow(row map[string]bigquery.Value, targetStruct interface{}) error
```

### Enhanced Query Methods

```go
// Query with custom unmarshaling (single row)
func (c *Client) QueryWithCustomUnmarshaling(query string, targetStruct interface{}, jsonFieldTypes map[string]reflect.Type) error

// Query with custom unmarshaling (multiple rows)
func (c *Client) QueryRowsWithCustomUnmarshaling(query string, targetSlice interface{}, jsonFieldTypes map[string]reflect.Type) error
```

### Convenience Functions

```go
// Register disputes field type
func (c *CustomJSONUnmarshaler) RegisterDisputesType(disputeStructType reflect.Type)

// Direct unmarshaling functions
func UnmarshalJSONToSlice(data interface{}, sliceType reflect.Type) (interface{}, error)
func UnmarshalJSONToStruct(data interface{}, structType reflect.Type) (interface{}, error)
func UnmarshalJSONToPointer(data interface{}, pointerType reflect.Type) (interface{}, error)

// Pre-configured unmarshalers
func CreateDisputesUnmarshaler(disputeStructType reflect.Type) *CustomJSONUnmarshaler
```

## Best Practices

1. **Register Types Early**: Register all JSON field types before processing rows
2. **Use Struct Tags**: Use `bigquery` and `json` tags consistently
3. **Handle Errors**: Always check for errors when unmarshaling
4. **Type Safety**: Use strongly-typed structs instead of `map[string]interface{}`
5. **Performance**: Reuse unmarshaler instances when processing multiple rows

## Migration Guide

If you're currently experiencing the JSON unmarshaling error:

1. **Identify JSON Fields**: Find all fields in your structs that are JSON type in BigQuery
2. **Define Field Types**: Create a map of field names to their Go types
3. **Update Query Code**: Replace direct struct scanning with custom unmarshaling
4. **Test Thoroughly**: Verify that all JSON fields are correctly unmarshaled

## Example Migration

**Before (causing error):**
```go
var transaction Transaction
err := it.Next(&transaction) // This will fail for JSON fields
```

**After (with custom unmarshaling):**
```go
var row map[string]bigquery.Value
err := it.Next(&row)
if err != nil {
    // handle error
}

var transaction Transaction
unmarshaler := bigquery.NewCustomJSONUnmarshaler()
unmarshaler.RegisterType("disputes", reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))))
err = unmarshaler.UnmarshalBigQueryRow(row, &transaction)
```

This custom JSON unmarshaling functionality provides a robust solution for handling complex JSON fields in BigQuery, allowing you to work with strongly-typed Go structs while maintaining compatibility with BigQuery's JSON data type.
