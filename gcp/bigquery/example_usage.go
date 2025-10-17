package bigquery

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
)

// ExampleTransaction represents a transaction with JSON fields
type ExampleTransaction struct {
	ID       string                 `bigquery:"id" json:"id"`
	Amount   float64                `bigquery:"amount" json:"amount"`
	Disputes []*Dispute             `bigquery:"disputes" json:"disputes"`
	Metadata map[string]interface{} `bigquery:"metadata" json:"metadata"`
	Created  time.Time              `bigquery:"created" json:"created"`
}

// Dispute represents a dispute in the disputes JSON array
type Dispute struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	ID        string    `json:"id,omitempty"`
	Memo      string    `json:"memo,omitempty"`
	Type      string    `json:"type,omitempty"`
}

// ExampleUsage demonstrates how to use the custom JSON unmarshaling
func ExampleUsage() {
	// Example 1: Using QueryWithCustomUnmarshaling for single row
	query := "SELECT id, amount, disputes, metadata, created FROM transactions WHERE id = @transaction_id"

	// Define the JSON field types
	jsonFieldTypes := map[string]reflect.Type{
		"disputes": reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))),
		"metadata": reflect.TypeOf(map[string]interface{}{}),
	}

	// Create a transaction struct to hold the result
	var transaction ExampleTransaction

	// Note: In real usage, you would call this on an actual BigQuery client
	// err := client.QueryWithCustomUnmarshaling(query, &transaction, jsonFieldTypes)
	fmt.Printf("Example 1: Single row query with custom JSON unmarshaling\n")
	fmt.Printf("Query: %s\n", query)
	fmt.Printf("Target struct: %+v\n", transaction)
	fmt.Printf("JSON field types: %+v\n", jsonFieldTypes)

	// Example 2: Using QueryRowsWithCustomUnmarshaling for multiple rows
	query2 := "SELECT id, amount, disputes, metadata, created FROM transactions LIMIT 10"

	// Create a slice to hold multiple transactions
	var transactions []ExampleTransaction

	// Note: In real usage, you would call this on an actual BigQuery client
	// err := client.QueryRowsWithCustomUnmarshaling(query2, &transactions, jsonFieldTypes)
	fmt.Printf("\nExample 2: Multiple rows query with custom JSON unmarshaling\n")
	fmt.Printf("Query: %s\n", query2)
	fmt.Printf("Target slice: %+v\n", transactions)

	// Example 3: Using the CustomJSONUnmarshaler directly
	unmarshaler := NewCustomJSONUnmarshaler()

	// Register types for specific fields
	unmarshaler.RegisterType("disputes", reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))))
	unmarshaler.RegisterType("metadata", reflect.TypeOf(map[string]interface{}{}))

	// Example JSON data (as it would come from BigQuery)
	jsonData := `[
		{
			"created_at": "2023-01-01T00:00:00Z",
			"id": "dispute_1",
			"memo": "Payment dispute",
			"type": "chargeback"
		},
		{
			"created_at": "2023-01-02T00:00:00Z",
			"id": "dispute_2",
			"memo": "Refund request",
			"type": "refund"
		}
	]`

	// Unmarshal the disputes JSON field
	disputes, err := unmarshaler.UnmarshalJSONField("disputes", jsonData)
	if err != nil {
		fmt.Printf("Error unmarshaling disputes: %v\n", err)
	} else {
		fmt.Printf("\nExample 3: Direct JSON unmarshaling\n")
		fmt.Printf("Unmarshaled disputes: %+v\n", disputes)
	}

	// Example 4: Using convenience functions
	disputeType := reflect.TypeOf(Dispute{})
	disputesType := reflect.SliceOf(reflect.PointerTo(disputeType))

	// Create a pre-configured unmarshaler for disputes
	disputesUnmarshaler := CreateDisputesUnmarshaler(disputeType)

	// Unmarshal using the pre-configured unmarshaler
	disputes2, err := disputesUnmarshaler.UnmarshalJSONField("disputes", jsonData)
	if err != nil {
		fmt.Printf("Error unmarshaling disputes with convenience function: %v\n", err)
	} else {
		fmt.Printf("\nExample 4: Using convenience functions\n")
		fmt.Printf("Unmarshaled disputes: %+v\n", disputes2)
	}

	// Example 5: Using direct unmarshaling functions
	disputes3, err := UnmarshalJSONToSlice(jsonData, disputesType)
	if err != nil {
		fmt.Printf("Error with direct unmarshaling: %v\n", err)
	} else {
		fmt.Printf("\nExample 5: Direct unmarshaling functions\n")
		fmt.Printf("Unmarshaled disputes: %+v\n", disputes3)
	}
}

// ExampleWithRealBigQueryClient demonstrates how to use with a real BigQuery client
func ExampleWithRealBigQueryClient(client *Client) error {
	// Define your transaction struct
	type Transaction struct {
		ID       string     `bigquery:"id" json:"id"`
		Amount   float64    `bigquery:"amount" json:"amount"`
		Disputes []*Dispute `bigquery:"disputes" json:"disputes"`
		Created  time.Time  `bigquery:"created" json:"created"`
	}

	// Define JSON field types
	jsonFieldTypes := map[string]reflect.Type{
		"disputes": reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(Dispute{}))),
	}

	// Query for a single transaction
	query := "SELECT id, amount, disputes, created FROM transactions WHERE id = @transaction_id"
	params := map[string]interface{}{
		"transaction_id": "txn_123",
	}

	// Execute parameterized query
	it, err := client.QueryWithParams(query, params)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Process results with custom unmarshaling
	unmarshaler := NewCustomJSONUnmarshaler()
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
			return fmt.Errorf("failed to read row: %w", err)
		}

		var transaction Transaction
		err = unmarshaler.UnmarshalBigQueryRow(row, &transaction)
		if err != nil {
			return fmt.Errorf("failed to unmarshal row: %w", err)
		}

		// Process the transaction
		fmt.Printf("Transaction ID: %s, Amount: %.2f, Disputes: %d\n",
			transaction.ID, transaction.Amount, len(transaction.Disputes))
	}

	return nil
}
