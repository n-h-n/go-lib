package voyage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// =============================================================================
// Voyage AI Embeddings API
// =============================================================================
// API Reference: https://docs.voyageai.com/reference/embeddings-api
//
// Models:
// - voyage-4: 1024 dimensions, best overall retrieval quality
// - voyage-4-lite: 512 dimensions, faster, good for large-scale
// - voyage-4-large: 1536 dimensions, high quality for general use
// - voyage-3: 1024 dimensions, previous gen retrieval
// - voyage-finance-2: 1024 dimensions, optimized for finance domain
// - voyage-code-2: 1536 dimensions, optimized for code
//
// Input Types:
// - "document": Use when embedding documents for indexing/storage
// - "query": Use when embedding search queries
// =============================================================================

// InputType specifies how the text should be embedded
type InputType string

const (
	// InputTypeDocument is used when embedding documents for storage/indexing
	InputTypeDocument InputType = "document"

	// InputTypeQuery is used when embedding search queries
	InputTypeQuery InputType = "query"
)

// EmbeddingRequest is the request body for the Voyage embeddings API
type EmbeddingRequest struct {
	// Input is the text(s) to embed. Can be a single string or array of strings.
	Input []string `json:"input"`

	// Model is the embedding model to use (e.g., "voyage-4")
	Model string `json:"model"`

	// InputType specifies how to process the input: "query" or "document"
	// Using "query" vs "document" can improve retrieval quality
	InputType InputType `json:"input_type,omitempty"`

	// Truncation controls how to handle inputs exceeding max token length
	// If true, truncates; if false, returns error. Default: true
	Truncation *bool `json:"truncation,omitempty"`

	// EncodingFormat specifies the format of embeddings. Default: "float"
	// Options: "float", "base64"
	EncodingFormat string `json:"encoding_format,omitempty"`
}

// EmbeddingResponse is the response from the Voyage embeddings API
type EmbeddingResponse struct {
	// Object is always "list" for embeddings
	Object string `json:"object"`

	// Data contains the embedding results
	Data []EmbeddingData `json:"data"`

	// Model is the model used for embedding
	Model string `json:"model"`

	// Usage contains token usage information
	Usage EmbeddingUsage `json:"usage"`
}

// EmbeddingData contains a single embedding result
type EmbeddingData struct {
	// Object is always "embedding"
	Object string `json:"object"`

	// Embedding is the vector representation (1024 dims for voyage-4)
	Embedding []float32 `json:"embedding"`

	// Index is the position in the input array
	Index int `json:"index"`
}

// EmbeddingUsage contains token usage information
type EmbeddingUsage struct {
	// TotalTokens is the total tokens processed
	TotalTokens int `json:"total_tokens"`
}

// ErrorResponse represents an error from the Voyage API
type ErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code,omitempty"`
	} `json:"error"`
}

// GenerateEmbeddings creates embeddings for a batch of texts
//
// Parameters:
// - ctx: Context for the request
// - texts: Array of strings to embed (max 128 texts per request)
// - inputType: "document" for indexing, "query" for search queries
//
// Returns:
// - [][]float32: Embeddings in the same order as input texts
// - error: Any error that occurred
//
// Example:
//
//	embeddings, err := client.GenerateEmbeddings(ctx, []string{
//	    "What is machine learning?",
//	    "How does neural network work?",
//	}, voyage.InputTypeQuery)
func (c *Client) GenerateEmbeddings(ctx context.Context, texts []string, inputType InputType) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, fmt.Errorf("at least one text is required")
	}

	if len(texts) > 128 {
		return nil, fmt.Errorf("maximum 128 texts per request, got %d", len(texts))
	}

	req := EmbeddingRequest{
		Input:     texts,
		Model:     c.model,
		InputType: inputType,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.makeRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return nil, fmt.Errorf("embedding request failed with status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("embedding request failed: %s (type: %s)", errResp.Error.Message, errResp.Error.Type)
	}

	var embResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Extract embeddings in order
	embeddings := make([][]float32, len(texts))
	for _, d := range embResp.Data {
		if d.Index >= 0 && d.Index < len(embeddings) {
			embeddings[d.Index] = d.Embedding
		}
	}

	// Verify all embeddings were received
	for i, emb := range embeddings {
		if emb == nil {
			return nil, fmt.Errorf("missing embedding for index %d", i)
		}
	}

	return embeddings, nil
}

// GenerateEmbedding creates an embedding for a single text (convenience method)
//
// Parameters:
// - ctx: Context for the request
// - text: The text to embed
// - inputType: "document" for indexing, "query" for search queries
//
// Returns:
// - []float32: The embedding vector
// - error: Any error that occurred
func (c *Client) GenerateEmbedding(ctx context.Context, text string, inputType InputType) ([]float32, error) {
	embeddings, err := c.GenerateEmbeddings(ctx, []string{text}, inputType)
	if err != nil {
		return nil, err
	}
	if len(embeddings) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	return embeddings[0], nil
}

// GenerateDocumentEmbeddings is a convenience method for embedding documents
func (c *Client) GenerateDocumentEmbeddings(ctx context.Context, texts []string) ([][]float32, error) {
	return c.GenerateEmbeddings(ctx, texts, InputTypeDocument)
}

// GenerateDocumentEmbedding is a convenience method for embedding a single document
func (c *Client) GenerateDocumentEmbedding(ctx context.Context, text string) ([]float32, error) {
	return c.GenerateEmbedding(ctx, text, InputTypeDocument)
}

// GenerateQueryEmbedding is a convenience method for embedding a search query
func (c *Client) GenerateQueryEmbedding(ctx context.Context, query string) ([]float32, error) {
	return c.GenerateEmbedding(ctx, query, InputTypeQuery)
}

// BatchGenerateEmbeddings handles large batches by splitting into chunks of 128
// and processing them sequentially with rate limiting
func (c *Client) BatchGenerateEmbeddings(ctx context.Context, texts []string, inputType InputType) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, fmt.Errorf("at least one text is required")
	}

	const batchSize = 128
	allEmbeddings := make([][]float32, len(texts))

	for i := 0; i < len(texts); i += batchSize {
		end := i + batchSize
		if end > len(texts) {
			end = len(texts)
		}

		batch := texts[i:end]
		embeddings, err := c.GenerateEmbeddings(ctx, batch, inputType)
		if err != nil {
			return nil, fmt.Errorf("failed to embed batch starting at index %d: %w", i, err)
		}

		// Copy embeddings to correct positions
		for j, emb := range embeddings {
			allEmbeddings[i+j] = emb
		}
	}

	return allEmbeddings, nil
}
