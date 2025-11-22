package memspace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// EmbeddingServerClient embedding service client
type EmbeddingServerClient struct {
	url    string
	client *http.Client
}

// NewEmbeddingServerClient create new client
func NewEmbeddingServerClient(url string) *EmbeddingServerClient {
	if url == "" {
		url = "http://localhost:5000" // Default address
	}

	return &EmbeddingServerClient{
		url: url,
		client: &http.Client{
			Timeout: 30 * time.Second, // 30 second timeout
		},
	}
}

// EmbedRequest embedding request structure
type EmbedRequest struct {
	Texts      []string `json:"texts"`
	Dimensions int      `json:"dimensions,omitempty"`
}

// EmbedResponse embedding response structure
type EmbedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	Dimension  int         `json:"dimension"`
	TextCount  int         `json:"text_count"`
	Model      string      `json:"model"`
	Timestamp  float64     `json:"timestamp"`
	Error      string      `json:"error,omitempty"`
}

// HealthResponse health check response
type HealthResponse struct {
	Status            string  `json:"status"`
	Model             string  `json:"model"`
	Timestamp         float64 `json:"timestamp"`
	ClientInitialized bool    `json:"client_initialized"`
}

// ModelsResponse model information response
type ModelsResponse struct {
	CurrentModel        string   `json:"current_model"`
	SupportedModels     []string `json:"supported_models"`
	SupportedDimensions []int    `json:"supported_dimensions"`
	DefaultDimensions   int      `json:"default_dimensions"`
	BatchSizeLimit      int      `json:"batch_size_limit"`
	TokenLimit          int      `json:"token_limit"`
	Languages           []string `json:"languages"`
}

// SimilarityRequest similarity calculation request
type SimilarityRequest struct {
	Text1      string `json:"text1"`
	Text2      string `json:"text2"`
	Dimensions int    `json:"dimensions,omitempty"`
}

// SimilarityResponse similarity calculation response
type SimilarityResponse struct {
	Text1      string  `json:"text1"`
	Text2      string  `json:"text2"`
	Similarity float64 `json:"similarity"`
	Dimension  int     `json:"dimension"`
	Timestamp  float64 `json:"timestamp"`
	Error      string  `json:"error,omitempty"`
}

// Embed generate text embedding vectors
func (c *EmbeddingServerClient) Embed(texts []string, dimensions int) ([][]float32, error) {
	request := EmbedRequest{
		Texts:      texts,
		Dimensions: dimensions,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize request: %v", err)
	}

	resp, err := c.client.Post(c.url+"/embed", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errorResp struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &errorResp); err != nil {
			return nil, fmt.Errorf("API returned error: %s, status code: %d", string(body), resp.StatusCode)
		}
		return nil, fmt.Errorf("API error: %s", errorResp.Error)
	}

	var embedResp EmbedResponse
	if err := json.Unmarshal(body, &embedResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return embedResp.Embeddings, nil
}

// EmbedSingle generate embedding vector for single text
func (c *EmbeddingServerClient) EmbedSingle(text string, dimensions int) ([]float32, error) {
	embeddings, err := c.Embed([]string{text}, dimensions)
	if err != nil {
		return nil, err
	}

	if len(embeddings) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return embeddings[0], nil
}

// HealthCheck health check
func (c *EmbeddingServerClient) HealthCheck() (*HealthResponse, error) {
	resp, err := c.client.Get(c.url + "/health")
	if err != nil {
		return nil, fmt.Errorf("health check request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read health check response: %v", err)
	}

	var healthResp HealthResponse
	if err := json.Unmarshal(body, &healthResp); err != nil {
		return nil, fmt.Errorf("failed to parse health check response: %v", err)
	}

	return &healthResp, nil
}

// GetModels get model information
func (c *EmbeddingServerClient) GetModels() (*ModelsResponse, error) {
	resp, err := c.client.Get(c.url + "/models")
	if err != nil {
		return nil, fmt.Errorf("get models request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read models response: %v", err)
	}

	var modelsResp ModelsResponse
	if err := json.Unmarshal(body, &modelsResp); err != nil {
		return nil, fmt.Errorf("failed to parse models response: %v", err)
	}

	return &modelsResp, nil
}

// CalculateSimilarity calculate text similarity
func (c *EmbeddingServerClient) CalculateSimilarity(text1, text2 string, dimensions int) (float64, error) {
	request := SimilarityRequest{
		Text1:      text1,
		Text2:      text2,
		Dimensions: dimensions,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize similarity request: %v", err)
	}

	resp, err := c.client.Post(c.url+"/similarity", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("similarity calculation request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read similarity response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errorResp struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &errorResp); err != nil {
			return 0, fmt.Errorf("similarity API returned error: %s, status code: %d", string(body), resp.StatusCode)
		}
		return 0, fmt.Errorf("similarity API error: %s", errorResp.Error)
	}

	var similarityResp SimilarityResponse
	if err := json.Unmarshal(body, &similarityResp); err != nil {
		return 0, fmt.Errorf("failed to parse similarity response: %v", err)
	}

	return similarityResp.Similarity, nil
}

// BatchTest batch testing
func (c *EmbeddingServerClient) BatchTest(texts []string) (map[string]interface{}, error) {
	request := map[string]interface{}{
		"texts": texts,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize batch test request: %v", err)
	}

	resp, err := c.client.Post(c.url+"/batch-test", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("batch test request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read batch test response: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse batch test response: %v", err)
	}

	return result, nil
}
