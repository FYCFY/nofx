package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	ProviderOpenAICodex       = "openai_codex"
	DefaultOpenAICodexBaseURL = "https://chatgpt.com/backend-api"
	DefaultOpenAICodexModel   = "gpt-5.2"
)

type TokenProvider func() (accessToken string, accountID string, err error)

type OpenAICodexClient struct {
	*Client
	AccessToken   string
	RefreshToken  string
	ExpiresAt     time.Time
	AccountID     string
	tokenProvider TokenProvider
	onRefresh     func(tokens OAuthTokens) error
}

// NewOpenAICodexClient creates OpenAI Codex OAuth client.
func NewOpenAICodexClient() AIClient {
	return NewOpenAICodexClientWithOptions()
}

// NewOpenAICodexClientWithOptions creates OpenAI Codex OAuth client (supports options pattern).
func NewOpenAICodexClientWithOptions(opts ...ClientOption) AIClient {
	codexOpts := []ClientOption{
		WithProvider(ProviderOpenAICodex),
		WithModel(DefaultOpenAICodexModel),
		WithBaseURL(DefaultOpenAICodexBaseURL),
	}
	allOpts := append(codexOpts, opts...)
	baseClient := NewClient(allOpts...).(*Client)
	codexClient := &OpenAICodexClient{Client: baseClient}
	baseClient.hooks = codexClient
	return codexClient
}

// SetAPIKey sets access token for Codex OAuth (apiKey parameter is treated as access token).
func (c *OpenAICodexClient) SetAPIKey(apiKey string, customURL string, customModel string) {
	c.AccessToken = apiKey
	c.APIKey = apiKey
	if customURL != "" {
		c.BaseURL = customURL
	}
	if customModel != "" {
		c.Model = customModel
	}
}

// SetOAuthTokens sets OAuth tokens and optional refresh callback.
func (c *OpenAICodexClient) SetOAuthTokens(accessToken, refreshToken string, expiresAt time.Time, accountID string, onRefresh func(tokens OAuthTokens) error) {
	c.AccessToken = accessToken
	c.APIKey = accessToken
	c.RefreshToken = refreshToken
	c.ExpiresAt = expiresAt
	c.AccountID = accountID
	c.onRefresh = onRefresh
}

// SetTokenProvider sets a provider that supplies a valid access token and account ID.
func (c *OpenAICodexClient) SetTokenProvider(provider TokenProvider) {
	c.tokenProvider = provider
}

func (c *OpenAICodexClient) ensureToken() error {
	if c.tokenProvider != nil {
		access, account, err := c.tokenProvider()
		if err != nil {
			return err
		}
		c.AccessToken = access
		c.APIKey = access
		c.AccountID = account
		return nil
	}

	if c.AccessToken == "" {
		return fmt.Errorf("codex oauth access token missing")
	}
	c.APIKey = c.AccessToken

	if c.RefreshToken != "" && !c.ExpiresAt.IsZero() {
		if time.Now().After(c.ExpiresAt.Add(-2 * time.Minute)) {
			refreshed, err := RefreshOpenAICodexAccessToken(c.RefreshToken)
			if err != nil {
				return err
			}
			c.AccessToken = refreshed.AccessToken
			c.APIKey = refreshed.AccessToken
			c.RefreshToken = refreshed.RefreshToken
			c.ExpiresAt = refreshed.ExpiresAt
			if c.onRefresh != nil {
				if err := c.onRefresh(refreshed); err != nil {
					return err
				}
			}
		}
	}

	if c.AccountID == "" {
		c.AccountID = DecodeOpenAICodexJWTAccountID(c.AccessToken)
	}

	return nil
}

func (c *OpenAICodexClient) buildMCPRequestBody(systemPrompt, userPrompt string) map[string]any {
	input := []map[string]any{}

	if userPrompt != "" {
		input = append(input, map[string]any{
			"type": "message",
			"role": "user",
			"content": []map[string]any{{
				"type": "input_text",
				"text": userPrompt,
			}},
		})
	}

	requestBody := map[string]any{
		"model":   c.Model,
		"store":   false,
		"stream":  true,
		"input":   input,
		"include": []string{"reasoning.encrypted_content"},
	}

	if systemPrompt != "" {
		requestBody["instructions"] = systemPrompt
	}

	return requestBody
}

func (c *OpenAICodexClient) buildUrl() string {
	base := strings.TrimSuffix(c.BaseURL, "/")
	return fmt.Sprintf("%s/codex/responses", base)
}

func (c *OpenAICodexClient) buildRequest(url string, jsonData []byte) (*http.Request, error) {
	if err := c.ensureToken(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("fail to build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("accept", "text/event-stream")
	c.setAuthHeader(req.Header)
	return req, nil
}

func (c *OpenAICodexClient) setAuthHeader(reqHeaders http.Header) {
	reqHeaders.Set("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	reqHeaders.Set("OpenAI-Beta", "responses=experimental")
	reqHeaders.Set("originator", "codex_cli_rs")
	if c.AccountID != "" {
		reqHeaders.Set("chatgpt-account-id", c.AccountID)
	}
}

func (c *OpenAICodexClient) marshalRequestBody(requestBody map[string]any) ([]byte, error) {
	// If request already uses responses format, just marshal
	if _, ok := requestBody["input"]; ok {
		return json.Marshal(requestBody)
	}

	// Convert chat/completions-like body to responses format
	var systemPrompt, userPrompt string
	if messages, ok := requestBody["messages"].([]map[string]string); ok {
		for _, msg := range messages {
			role := msg["role"]
			content := msg["content"]
			switch role {
			case "system":
				systemPrompt = content
			case "user":
				userPrompt = content
			}
		}
	}

	converted := c.buildMCPRequestBody(systemPrompt, userPrompt)
	return json.Marshal(converted)
}

func (c *OpenAICodexClient) parseMCPResponse(body []byte) (string, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "", fmt.Errorf("empty response body")
	}

	if strings.HasPrefix(text, "data:") {
		return parseCodexSSE(text)
	}

	return parseCodexJSON(body)
}

func parseCodexSSE(sse string) (string, error) {
	lines := strings.Split(sse, "\n")
	var output strings.Builder
	var finalText string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" || data == "" {
			continue
		}
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}

		typeVal, _ := event["type"].(string)
		switch typeVal {
		case "response.output_text.delta":
			if delta, ok := event["delta"].(string); ok {
				output.WriteString(delta)
			}
		case "response.output_text.done":
			if textVal, ok := event["text"].(string); ok {
				finalText = textVal
			}
		case "response.completed", "response.done":
			if resp, ok := event["response"].(map[string]interface{}); ok {
				finalText = extractTextFromResponse(resp)
			}
		}
	}

	if finalText != "" {
		return finalText, nil
	}
	if output.Len() > 0 {
		return output.String(), nil
	}
	return "", fmt.Errorf("failed to parse codex response stream")
}

func parseCodexJSON(body []byte) (string, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("failed to parse codex json response: %w", err)
	}
	if response, ok := payload["response"].(map[string]interface{}); ok {
		text := extractTextFromResponse(response)
		if text != "" {
			return text, nil
		}
	}
	text := extractTextFromResponse(payload)
	if text != "" {
		return text, nil
	}
	return "", fmt.Errorf("codex response missing output text")
}

func extractTextFromResponse(response map[string]interface{}) string {
	output, ok := response["output"].([]interface{})
	if !ok {
		return ""
	}

	var builder strings.Builder
	for _, item := range output {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if entryType, _ := entry["type"].(string); entryType != "message" {
			continue
		}
		contents, ok := entry["content"].([]interface{})
		if !ok {
			continue
		}
		for _, contentItem := range contents {
			contentMap, ok := contentItem.(map[string]interface{})
			if !ok {
				continue
			}
			if contentType, _ := contentMap["type"].(string); contentType != "output_text" {
				continue
			}
			if textVal, ok := contentMap["text"].(string); ok {
				builder.WriteString(textVal)
			}
		}
	}

	return builder.String()
}
