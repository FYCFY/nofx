package mcp

import (
	"net/http"
)

const (
	ProviderMinmax       = "minmax"
	DefaultMinmaxBaseURL = "https://api.minimaxi.com/v1"
	DefaultMinmaxModel   = "MiniMax-M2.1"
)

type MinmaxClient struct {
	*Client
}

// NewMinmaxClient creates MinMax client (backward compatible)
func NewMinmaxClient() AIClient {
	return NewMinmaxClientWithOptions()
}

// NewMinmaxClientWithOptions creates MinMax client (supports options pattern)
func NewMinmaxClientWithOptions(opts ...ClientOption) AIClient {
	minmaxOpts := []ClientOption{
		WithProvider(ProviderMinmax),
		WithModel(DefaultMinmaxModel),
		WithBaseURL(DefaultMinmaxBaseURL),
	}

	allOpts := append(minmaxOpts, opts...)
	baseClient := NewClient(allOpts...).(*Client)

	mmClient := &MinmaxClient{Client: baseClient}
	baseClient.hooks = mmClient

	return mmClient
}

func (m *MinmaxClient) SetAPIKey(apiKey string, customURL string, customModel string) {
	m.APIKey = apiKey

	if len(apiKey) > 8 {
		m.logger.Infof("🔧 [MCP] MinMax API Key: %s...%s", apiKey[:4], apiKey[len(apiKey)-4:])
	}
	if customURL != "" {
		m.BaseURL = customURL
		m.logger.Infof("🔧 [MCP] MinMax using custom BaseURL: %s", customURL)
	} else {
		m.logger.Infof("🔧 [MCP] MinMax using default BaseURL: %s", m.BaseURL)
	}
	if customModel != "" {
		m.Model = customModel
		m.logger.Infof("🔧 [MCP] MinMax using custom Model: %s", customModel)
	} else {
		m.logger.Infof("🔧 [MCP] MinMax using default Model: %s", m.Model)
	}
}

func (m *MinmaxClient) setAuthHeader(reqHeaders http.Header) {
	m.Client.setAuthHeader(reqHeaders)
}
