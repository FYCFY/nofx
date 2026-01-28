package ai

import (
	"fmt"
	"strings"
	"time"

	"nofx/mcp"
	"nofx/store"
)

type aiModelStore interface {
	Get(userID, modelID string) (*store.AIModel, error)
	UpdateOAuthTokens(userID, modelID, accessToken, refreshToken string, expiresAt time.Time) error
	UpdateOAuthAccountID(userID, modelID, accountID string) error
}

// OpenAICodexTokenProvider returns a token provider using the main store.
func OpenAICodexTokenProvider(st *store.Store, userID, modelID string) mcp.TokenProvider {
	if st == nil {
		return func() (string, string, error) {
			return "", "", fmt.Errorf("store unavailable for oauth refresh")
		}
	}
	return OpenAICodexTokenProviderWithStore(st.AIModel(), userID, modelID)
}

// OpenAICodexTokenProviderWithStore returns a token provider using an AIModelStore.
func OpenAICodexTokenProviderWithStore(modelStore aiModelStore, userID, modelID string) mcp.TokenProvider {
	return func() (string, string, error) {
		if modelStore == nil {
			return "", "", fmt.Errorf("model store unavailable for oauth refresh")
		}

		model, err := modelStore.Get(userID, modelID)
		if err != nil {
			return "", "", err
		}

		accessToken := strings.TrimSpace(string(model.OAuthAccessToken))
		if accessToken == "" {
			return "", "", fmt.Errorf("missing codex oauth access token")
		}

		accountID := strings.TrimSpace(model.OAuthAccountID)
		shouldRefresh := false
		if model.OAuthExpiresAt != nil {
			if time.Now().After(model.OAuthExpiresAt.Add(-2 * time.Minute)) {
				shouldRefresh = true
			}
		}

		if shouldRefresh {
			refreshToken := strings.TrimSpace(string(model.OAuthRefreshToken))
			if refreshToken == "" {
				return "", "", fmt.Errorf("missing codex oauth refresh token")
			}
			refreshed, err := mcp.RefreshOpenAICodexAccessToken(refreshToken)
			if err != nil {
				return "", "", err
			}

			if err := modelStore.UpdateOAuthTokens(userID, modelID, refreshed.AccessToken, refreshed.RefreshToken, refreshed.ExpiresAt); err != nil {
				return "", "", err
			}
			accessToken = refreshed.AccessToken
			if accountID == "" {
				accountID = mcp.DecodeOpenAICodexJWTAccountID(accessToken)
				_ = modelStore.UpdateOAuthAccountID(userID, modelID, accountID)
			}
		}

		if accountID == "" {
			accountID = mcp.DecodeOpenAICodexJWTAccountID(accessToken)
			_ = modelStore.UpdateOAuthAccountID(userID, modelID, accountID)
		}

		return accessToken, accountID, nil
	}
}
