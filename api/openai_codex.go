package api

import (
	"net/http"
	"strings"
	"time"

	"nofx/logger"
	"nofx/mcp"
	"nofx/store"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type openAICodexAuthorizeRequest struct {
	ModelID string `json:"model_id"`
}

type openAICodexAuthorizeResponse struct {
	AuthorizeURL string `json:"authorize_url"`
	State        string `json:"state"`
}

type openAICodexCallbackRequest struct {
	ModelID     string `json:"model_id"`
	CallbackURL string `json:"callback_url"`
	Code        string `json:"code"`
	State       string `json:"state"`
}

type openAICodexRefreshRequest struct {
	ModelID string `json:"model_id"`
}

func (s *Server) handleOpenAICodexAuthorize(c *gin.Context) {
	userID := c.GetString("user_id")
	var req openAICodexAuthorizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		SafeBadRequest(c, "Invalid request parameters")
		return
	}
	modelID := strings.TrimSpace(req.ModelID)
	if modelID == "" {
		modelID = "openai"
	}

	model, err := s.resolveOpenAIModel(userID, modelID)
	if err != nil {
		if err := s.store.AIModel().Update(userID, modelID, false, "", "", "", "codex_oauth"); err != nil {
			SafeInternalError(c, "Failed to init model", err)
			return
		}
		model, err = s.resolveOpenAIModel(userID, modelID)
		if err != nil {
			SafeInternalError(c, "Failed to load model", err)
			return
		}
	}

	verifier, challenge, err := mcp.CreateOpenAICodexPKCE()
	if err != nil {
		SafeInternalError(c, "Failed to create PKCE", err)
		return
	}

	state, err := mcp.CreateOpenAICodexState()
	if err != nil {
		SafeInternalError(c, "Failed to create OAuth state", err)
		return
	}

	if err := s.store.AIModel().SaveOAuthPKCE(userID, model.ID, verifier, challenge, state); err != nil {
		SafeInternalError(c, "Failed to persist OAuth state", err)
		return
	}

	authorizeURL := mcp.BuildOpenAICodexAuthorizeURL(challenge, state)
	c.JSON(http.StatusOK, openAICodexAuthorizeResponse{AuthorizeURL: authorizeURL, State: state})
}

func (s *Server) handleOpenAICodexCallback(c *gin.Context) {
	userID := c.GetString("user_id")
	var req openAICodexCallbackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		SafeBadRequest(c, "Invalid request parameters")
		return
	}

	modelID := strings.TrimSpace(req.ModelID)
	if modelID == "" {
		modelID = "openai"
	}

	model, err := s.resolveOpenAIModel(userID, modelID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			SafeBadRequest(c, "AI model not found")
			return
		}
		SafeInternalError(c, "Failed to load model", err)
		return
	}

	input := strings.TrimSpace(req.CallbackURL)
	if input == "" {
		input = strings.TrimSpace(req.Code)
	}
	parsed := mcp.ParseOpenAICodexAuthorizationInput(input)
	if parsed.Code == "" {
		SafeBadRequest(c, "Missing authorization code")
		return
	}
	state := parsed.State
	if req.State != "" {
		state = strings.TrimSpace(req.State)
	}
	if strings.TrimSpace(model.OAuthState) != "" && state != model.OAuthState {
		SafeBadRequest(c, "Invalid OAuth state")
		return
	}

	verifier := strings.TrimSpace(string(model.OAuthPKCEVerifier))
	if verifier == "" {
		SafeBadRequest(c, "Missing PKCE verifier")
		return
	}

	tokens, err := mcp.ExchangeOpenAICodexAuthorizationCode(parsed.Code, verifier)
	if err != nil {
		SafeInternalError(c, "Failed to exchange authorization code", err)
		return
	}

	accountID := mcp.DecodeOpenAICodexJWTAccountID(tokens.AccessToken)
	if accountID == "" {
		logger.Warnf("Failed to decode chatgpt account id for user %s", userID)
	}

	if err := s.store.AIModel().SaveOAuthTokens(userID, model.ID, tokens.AccessToken, tokens.RefreshToken, accountID, tokens.ExpiresAt); err != nil {
		SafeInternalError(c, "Failed to save OAuth tokens", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "ok",
		"expires_at": tokens.ExpiresAt.Format(time.RFC3339),
	})
}

func (s *Server) handleOpenAICodexRefresh(c *gin.Context) {
	userID := c.GetString("user_id")
	var req openAICodexRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		SafeBadRequest(c, "Invalid request parameters")
		return
	}

	modelID := strings.TrimSpace(req.ModelID)
	if modelID == "" {
		modelID = "openai"
	}

	model, err := s.resolveOpenAIModel(userID, modelID)
	if err != nil {
		SafeInternalError(c, "Failed to load model", err)
		return
	}

	refreshToken := strings.TrimSpace(string(model.OAuthRefreshToken))
	if refreshToken == "" {
		SafeBadRequest(c, "Missing refresh token")
		return
	}

	refreshed, err := mcp.RefreshOpenAICodexAccessToken(refreshToken)
	if err != nil {
		SafeInternalError(c, "Failed to refresh token", err)
		return
	}

	if err := s.store.AIModel().UpdateOAuthTokens(userID, model.ID, refreshed.AccessToken, refreshed.RefreshToken, refreshed.ExpiresAt); err != nil {
		SafeInternalError(c, "Failed to persist refreshed token", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "ok",
		"expires_at": refreshed.ExpiresAt.Format(time.RFC3339),
	})
}

func (s *Server) resolveOpenAIModel(userID, modelID string) (*store.AIModel, error) {
	modelID = strings.TrimSpace(modelID)
	if modelID == "" {
		modelID = "openai"
	}
	model, err := s.store.AIModel().Get(userID, modelID)
	if err == nil {
		return model, nil
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return s.store.AIModel().GetByProvider(userID, "openai")
}
