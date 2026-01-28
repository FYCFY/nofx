package mcp

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	OpenAICodexClientID     = "app_EMoamEEZ73f0CkXaXp7hrann"
	OpenAICodexAuthorizeURL = "https://auth.openai.com/oauth/authorize"
	OpenAICodexTokenURL     = "https://auth.openai.com/oauth/token"
	OpenAICodexRedirectURI  = "http://localhost:1455/auth/callback"
	OpenAICodexScope        = "openid profile email offline_access"
)

type OAuthTokens struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

type ParsedAuthInput struct {
	Code  string
	State string
}

func CreateOpenAICodexState() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func CreateOpenAICodexPKCE() (verifier string, challenge string, err error) {
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return "", "", err
	}
	verifier = base64.RawURLEncoding.EncodeToString(bytes)
	sum := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(sum[:])
	return verifier, challenge, nil
}

func BuildOpenAICodexAuthorizeURL(challenge, state string) string {
	u, _ := url.Parse(OpenAICodexAuthorizeURL)
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", OpenAICodexClientID)
	q.Set("redirect_uri", OpenAICodexRedirectURI)
	q.Set("scope", OpenAICodexScope)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", state)
	q.Set("id_token_add_organizations", "true")
	q.Set("codex_cli_simplified_flow", "true")
	q.Set("originator", "codex_cli_rs")
	u.RawQuery = q.Encode()
	return u.String()
}

func ParseOpenAICodexAuthorizationInput(input string) ParsedAuthInput {
	value := strings.TrimSpace(input)
	if value == "" {
		return ParsedAuthInput{}
	}

	if strings.Contains(value, "http") {
		if parsed, err := url.Parse(value); err == nil {
			return ParsedAuthInput{
				Code:  parsed.Query().Get("code"),
				State: parsed.Query().Get("state"),
			}
		}
	}

	if strings.Contains(value, "#") {
		parts := strings.SplitN(value, "#", 2)
		return ParsedAuthInput{Code: parts[0], State: parts[1]}
	}

	if strings.Contains(value, "code=") {
		params, err := url.ParseQuery(value)
		if err == nil {
			return ParsedAuthInput{
				Code:  params.Get("code"),
				State: params.Get("state"),
			}
		}
	}

	return ParsedAuthInput{Code: value}
}

func ExchangeOpenAICodexAuthorizationCode(code, verifier string) (OAuthTokens, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", OpenAICodexClientID)
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	form.Set("redirect_uri", OpenAICodexRedirectURI)

	req, err := http.NewRequest("POST", OpenAICodexTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return OAuthTokens{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return OAuthTokens{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&payload); err != nil {
		return OAuthTokens{}, fmt.Errorf("failed to parse oauth token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return OAuthTokens{}, fmt.Errorf("oauth token exchange failed: status %d", resp.StatusCode)
	}
	if payload.AccessToken == "" || payload.RefreshToken == "" || payload.ExpiresIn == 0 {
		return OAuthTokens{}, fmt.Errorf("oauth token response missing fields")
	}

	return OAuthTokens{
		AccessToken:  payload.AccessToken,
		RefreshToken: payload.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(payload.ExpiresIn) * time.Second),
	}, nil
}

func RefreshOpenAICodexAccessToken(refreshToken string) (OAuthTokens, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	form.Set("client_id", OpenAICodexClientID)

	req, err := http.NewRequest("POST", OpenAICodexTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return OAuthTokens{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return OAuthTokens{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&payload); err != nil {
		return OAuthTokens{}, fmt.Errorf("failed to parse oauth refresh response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return OAuthTokens{}, fmt.Errorf("oauth refresh failed: status %d", resp.StatusCode)
	}
	if payload.AccessToken == "" || payload.RefreshToken == "" || payload.ExpiresIn == 0 {
		return OAuthTokens{}, fmt.Errorf("oauth refresh response missing fields")
	}

	return OAuthTokens{
		AccessToken:  payload.AccessToken,
		RefreshToken: payload.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(payload.ExpiresIn) * time.Second),
	}, nil
}

func DecodeOpenAICodexJWTAccountID(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload := parts[1]
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return ""
		}
	}
	var data map[string]interface{}
	if err := json.Unmarshal(decoded, &data); err != nil {
		return ""
	}
	claim, ok := data["https://api.openai.com/auth"].(map[string]interface{})
	if !ok {
		return ""
	}
	accountID, _ := claim["chatgpt_account_id"].(string)
	return accountID
}
