package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"nofx/config"
	"nofx/crypto"
	"nofx/logger"
	"nofx/store"

	"github.com/gin-gonic/gin"
)

type TelegramConfigRequest struct {
	Enabled         bool   `json:"enabled"`
	BotToken        string `json:"bot_token"`
	ChatID          string `json:"chat_id"`
	DefaultTraderID string `json:"default_trader_id"`
}

// handleGetTelegramConfig returns current telegram config
func (s *Server) handleGetTelegramConfig(c *gin.Context) {
	userID := c.GetString("user_id")
	setting, err := s.store.Telegram().GetSetting(userID)
	if err != nil {
		SafeInternalError(c, "Get telegram config", err)
		return
	}

	resp := gin.H{
		"enabled":           false,
		"chat_id":           "",
		"default_trader_id": "",
		"bot_token_set":     false,
	}
	if setting != nil {
		resp["enabled"] = setting.Enabled
		resp["chat_id"] = setting.ChatID
		resp["default_trader_id"] = setting.DefaultTraderID
		resp["bot_token_set"] = strings.TrimSpace(string(setting.BotToken)) != ""
	}

	c.JSON(http.StatusOK, resp)
}

// handleUpdateTelegramConfig updates telegram config (supports encrypted payload)
func (s *Server) handleUpdateTelegramConfig(c *gin.Context) {
	userID := c.GetString("user_id")
	cfg := config.Get()

	bodyBytes, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	var req TelegramConfigRequest
	if !cfg.TransportEncryption {
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			logger.Infof("❌ Failed to parse plain JSON telegram config: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
			return
		}
	} else {
		var encryptedPayload crypto.EncryptedPayload
		if err := json.Unmarshal(bodyBytes, &encryptedPayload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format, encrypted transmission required"})
			return
		}
		if encryptedPayload.WrappedKey == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "This endpoint only supports encrypted transmission, please use encrypted client",
				"code":    "ENCRYPTION_REQUIRED",
				"message": "Encrypted transmission is required for security reasons",
			})
			return
		}
		decrypted, err := s.cryptoHandler.cryptoService.DecryptSensitiveData(&encryptedPayload)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to decrypt data"})
			return
		}
		if err := json.Unmarshal([]byte(decrypted), &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse decrypted data"})
			return
		}
	}

	setting, err := s.store.Telegram().GetSetting(userID)
	if err != nil {
		SafeInternalError(c, "Get telegram config", err)
		return
	}
	if setting == nil {
		setting = &store.TelegramSetting{UserID: userID}
	}

	if strings.TrimSpace(req.BotToken) != "" {
		setting.BotToken = crypto.EncryptedString(strings.TrimSpace(req.BotToken))
	}
	setting.ChatID = strings.TrimSpace(req.ChatID)
	setting.Enabled = req.Enabled
	setting.DefaultTraderID = strings.TrimSpace(req.DefaultTraderID)

	if setting.Enabled && strings.TrimSpace(string(setting.BotToken)) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bot Token 不能为空"})
		return
	}

	if err := s.store.Telegram().UpsertSetting(setting); err != nil {
		SafeInternalError(c, "Update telegram config", err)
		return
	}

	if s.telegramManager != nil {
		if err := s.telegramManager.ReloadUser(userID); err != nil {
			logger.Infof("⚠️ Failed to reload telegram bot: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Telegram 配置已更新"})
}

// handleTestTelegram sends a test message
func (s *Server) handleTestTelegram(c *gin.Context) {
	userID := c.GetString("user_id")
	if s.telegramManager == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Telegram 服务未初始化"})
		return
	}
	if err := s.telegramManager.SendMessage(userID, "测试消息：Telegram 通知已配置"); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("发送失败: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "测试消息已发送"})
}
