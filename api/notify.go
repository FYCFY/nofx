package api

import (
	"net/http"
	"strings"

	"nofx/store"

	"github.com/gin-gonic/gin"
)

type TraderNotifyRuleRequest struct {
	TargetEquity float64 `json:"target_equity"`
	TriggerMode  string  `json:"trigger_mode"` // once | cross
}

// handleGetTraderNotifyRule returns notify rule for trader
func (s *Server) handleGetTraderNotifyRule(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	// Verify trader belongs to user
	if _, err := s.store.Trader().GetFullConfig(userID, traderID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader does not exist or no access permission"})
		return
	}

	rule, err := s.store.Notify().GetRule(traderID)
	if err != nil {
		SafeInternalError(c, "Get notify rule", err)
		return
	}
	if rule == nil {
		c.JSON(http.StatusOK, gin.H{
			"target_equity": 0,
			"trigger_mode":  "once",
			"triggered":     false,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"target_equity": rule.TargetEquity,
		"trigger_mode":  rule.TriggerMode,
		"triggered":     rule.Triggered,
	})
}

// handleUpdateTraderNotifyRule updates notify rule for trader
func (s *Server) handleUpdateTraderNotifyRule(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	// Verify trader belongs to user
	if _, err := s.store.Trader().GetFullConfig(userID, traderID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader does not exist or no access permission"})
		return
	}

	var req TraderNotifyRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		SafeBadRequest(c, "Invalid request parameters")
		return
	}

	mode := strings.ToLower(strings.TrimSpace(req.TriggerMode))
	if mode == "" {
		mode = "once"
	}
	if mode != "once" && mode != "cross" {
		SafeBadRequest(c, "Invalid trigger_mode")
		return
	}

	rule := &store.TraderNotifyRule{
		TraderID:     traderID,
		UserID:       userID,
		TargetEquity: req.TargetEquity,
		TriggerMode:  mode,
	}
	if err := s.store.Notify().UpsertRule(rule); err != nil {
		SafeInternalError(c, "Update notify rule", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "通知规则已更新"})
}
