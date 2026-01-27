package store

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// TraderNotifyRule defines per-trader notification rules
type TraderNotifyRule struct {
	TraderID     string    `gorm:"primaryKey" json:"trader_id"`
	UserID       string    `gorm:"column:user_id;index" json:"user_id"`
	TargetEquity float64   `gorm:"column:target_equity;default:0" json:"target_equity"`
	TriggerMode  string    `gorm:"column:trigger_mode;default:once" json:"trigger_mode"` // once | cross
	Triggered    bool      `gorm:"column:triggered;default:false" json:"triggered"`
	LastEquity   float64   `gorm:"column:last_equity;default:0" json:"last_equity"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (TraderNotifyRule) TableName() string { return "trader_notify_rules" }

// NotifyStore notification rule storage
type NotifyStore struct {
	db *gorm.DB
}

func NewNotifyStore(db *gorm.DB) *NotifyStore {
	return &NotifyStore{db: db}
}

func (s *NotifyStore) initTables() error {
	if err := s.db.AutoMigrate(&TraderNotifyRule{}); err != nil {
		return fmt.Errorf("failed to migrate notify tables: %w", err)
	}
	return nil
}

// GetRule returns rule for trader (nil if not found)
func (s *NotifyStore) GetRule(traderID string) (*TraderNotifyRule, error) {
	var rule TraderNotifyRule
	err := s.db.Where("trader_id = ?", traderID).First(&rule).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &rule, nil
}

// UpsertRule creates or updates rule, resets trigger state if target/mode changed
func (s *NotifyStore) UpsertRule(rule *TraderNotifyRule) error {
	existing, err := s.GetRule(rule.TraderID)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.TargetEquity != rule.TargetEquity || existing.TriggerMode != rule.TriggerMode {
			rule.Triggered = false
			rule.LastEquity = 0
		} else {
			rule.Triggered = existing.Triggered
			rule.LastEquity = existing.LastEquity
		}
	}
	rule.UpdatedAt = time.Now().UTC()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = time.Now().UTC()
	}
	return s.db.Save(rule).Error
}

// UpdateTriggerState updates trigger state and last equity
func (s *NotifyStore) UpdateTriggerState(traderID string, triggered bool, lastEquity float64) error {
	return s.db.Model(&TraderNotifyRule{}).
		Where("trader_id = ?", traderID).
		Updates(map[string]interface{}{
			"triggered":   triggered,
			"last_equity": lastEquity,
			"updated_at":  time.Now().UTC(),
		}).Error
}
