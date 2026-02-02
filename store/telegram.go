package store

import (
	"fmt"
	"time"

	"nofx/crypto"

	"gorm.io/gorm"
)

// TelegramSetting per-user Telegram bot configuration
type TelegramSetting struct {
	UserID          string                 `gorm:"primaryKey" json:"user_id"`
	BotToken        crypto.EncryptedString `gorm:"column:bot_token;default:''" json:"-"`
	ChatID          string                 `gorm:"column:chat_id;default:''" json:"chat_id"`
	Enabled         bool                   `gorm:"column:enabled;default:false" json:"enabled"`
	DefaultTraderID string                 `gorm:"column:default_trader_id;default:''" json:"default_trader_id"`
	EnabledTraderIDs string                `gorm:"column:enabled_trader_ids;default:''" json:"enabled_trader_ids"`
	NotifyTypes     string                 `gorm:"column:notify_types;default:''" json:"notify_types"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

func (TelegramSetting) TableName() string { return "telegram_settings" }

// TelegramChatState tracks current selected trader per chat
type TelegramChatState struct {
	ChatID          string    `gorm:"primaryKey" json:"chat_id"`
	UserID          string    `gorm:"column:user_id;index" json:"user_id"`
	CurrentTraderID string    `gorm:"column:current_trader_id;default:''" json:"current_trader_id"`
	UpdatedAt       time.Time `json:"updated_at" gorm:"autoUpdateTime"`
}

func (TelegramChatState) TableName() string { return "telegram_chat_states" }

// TelegramStore telegram configuration storage
type TelegramStore struct {
	db *gorm.DB
}

func NewTelegramStore(db *gorm.DB) *TelegramStore {
	return &TelegramStore{db: db}
}

func (s *TelegramStore) initTables() error {
	if err := s.db.AutoMigrate(&TelegramSetting{}, &TelegramChatState{}); err != nil {
		return fmt.Errorf("failed to migrate telegram tables: %w", err)
	}
	return nil
}

// GetSetting returns telegram setting for user (nil if not found)
func (s *TelegramStore) GetSetting(userID string) (*TelegramSetting, error) {
	var setting TelegramSetting
	err := s.db.Where("user_id = ?", userID).First(&setting).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &setting, nil
}

// UpsertSetting creates or updates telegram setting
func (s *TelegramStore) UpsertSetting(setting *TelegramSetting) error {
	setting.UpdatedAt = time.Now().UTC()
	if setting.CreatedAt.IsZero() {
		setting.CreatedAt = time.Now().UTC()
	}
	return s.db.Save(setting).Error
}

// UpdateChatState sets current trader for chat
func (s *TelegramStore) UpdateChatState(userID, chatID, traderID string) error {
	state := &TelegramChatState{
		ChatID:          chatID,
		UserID:          userID,
		CurrentTraderID: traderID,
		UpdatedAt:       time.Now().UTC(),
	}
	return s.db.Save(state).Error
}

// GetChatState returns chat state (nil if not found)
func (s *TelegramStore) GetChatState(chatID string) (*TelegramChatState, error) {
	var state TelegramChatState
	err := s.db.Where("chat_id = ?", chatID).First(&state).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &state, nil
}
