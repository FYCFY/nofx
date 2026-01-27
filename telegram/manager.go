package telegram

import (
	"fmt"
	"sync"

	"nofx/manager"
	"nofx/store"
)

// Manager manages per-user Telegram bot services
type Manager struct {
	st            *store.Store
	traderManager *manager.TraderManager
	mu            sync.RWMutex
	services      map[string]*Service
}

func NewManager(st *store.Store, tm *manager.TraderManager) *Manager {
	return &Manager{
		st:            st,
		traderManager: tm,
		services:      make(map[string]*Service),
	}
}

// StartAllEnabled starts bots for all users with enabled settings
func (m *Manager) StartAllEnabled() {
	userIDs, err := m.st.User().GetAllIDs()
	if err != nil {
		return
	}
	for _, userID := range userIDs {
		_ = m.ReloadUser(userID)
	}
}

// ReloadUser stops existing service and starts if enabled
func (m *Manager) ReloadUser(userID string) error {
	m.StopUser(userID)
	setting, err := m.st.Telegram().GetSetting(userID)
	if err != nil {
		return err
	}
	if setting == nil || !setting.Enabled || setting.BotToken == "" {
		return nil
	}
	return m.startService(setting)
}

// StopUser stops and removes service
func (m *Manager) StopUser(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.services[userID]; ok {
		svc.Stop()
		delete(m.services, userID)
	}
}

// SendMessage sends a message via user's bot
func (m *Manager) SendMessage(userID, text string) error {
	m.mu.RLock()
	svc := m.services[userID]
	m.mu.RUnlock()
	if svc == nil {
		return fmt.Errorf("telegram not configured")
	}
	return svc.SendText(text)
}

func (m *Manager) startService(setting *store.TelegramSetting) error {
	svc, err := NewService(m.st, m.traderManager, setting)
	if err != nil {
		return err
	}
	if err := svc.Start(); err != nil {
		return err
	}
	m.mu.Lock()
	m.services[setting.UserID] = svc
	m.mu.Unlock()
	return nil
}
