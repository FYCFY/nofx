package telegram

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"nofx/logger"
	"nofx/manager"
	"nofx/store"
	"nofx/trader"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// Service handles a single user's Telegram bot
type Service struct {
	st            *store.Store
	traderManager *manager.TraderManager
	settings      *store.TelegramSetting
	bot           *tgbotapi.BotAPI
	stopCh        chan struct{}
	stopOnce      sync.Once
}

func NewService(st *store.Store, tm *manager.TraderManager, setting *store.TelegramSetting) (*Service, error) {
	bot, err := tgbotapi.NewBotAPI(string(setting.BotToken))
	if err != nil {
		return nil, err
	}
	bot.Debug = false
	return &Service{
		st:            st,
		traderManager: tm,
		settings:      setting,
		bot:           bot,
		stopCh:        make(chan struct{}),
	}, nil
}

func (s *Service) Start() error {
	go s.loopUpdates()
	logger.Infof("📲 Telegram bot started for user %s", s.settings.UserID)
	return nil
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	if s.bot != nil {
		s.bot.StopReceivingUpdates()
	}
}

func (s *Service) SendText(text string) error {
	if !s.settings.Enabled || s.settings.ChatID == "" {
		return fmt.Errorf("telegram chat not configured")
	}
	chatID, err := strconv.ParseInt(s.settings.ChatID, 10, 64)
	if err != nil {
		return err
	}
	msg := tgbotapi.NewMessage(chatID, text)
	_, err = s.bot.Send(msg)
	return err
}

func (s *Service) loopUpdates() {
	updateCfg := tgbotapi.NewUpdate(0)
	updateCfg.Timeout = 60
	updates := s.bot.GetUpdatesChan(updateCfg)

	for {
		select {
		case update := <-updates:
			if update.Message != nil {
				s.handleMessage(update.Message)
			}
			if update.CallbackQuery != nil {
				s.handleCallback(update.CallbackQuery)
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *Service) handleMessage(msg *tgbotapi.Message) {
	if msg == nil || msg.Chat == nil {
		return
	}
	if !s.isChatAllowed(msg.Chat.ID) {
		s.replyText(msg.Chat.ID, "无权限访问该机器人。")
		return
	}

	if msg.IsCommand() {
		switch msg.Command() {
		case "start":
			s.onStart(msg.Chat.ID)
			return
		case "menu":
			s.sendMainMenu(msg.Chat.ID)
			return
		}
	}

	// Default: show menu
	s.sendMainMenu(msg.Chat.ID)
}

func (s *Service) handleCallback(cb *tgbotapi.CallbackQuery) {
	if cb == nil || cb.Message == nil || cb.Message.Chat == nil {
		return
	}
	_, _ = s.bot.Request(tgbotapi.NewCallback(cb.ID, ""))

	chatID := cb.Message.Chat.ID
	if !s.isChatAllowed(chatID) {
		s.replyText(chatID, "无权限访问该机器人。")
		return
	}

	data := cb.Data
	switch {
	case data == "panel:account":
		s.sendAccountPanel(chatID)
	case data == "panel:positions":
		s.sendPositionsPanel(chatID)
	case data == "panel:pending":
		s.sendPendingOrdersPanel(chatID)
	case data == "panel:orders":
		s.sendOrdersPanel(chatID)
	case data == "panel:fills":
		s.sendFillsPanel(chatID)
	case data == "panel:select":
		s.sendTraderSelectPanel(chatID)
	case strings.HasPrefix(data, "select:"):
		traderID := strings.TrimPrefix(data, "select:")
		s.setCurrentTrader(chatID, traderID)
		s.replyText(chatID, "已切换交易员。")
		s.sendMainMenu(chatID)
	case data == "action:start":
		s.startCurrentTrader(chatID)
	case data == "action:stop":
		s.stopCurrentTrader(chatID)
	case data == "action:delete":
		s.confirmDeleteTrader(chatID)
	case strings.HasPrefix(data, "delete:confirm:"):
		traderID := strings.TrimPrefix(data, "delete:confirm:")
		s.deleteTrader(chatID, traderID)
	case data == "delete:cancel":
		s.replyText(chatID, "已取消删除。")
		s.sendMainMenu(chatID)
	default:
		s.sendMainMenu(chatID)
	}
}

func (s *Service) onStart(chatID int64) {
	if s.settings.ChatID == "" {
		s.settings.ChatID = fmt.Sprintf("%d", chatID)
		_ = s.st.Telegram().UpsertSetting(s.settings)
	}
	s.sendMainMenu(chatID)
}

func (s *Service) sendMainMenu(chatID int64) {
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("账户概览", "panel:account"),
			tgbotapi.NewInlineKeyboardButtonData("持仓列表", "panel:positions"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("挂单列表", "panel:pending"),
			tgbotapi.NewInlineKeyboardButtonData("订单记录", "panel:orders"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("成交记录", "panel:fills"),
			tgbotapi.NewInlineKeyboardButtonData("选择交易员", "panel:select"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("启动交易员", "action:start"),
			tgbotapi.NewInlineKeyboardButtonData("停止交易员", "action:stop"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("删除交易员", "action:delete"),
		),
	)

	msg := tgbotapi.NewMessage(chatID, "请选择操作：")
	msg.ReplyMarkup = kb
	_, _ = s.bot.Send(msg)
}

func (s *Service) sendTraderSelectPanel(chatID int64) {
	traders, err := s.st.Trader().List(s.settings.UserID)
	if err != nil || len(traders) == 0 {
		s.replyText(chatID, "暂无可用交易员。")
		return
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	for _, t := range traders {
		label := fmt.Sprintf("%s (%s)", t.Name, t.ID[:8])
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(label, "select:"+t.ID),
		))
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)
	msg := tgbotapi.NewMessage(chatID, "请选择交易员：")
	msg.ReplyMarkup = kb
	_, _ = s.bot.Send(msg)
}

func (s *Service) sendAccountPanel(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	info, err := trader.GetAccountInfo()
	if err != nil {
		s.replyText(chatID, "获取账户信息失败。")
		return
	}

	name := trader.GetName()
	msg := fmt.Sprintf(
		"账户概览\n交易员: %s\n总净值: %.2f\n可用余额: %.2f\n未实现盈亏: %.2f\n总盈亏: %.2f (%.2f%%)\n持仓数: %d\n保证金占用: %.2f%%",
		name,
		asFloat(info["total_equity"]),
		asFloat(info["available_balance"]),
		asFloat(info["unrealized_profit"]),
		asFloat(info["total_pnl"]),
		asFloat(info["total_pnl_pct"]),
		asInt(info["position_count"]),
		asFloat(info["margin_used_pct"]),
	)
	s.replyText(chatID, msg)
}

func (s *Service) sendPositionsPanel(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	positions, err := trader.GetPositions()
	if err != nil {
		s.replyText(chatID, "获取持仓失败。")
		return
	}
	if len(positions) == 0 {
		s.replyText(chatID, "当前无持仓。")
		return
	}

	var sb strings.Builder
	sb.WriteString("持仓列表\n")
	for i, p := range positions {
		if i >= 10 {
			sb.WriteString("...仅显示前 10 条\n")
			break
		}
		sb.WriteString(fmt.Sprintf(
			"%s %s 数量: %.4f 入场: %.4f 标记: %.4f 盈亏: %.2f (%.2f%%)\n",
			p["symbol"], positionSideCN(fmt.Sprint(p["side"])),
			asFloat(p["quantity"]),
			asFloat(p["entry_price"]),
			asFloat(p["mark_price"]),
			asFloat(p["unrealized_pnl"]),
			asFloat(p["unrealized_pnl_pct"]),
		))
	}
	s.replyText(chatID, sb.String())
}

func (s *Service) sendPendingOrdersPanel(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	orders, err := trader.GetOpenLimitOrdersSnapshot("")
	if err != nil {
		s.replyText(chatID, "获取挂单失败。")
		return
	}
	if len(orders) == 0 {
		s.replyText(chatID, "当前无挂单。")
		return
	}

	var sb strings.Builder
	sb.WriteString("挂单列表\n")
	for i, o := range orders {
		if i >= 10 {
			sb.WriteString("...仅显示前 10 条\n")
			break
		}
		sb.WriteString(fmt.Sprintf(
			"%s %s 价格: %.4f 数量: %.4f\n",
			o.Symbol, sideCN(o.Side), o.Price, o.Quantity,
		))
	}
	s.replyText(chatID, sb.String())
}

func (s *Service) sendOrdersPanel(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	orders, err := s.st.Order().GetTraderOrders(trader.GetID(), 10)
	if err != nil {
		s.replyText(chatID, "获取订单失败。")
		return
	}
	if len(orders) == 0 {
		s.replyText(chatID, "暂无订单记录。")
		return
	}

	var sb strings.Builder
	sb.WriteString("订单记录\n")
	for _, o := range orders {
		sb.WriteString(fmt.Sprintf(
			"%s %s 状态:%s 价:%.4f 量:%.4f\n",
			o.Symbol, sideCN(o.Side), o.Status, o.Price, o.Quantity,
		))
	}
	s.replyText(chatID, sb.String())
}

func (s *Service) sendFillsPanel(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	fills, err := s.st.Order().GetTraderFills(trader.GetID(), 10)
	if err != nil {
		s.replyText(chatID, "获取成交失败。")
		return
	}
	if len(fills) == 0 {
		s.replyText(chatID, "暂无成交记录。")
		return
	}

	var sb strings.Builder
	sb.WriteString("成交记录\n")
	for _, f := range fills {
		ts := time.UnixMilli(f.CreatedAt).UTC().Format("01-02 15:04")
		sb.WriteString(fmt.Sprintf(
			"%s %s 价:%.4f 量:%.4f 盈亏:%.2f %s\n",
			f.Symbol, sideCN(f.Side), f.Price, f.Quantity, f.RealizedPnL, ts,
		))
	}
	s.replyText(chatID, sb.String())
}

func (s *Service) startCurrentTrader(chatID int64) {
	traderID := s.getCurrentTraderID(chatID)
	if traderID == "" {
		s.replyText(chatID, "请先选择交易员。")
		return
	}
	_, err := s.st.Trader().GetFullConfig(s.settings.UserID, traderID)
	if err != nil {
		s.replyText(chatID, "交易员不存在或无权限。")
		return
	}
	existing, _ := s.traderManager.GetTrader(traderID)
	if existing != nil {
		status := existing.GetStatus()
		if isRunning, ok := status["is_running"].(bool); ok && isRunning {
			s.replyText(chatID, "交易员已在运行。")
			return
		}
		s.traderManager.RemoveTrader(traderID)
	}
	if err := s.traderManager.LoadUserTradersFromStore(s.st, s.settings.UserID); err != nil {
		s.replyText(chatID, "加载交易员失败。")
		return
	}
	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		s.replyText(chatID, "启动失败，请检查配置。")
		return
	}
	go func() {
		if err := trader.Run(); err != nil {
			logger.Infof("❌ Trader %s runtime error: %v", trader.GetName(), err)
		}
	}()
	_ = s.st.Trader().UpdateStatus(s.settings.UserID, traderID, true)
	s.replyText(chatID, "交易员已启动。")
}

func (s *Service) stopCurrentTrader(chatID int64) {
	trader := s.getCurrentTrader(chatID)
	if trader == nil {
		return
	}
	status := trader.GetStatus()
	if isRunning, ok := status["is_running"].(bool); ok && !isRunning {
		s.replyText(chatID, "交易员已停止。")
		return
	}
	trader.Stop()
	_ = s.st.Trader().UpdateStatus(s.settings.UserID, trader.GetID(), false)
	s.replyText(chatID, "交易员已停止。")
}

func (s *Service) confirmDeleteTrader(chatID int64) {
	traderID := s.getCurrentTraderID(chatID)
	if traderID == "" {
		s.replyText(chatID, "请先选择交易员。")
		return
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("确认删除", "delete:confirm:"+traderID),
			tgbotapi.NewInlineKeyboardButtonData("取消", "delete:cancel"),
		),
	)
	msg := tgbotapi.NewMessage(chatID, "删除后无法恢复，确认删除该交易员？")
	msg.ReplyMarkup = kb
	_, _ = s.bot.Send(msg)
}

func (s *Service) deleteTrader(chatID int64, traderID string) {
	_ = s.st.Trader().Delete(s.settings.UserID, traderID)
	s.traderManager.RemoveTrader(traderID)
	if s.settings.DefaultTraderID == traderID {
		s.settings.DefaultTraderID = ""
		_ = s.st.Telegram().UpsertSetting(s.settings)
	}
	_ = s.st.Telegram().UpdateChatState(s.settings.UserID, fmt.Sprintf("%d", chatID), "")
	s.replyText(chatID, "交易员已删除。")
	s.sendMainMenu(chatID)
}

func (s *Service) getCurrentTrader(chatID int64) *trader.AutoTrader {
	traderID := s.getCurrentTraderID(chatID)
	if traderID == "" {
		s.replyText(chatID, "请先选择交易员。")
		return nil
	}
	t, err := s.traderManager.GetTrader(traderID)
	if err == nil {
		return t
	}
	_ = s.traderManager.LoadUserTradersFromStore(s.st, s.settings.UserID)
	t, err = s.traderManager.GetTrader(traderID)
	if err != nil {
		s.replyText(chatID, "交易员未加载，请稍后再试。")
		return nil
	}
	return t
}

func (s *Service) getCurrentTraderID(chatID int64) string {
	state, _ := s.st.Telegram().GetChatState(fmt.Sprintf("%d", chatID))
	if state != nil && state.CurrentTraderID != "" {
		return state.CurrentTraderID
	}
	if s.settings.DefaultTraderID != "" {
		return s.settings.DefaultTraderID
	}
	traders, err := s.st.Trader().List(s.settings.UserID)
	if err == nil && len(traders) == 1 {
		return traders[0].ID
	}
	return ""
}

func (s *Service) setCurrentTrader(chatID int64, traderID string) {
	_ = s.st.Telegram().UpdateChatState(s.settings.UserID, fmt.Sprintf("%d", chatID), traderID)
}

func (s *Service) replyText(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	_, _ = s.bot.Send(msg)
}

func (s *Service) isChatAllowed(chatID int64) bool {
	if s.settings.ChatID == "" {
		return true
	}
	return s.settings.ChatID == fmt.Sprintf("%d", chatID)
}

func asFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}

func asInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func sideCN(side string) string {
	switch strings.ToUpper(side) {
	case "BUY":
		return "买入"
	case "SELL":
		return "卖出"
	default:
		return side
	}
}

func positionSideCN(side string) string {
	switch strings.ToUpper(side) {
	case "LONG":
		return "多"
	case "SHORT":
		return "空"
	default:
		return side
	}
}
