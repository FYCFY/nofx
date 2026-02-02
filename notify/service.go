package notify

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"nofx/store"
)

// Sender defines minimal interface to send messages
type Sender interface {
	SendMessage(userID, text string) error
}

// Service provides notification utilities
type Service struct {
	st     *store.Store
	sender Sender
}

var global *Service

// Init initializes global notification service
func Init(st *store.Store, sender Sender) {
	global = &Service{st: st, sender: sender}
}

// get returns global service
func get() *Service {
	return global
}

// TradeInfo is a simplified trade payload for notifications
type TradeInfo struct {
	Symbol      string
	OrderAction string // close_long | close_short
	Side        string // BUY/SELL
	Price       float64
	Quantity    float64
	RealizedPnL float64
	Fee         float64
	OrderID     string
	Time        time.Time
}

type OpenTradeInfo struct {
	Symbol   string
	Side     string // BUY/SELL
	Price    float64
	Quantity float64
	Leverage int
	Time     time.Time
}

type closeTradeDeduper struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

func (d *closeTradeDeduper) allow(key string) bool {
	if d == nil {
		return true
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, ts := range d.seen {
		if now.Sub(ts) > d.ttl {
			delete(d.seen, k)
		}
	}
	if ts, ok := d.seen[key]; ok && now.Sub(ts) <= d.ttl {
		return false
	}
	d.seen[key] = now
	return true
}

var closeDeduper = &closeTradeDeduper{
	seen: make(map[string]time.Time),
	ttl:  5 * time.Minute,
}

type NotificationType string

const (
	NotifyTypeLimitOrderPlaced NotificationType = "limit_order_placed"
	NotifyTypeLimitOrderFilled NotificationType = "limit_order_filled"
	NotifyTypeMarketOpen       NotificationType = "market_open"
	NotifyTypeUpdateStopLoss   NotificationType = "update_stop_loss"
	NotifyTypeUpdateTakeProfit NotificationType = "update_take_profit"
	NotifyTypeCloseTakeProfit  NotificationType = "close_take_profit"
	NotifyTypeCloseStopLoss    NotificationType = "close_stop_loss"
	NotifyTypeCloseManual      NotificationType = "close_manual"
)

func (s *Service) shouldNotify(userID, traderID string, notifyType NotificationType) bool {
	if s == nil || s.sender == nil || s.st == nil {
		return false
	}
	setting, err := s.st.Telegram().GetSetting(userID)
	if err != nil || setting == nil || !setting.Enabled {
		return false
	}
	if !isTraderEnabled(setting.EnabledTraderIDs, traderID) {
		return false
	}
	if !isNotifyTypeEnabled(setting.NotifyTypes, notifyType) {
		return false
	}
	return true
}

func isTraderEnabled(raw string, traderID string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return true
	}
	if len(ids) == 0 {
		return true
	}
	for _, id := range ids {
		if id == traderID {
			return true
		}
	}
	return false
}

func isNotifyTypeEnabled(raw string, notifyType NotificationType) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	var types map[string]bool
	if err := json.Unmarshal([]byte(raw), &types); err != nil {
		return true
	}
	if len(types) == 0 {
		return true
	}
	enabled, ok := types[string(notifyType)]
	return ok && enabled
}

// NotifyLimitFill notifies limit order fill
func NotifyLimitFill(userID, traderID, symbol, side string, price, qty float64) {
	svc := get()
	if svc == nil || !svc.shouldNotify(userID, traderID, NotifyTypeLimitOrderFilled) {
		return
	}
	name := svc.traderName(traderID)
	title := "挂单开仓成功 ✅"
	sideText := sideLabel(side)
	msg := fmt.Sprintf("%s\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n时间: %s",
		title, name, symbol, sideText, price, qty, time.Now().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyLimitOrderPlaced notifies limit order placement
func NotifyLimitOrderPlaced(userID, traderID, symbol, side string, price, qty float64) {
	svc := get()
	if svc == nil || !svc.shouldNotify(userID, traderID, NotifyTypeLimitOrderPlaced) {
		return
	}
	name := svc.traderName(traderID)
	sideText := sideLabel(side)
	msg := fmt.Sprintf("限价委托挂单 ✅\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n时间: %s",
		name, symbol, sideText, price, qty, time.Now().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyOpenTrade notifies market open trade
func NotifyOpenTrade(userID, traderID string, trade OpenTradeInfo) {
	svc := get()
	if svc == nil || !svc.shouldNotify(userID, traderID, NotifyTypeMarketOpen) {
		return
	}
	name := svc.traderName(traderID)
	direction := "开多"
	if strings.ToUpper(trade.Side) == "SELL" {
		direction = "开空"
	}
	msg := fmt.Sprintf("市价开仓 ✅\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n杠杆: %dx\n时间: %s",
		name, trade.Symbol, direction, trade.Price, trade.Quantity, trade.Leverage, trade.Time.Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyUpdateStopLoss notifies stop loss update
func NotifyUpdateStopLoss(userID, traderID, symbol string, price float64, qty float64) {
	svc := get()
	if svc == nil || !svc.shouldNotify(userID, traderID, NotifyTypeUpdateStopLoss) {
		return
	}
	name := svc.traderName(traderID)
	msg := fmt.Sprintf("更新止损 ✅\n交易员: %s\n品种: %s\n止损价: %.4f\n数量: %.4f\n时间: %s",
		name, symbol, price, qty, time.Now().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyUpdateTakeProfit notifies take profit update
func NotifyUpdateTakeProfit(userID, traderID, symbol string, price float64, qty float64) {
	svc := get()
	if svc == nil || !svc.shouldNotify(userID, traderID, NotifyTypeUpdateTakeProfit) {
		return
	}
	name := svc.traderName(traderID)
	msg := fmt.Sprintf("更新止盈 ✅\n交易员: %s\n品种: %s\n止盈价: %.4f\n数量: %.4f\n时间: %s",
		name, symbol, price, qty, time.Now().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyCloseTrade notifies close trade (stop loss / take profit / close)
func NotifyCloseTrade(userID, traderID string, trade TradeInfo) {
	svc := get()
	if svc == nil || svc.sender == nil || svc.st == nil {
		return
	}
	if trade.OrderAction != "close_long" && trade.OrderAction != "close_short" {
		return
	}

	pos, err := svc.findClosedPosition(traderID, trade)
	if err != nil || pos == nil {
		return
	}
	key := fmt.Sprintf("%s|%s|%d", userID, traderID, pos.ID)
	if !closeDeduper.allow(key) {
		return
	}

	notifyType := NotifyTypeCloseManual
	title := "直接平仓 ✅"
	if pos.RealizedPnL > 0 {
		notifyType = NotifyTypeCloseTakeProfit
		title = "止盈平仓成交 ✅"
	} else if pos.RealizedPnL < 0 {
		notifyType = NotifyTypeCloseStopLoss
		title = "止损平仓成交 ⚠️"
	}
	if !svc.shouldNotify(userID, traderID, notifyType) {
		return
	}

	name := svc.traderName(traderID)
	direction := "平多"
	if strings.ToLower(pos.Side) == "short" {
		direction = "平空"
	}
	msg := fmt.Sprintf("%s\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n盈亏: %.2f\n手续费: %.4f\n时间: %s",
		title, name, pos.Symbol, direction, pos.ExitPrice, pos.EntryQuantity, pos.RealizedPnL, pos.Fee,
		time.UnixMilli(pos.ExitTime).UTC().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// CheckEquityTarget checks target equity rule and notifies if reached
func CheckEquityTarget(userID, traderID string, equity float64) {
	svc := get()
	if svc == nil || svc.sender == nil || svc.st == nil {
		return
	}
	rule, err := svc.st.Notify().GetRule(traderID)
	if err != nil || rule == nil {
		return
	}
	if rule.TargetEquity <= 0 {
		return
	}

	triggered := rule.Triggered
	shouldSend := false
	if rule.TriggerMode == "cross" {
		if rule.LastEquity < rule.TargetEquity && equity >= rule.TargetEquity {
			shouldSend = true
		}
	} else {
		if !triggered && equity >= rule.TargetEquity {
			shouldSend = true
		}
	}

	if shouldSend {
		name := svc.traderName(traderID)
		msg := fmt.Sprintf("净值达到目标 🎯\n交易员: %s\n当前净值: %.2f\n目标净值: %.2f\n时间: %s",
			name, equity, rule.TargetEquity, time.Now().Format("2006-01-02 15:04:05"))
		_ = svc.sender.SendMessage(userID, msg)
		if rule.TriggerMode == "once" {
			triggered = true
		}
	}

	_ = svc.st.Notify().UpdateTriggerState(traderID, triggered, equity)
}

func (s *Service) findClosedPosition(traderID string, trade TradeInfo) (*store.TraderPosition, error) {
	if s == nil || s.st == nil {
		return nil, nil
	}
	if trade.OrderID != "" {
		pos, err := s.st.Position().GetClosedPositionByExitOrderID(traderID, trade.OrderID)
		if err != nil {
			return nil, err
		}
		if pos != nil {
			return pos, nil
		}
	}

	side := "LONG"
	if trade.OrderAction == "close_short" {
		side = "SHORT"
	}
	pos, err := s.st.Position().GetLatestClosedPositionBySymbol(traderID, trade.Symbol, side)
	if err != nil {
		return nil, err
	}
	if pos == nil {
		return nil, nil
	}
	if !trade.Time.IsZero() {
		delta := pos.ExitTime - trade.Time.UTC().UnixMilli()
		if delta < 0 {
			delta = -delta
		}
		if delta > int64(10*time.Minute/time.Millisecond) {
			return nil, nil
		}
	}
	return pos, nil
}

func (s *Service) traderName(traderID string) string {
	if s == nil || s.st == nil {
		return traderID
	}
	trader, err := s.st.Trader().GetByID(traderID)
	if err != nil || trader == nil {
		return traderID
	}
	return trader.Name
}

func sideLabel(side string) string {
	switch strings.ToUpper(side) {
	case "BUY":
		return "买入"
	case "SELL":
		return "卖出"
	default:
		return side
	}
}
