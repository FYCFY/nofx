package notify

import (
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

// NotifyLimitFill notifies limit order fill
func NotifyLimitFill(userID, traderID, symbol, side string, price, qty float64) {
	svc := get()
	if svc == nil || svc.sender == nil {
		return
	}
	name := svc.traderName(traderID)
	title := "挂单成交 ✅"
	sideText := sideLabel(side)
	msg := fmt.Sprintf("%s\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n时间: %s",
		title, name, symbol, sideText, price, qty, time.Now().Format("2006-01-02 15:04:05"))
	_ = svc.sender.SendMessage(userID, msg)
}

// NotifyOpenTrade notifies market open trade
func NotifyOpenTrade(userID, traderID string, trade OpenTradeInfo) {
	svc := get()
	if svc == nil || svc.sender == nil {
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

// NotifyCloseTrade notifies close trade (stop loss / take profit / close)
func NotifyCloseTrade(userID, traderID string, trade TradeInfo) {
	svc := get()
	if svc == nil || svc.sender == nil {
		return
	}
	if trade.OrderAction != "close_long" && trade.OrderAction != "close_short" {
		return
	}
	key := fmt.Sprintf("%s|%s|%s|%s|%.6f",
		userID, traderID, trade.OrderAction, trade.Symbol, trade.Quantity)
	if !closeDeduper.allow(key) {
		return
	}
	name := svc.traderName(traderID)
	title := "平仓完成 ✅"
	if trade.RealizedPnL > 0 {
		title = "止盈平仓成交 ✅"
	} else if trade.RealizedPnL < 0 {
		title = "止损平仓成交 ⚠️"
	}
	direction := "平多"
	if trade.OrderAction == "close_short" {
		direction = "平空"
	}
	msg := fmt.Sprintf("%s\n交易员: %s\n品种: %s\n方向: %s\n价格: %.4f\n数量: %.4f\n盈亏: %.2f\n时间: %s",
		title, name, trade.Symbol, direction, trade.Price, trade.Quantity, trade.RealizedPnL, trade.Time.Format("2006-01-02 15:04:05"))
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
