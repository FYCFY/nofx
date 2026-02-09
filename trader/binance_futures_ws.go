package trader

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"nofx/logger"

	"github.com/adshao/go-binance/v2/futures"
)

const (
	userStreamKeepaliveInterval = 25 * time.Minute
	userStreamStaleDuration     = 2 * time.Minute
	userStreamBackoffMax        = time.Minute
)

func (t *FuturesTrader) startUserStream() {
	go t.userStreamLoop()
}

func (t *FuturesTrader) userStreamLoop() {
	backoff := time.Second
	for {
		listenKey, err := t.client.NewStartUserStreamService().Do(context.Background())
		if err != nil {
			logger.Infof("⚠️ Failed to start Binance user stream: %v", err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		logger.Infof("🔌 Binance user stream started")
		backoff = time.Second

		var stopOnce sync.Once
		var stopC chan struct{}
		stop := func() {
			if stopC == nil {
				return
			}
			stopOnce.Do(func() { close(stopC) })
		}

		handler := func(event *futures.WsUserDataEvent) {
			t.handleUserStreamEvent(event)
			if event != nil && event.Event == futures.UserDataEventTypeListenKeyExpired {
				logger.Infof("⚠️ Binance user stream listenKey expired, reconnecting")
				stop()
			}
		}
		errHandler := func(err error) {
			logger.Infof("⚠️ Binance user stream error: %v", err)
			stop()
		}

		doneC, stopChan, err := futures.WsUserDataServe(listenKey, handler, errHandler)
		if err != nil {
			logger.Infof("⚠️ Failed to connect Binance user stream: %v", err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		stopC = stopChan
		t.setUserStreamActive(true)

		keepaliveTicker := time.NewTicker(userStreamKeepaliveInterval)
		for {
			select {
			case <-keepaliveTicker.C:
				if err := t.client.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(context.Background()); err != nil {
					logger.Infof("⚠️ Binance user stream keepalive failed: %v", err)
				}
			case <-doneC:
				keepaliveTicker.Stop()
				t.setUserStreamActive(false)
				logger.Infof("⚠️ Binance user stream disconnected, reconnecting")
				goto Reconnect
			}
		}

	Reconnect:
		time.Sleep(backoff)
		backoff = nextBackoff(backoff)
	}
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > userStreamBackoffMax {
		return userStreamBackoffMax
	}
	return next
}

func (t *FuturesTrader) handleUserStreamEvent(event *futures.WsUserDataEvent) {
	if event == nil {
		return
	}
	t.markUserStreamEvent()

	switch event.Event {
	case futures.UserDataEventTypeAccountUpdate:
		t.updateFromAccountUpdate(event.WsUserDataAccountUpdate.AccountUpdate)
	case futures.UserDataEventTypeOrderTradeUpdate:
		t.updateFromOrderTradeUpdate(event.WsUserDataOrderTradeUpdate.OrderTradeUpdate)
	case futures.UserDataEventTypeAlgoUpdate:
		t.updateFromAlgoUpdate(event.WsUserDataAlgoUpdate.AlgoUpdate)
	}
}

func (t *FuturesTrader) updateFromAccountUpdate(update futures.WsAccountUpdate) {
	walletBalance, availableBalance := extractFuturesBalances(update.Balances)

	var positions []map[string]interface{}
	posSnapshots := make([]BinancePositionSnapshot, 0, len(update.Positions))
	totalUnrealized := 0.0
	for _, pos := range update.Positions {
		amount := parseFloatWS(pos.Amount)
		if amount == 0 {
			continue
		}

		entryPrice := parseFloatWS(pos.EntryPrice)
		markPrice := parseFloatWS(pos.MarkPrice)
		unrealized := parseFloatWS(pos.UnrealizedPnL)
		totalUnrealized += unrealized

		side := "long"
		if amount < 0 {
			side = "short"
		}

		positions = append(positions, map[string]interface{}{
			"symbol":           pos.Symbol,
			"positionAmt":      amount,
			"entryPrice":       entryPrice,
			"markPrice":        markPrice,
			"unRealizedProfit": unrealized,
			"leverage":         10.0,
			"liquidationPrice": 0.0,
			"side":             side,
		})
		posSnapshots = append(posSnapshots, BinancePositionSnapshot{
			Symbol:           pos.Symbol,
			PositionAmt:      amount,
			EntryPrice:       entryPrice,
			MarkPrice:        markPrice,
			UnRealizedProfit: unrealized,
			Leverage:         10.0,
			LiquidationPrice: 0,
			Side:             side,
		})
	}

	balance := map[string]interface{}{
		"totalWalletBalance":    walletBalance,
		"availableBalance":      availableBalance,
		"totalUnrealizedProfit": totalUnrealized,
	}

	t.balanceCacheMutex.Lock()
	t.cachedBalance = balance
	t.balanceCacheTime = time.Now()
	t.balanceCacheMutex.Unlock()

	t.positionsCacheMutex.Lock()
	t.cachedPositions = positions
	t.positionsCacheTime = time.Now()
	t.positionsCacheMutex.Unlock()

	if t.realtimeEngine != nil {
		t.realtimeEngine.SetBaselineFromAccountSnapshot(&BinanceAccountSnapshot{
			TotalWalletBalance:    walletBalance,
			AvailableBalance:      availableBalance,
			TotalUnrealizedProfit: totalUnrealized,
			Positions:             posSnapshots,
			UpdateTime:            time.Now().UTC(),
		})
	}
}

func (t *FuturesTrader) updateFromOrderTradeUpdate(update futures.WsOrderTradeUpdate) {
	orderID := strconv.FormatInt(update.ID, 10)
	order := OpenOrder{
		OrderID:      orderID,
		Symbol:       update.Symbol,
		Side:         string(update.Side),
		PositionSide: string(update.PositionSide),
		Type:         string(update.Type),
		Price:        parseFloatWS(update.OriginalPrice),
		StopPrice:    parseFloatWS(update.StopPrice),
		Quantity:     parseFloatWS(update.OriginalQty),
		Status:       string(update.Status),
	}

	t.openOrdersMu.Lock()
	if isTerminalOrderStatus(order.Status) {
		delete(t.openOrders, order.OrderID)
	} else {
		t.openOrders[order.OrderID] = order
	}
	t.openOrdersCache = time.Now()
	t.openOrdersMu.Unlock()

	if strings.ToUpper(string(update.ExecutionType)) != "TRADE" || update.TradeID <= 0 {
		return
	}
	trade := TradeRecord{
		TradeID:      strconv.FormatInt(update.TradeID, 10),
		Symbol:       update.Symbol,
		Side:         string(update.Side),
		PositionSide: string(update.PositionSide),
		Price:        parseFloatWS(update.LastFilledPrice),
		Quantity:     parseFloatWS(update.LastFilledQty),
		RealizedPnL:  parseFloatWS(update.RealizedPnL),
		Fee:          parseFloatWS(update.Commission),
		Time:         time.UnixMilli(update.TradeTime).UTC(),
	}
	t.appendWsTrade(trade)

	if t.realtimeEngine != nil {
		_ = t.realtimeEngine.EnsureSymbol(update.Symbol)
	}
}

func (t *FuturesTrader) updateFromAlgoUpdate(update futures.WsAlgoUpdate) {
	order := OpenOrder{
		OrderID:      update.OrderID,
		Symbol:       update.Symbol,
		Side:         string(update.Side),
		PositionSide: string(update.PositionSide),
		Type:         string(update.OrderType),
		Price:        parseFloatWS(update.OrderPrice),
		StopPrice:    parseFloatWS(update.TriggerPrice),
		Quantity:     parseFloatWS(update.Quantity),
		Status:       update.AlgoStatus,
	}
	t.openOrdersMu.Lock()
	if isTerminalOrderStatus(order.Status) {
		delete(t.openOrders, order.OrderID)
	} else {
		t.openOrders[order.OrderID] = order
	}
	t.openOrdersCache = time.Now()
	t.openOrdersMu.Unlock()
}

func extractFuturesBalances(balances []futures.WsBalance) (walletBalance, availableBalance float64) {
	for _, bal := range balances {
		if strings.EqualFold(bal.Asset, "USDT") {
			return parseFloatWS(bal.Balance), parseFloatWS(bal.CrossWalletBalance)
		}
	}

	for _, bal := range balances {
		walletBalance += parseFloatWS(bal.Balance)
		availableBalance += parseFloatWS(bal.CrossWalletBalance)
	}

	return walletBalance, availableBalance
}

func parseFloatWS(value string) float64 {
	if value == "" {
		return 0
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return f
}

func (t *FuturesTrader) markUserStreamEvent() {
	t.userStreamMu.Lock()
	t.userStreamLastEvent = time.Now()
	t.userStreamMu.Unlock()
}

func (t *FuturesTrader) setUserStreamActive(active bool) {
	t.userStreamMu.Lock()
	t.userStreamActive = active
	if !active {
		t.userStreamLastEvent = time.Time{}
	}
	t.userStreamMu.Unlock()
}

func (t *FuturesTrader) isUserStreamFresh() bool {
	t.userStreamMu.RLock()
	active := t.userStreamActive
	lastEvent := t.userStreamLastEvent
	t.userStreamMu.RUnlock()

	if !active || lastEvent.IsZero() {
		return false
	}
	return time.Since(lastEvent) < userStreamStaleDuration
}

func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Too many requests") || strings.Contains(msg, "-1003")
}

func isTerminalOrderStatus(status string) bool {
	s := strings.ToUpper(strings.TrimSpace(status))
	return s == "FILLED" || s == "CANCELED" || s == "CANCELLED" || s == "EXPIRED" || s == "REJECTED"
}

func (t *FuturesTrader) appendWsTrade(trade TradeRecord) {
	if trade.TradeID == "" || trade.Symbol == "" {
		return
	}
	key := trade.Symbol + ":" + trade.TradeID
	t.wsTradesMu.Lock()
	if _, exists := t.wsTradeSeen[key]; exists {
		t.wsTradesMu.Unlock()
		return
	}
	t.wsTradeSeen[key] = struct{}{}
	t.wsTrades = append(t.wsTrades, trade)
	if len(t.wsTrades) > t.wsTradesMaxSize {
		drop := len(t.wsTrades) - t.wsTradesMaxSize
		for i := 0; i < drop; i++ {
			old := t.wsTrades[i]
			delete(t.wsTradeSeen, old.Symbol+":"+old.TradeID)
		}
		t.wsTrades = t.wsTrades[drop:]
	}
	t.wsTradesMu.Unlock()
}

func (t *FuturesTrader) getWsTradesSince(tsMs int64) []TradeRecord {
	t.wsTradesMu.RLock()
	defer t.wsTradesMu.RUnlock()
	if len(t.wsTrades) == 0 {
		return nil
	}
	out := make([]TradeRecord, 0, len(t.wsTrades))
	for _, tr := range t.wsTrades {
		if tr.Time.UnixMilli() >= tsMs {
			out = append(out, tr)
		}
	}
	return out
}
