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
	}
}

func (t *FuturesTrader) updateFromAccountUpdate(update futures.WsAccountUpdate) {
	walletBalance, availableBalance := extractFuturesBalances(update.Balances)

	var positions []map[string]interface{}
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
			"liquidationPrice": 0.0,
			"side":             side,
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
