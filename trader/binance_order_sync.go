package trader

import (
	"fmt"
	"nofx/logger"
	"nofx/market"
	"nofx/notify"
	"nofx/store"
	"sort"
	"strings"
	"sync"
	"time"
)

// syncState stores the last sync time (Unix ms) for incremental sync
var (
	binanceSyncState      = make(map[string]int64) // exchangeID -> lastSyncTimeMs (Unix ms)
	binanceSyncStateMutex sync.RWMutex
)

// SyncOrdersFromBinance consumes incremental ORDER_TRADE_UPDATE events captured from Binance user stream
// and persists them to local database.
// In strict websocket mode this method never calls Binance REST APIs.
func (t *FuturesTrader) SyncOrdersFromBinance(traderID string, exchangeID string, exchangeType string, st *store.Store) error {
	if st == nil {
		return fmt.Errorf("store is nil")
	}

	nowMs := time.Now().UTC().UnixMilli()
	orderStore := st.Order()
	positionStore := st.Position()
	posBuilder := store.NewPositionBuilder(positionStore)

	userID := ""
	if traderCfg, err := st.Trader().GetByID(traderID); err == nil && traderCfg != nil {
		userID = traderCfg.UserID
	}

	binanceSyncStateMutex.RLock()
	lastSyncTimeMs, exists := binanceSyncState[exchangeID]
	binanceSyncStateMutex.RUnlock()

	if !exists {
		lastFillTimeMs, err := orderStore.GetLastFillTimeByExchange(exchangeID)
		if err == nil && lastFillTimeMs > 0 && lastFillTimeMs <= nowMs {
			lastSyncTimeMs = lastFillTimeMs + 1
		} else {
			lastSyncTimeMs = nowMs - 24*60*60*1000
		}
	}

	trades := t.getWsTradesSince(lastSyncTimeMs)
	if len(trades) == 0 {
		return nil
	}

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Time.UnixMilli() < trades[j].Time.UnixMilli()
	})

	syncedCount := 0
	skippedCount := 0
	latestTradeTimeMs := lastSyncTimeMs

	for _, trade := range trades {
		existing, err := orderStore.GetOrderByExchangeID(exchangeID, trade.TradeID)
		if err == nil && existing != nil {
			skippedCount++
			if tm := trade.Time.UTC().UnixMilli(); tm > latestTradeTimeMs {
				latestTradeTimeMs = tm
			}
			continue
		}

		symbol := market.Normalize(trade.Symbol)
		orderAction := t.determineOrderAction(trade.Side, trade.PositionSide, trade.RealizedPnL)

		positionSide := strings.ToUpper(trade.PositionSide)
		if positionSide == "" || positionSide == "BOTH" {
			if strings.Contains(orderAction, "long") {
				positionSide = "LONG"
			} else {
				positionSide = "SHORT"
			}
		}
		side := strings.ToUpper(trade.Side)
		tradeTimeMs := trade.Time.UTC().UnixMilli()

		orderRecord := &store.TraderOrder{
			TraderID:        traderID,
			ExchangeID:      exchangeID,
			ExchangeType:    exchangeType,
			ExchangeOrderID: trade.TradeID,
			Symbol:          symbol,
			Side:            side,
			PositionSide:    positionSide,
			Type:            "MARKET",
			OrderAction:     orderAction,
			Quantity:        trade.Quantity,
			Price:           trade.Price,
			Status:          "FILLED",
			FilledQuantity:  trade.Quantity,
			AvgFillPrice:    trade.Price,
			Commission:      trade.Fee,
			FilledAt:        tradeTimeMs,
			CreatedAt:       tradeTimeMs,
			UpdatedAt:       tradeTimeMs,
		}
		if err := orderStore.CreateOrder(orderRecord); err != nil {
			logger.Infof("  ⚠️ Failed to sync ws trade %s: %v", trade.TradeID, err)
			continue
		}

		fillRecord := &store.TraderFill{
			TraderID:        traderID,
			ExchangeID:      exchangeID,
			ExchangeType:    exchangeType,
			OrderID:         orderRecord.ID,
			ExchangeOrderID: trade.TradeID,
			ExchangeTradeID: trade.TradeID,
			Symbol:          symbol,
			Side:            side,
			Price:           trade.Price,
			Quantity:        trade.Quantity,
			QuoteQuantity:   trade.Price * trade.Quantity,
			Commission:      trade.Fee,
			CommissionAsset: "USDT",
			RealizedPnL:     trade.RealizedPnL,
			IsMaker:         false,
			CreatedAt:       tradeTimeMs,
		}
		if err := orderStore.CreateFill(fillRecord); err != nil {
			logger.Infof("  ⚠️ Failed to sync fill for ws trade %s: %v", trade.TradeID, err)
		}

		if err := posBuilder.ProcessTrade(
			traderID, exchangeID, exchangeType,
			symbol, positionSide, orderAction,
			trade.Quantity, trade.Price, trade.Fee, trade.RealizedPnL,
			tradeTimeMs, trade.TradeID,
		); err != nil {
			logger.Infof("  ⚠️ Failed to sync position for ws trade %s: %v", trade.TradeID, err)
		}

		if userID != "" {
			notify.NotifyCloseTrade(userID, traderID, notify.TradeInfo{
				Symbol:      symbol,
				OrderAction: orderAction,
				Side:        side,
				Price:       trade.Price,
				Quantity:    trade.Quantity,
				RealizedPnL: trade.RealizedPnL,
				Fee:         trade.Fee,
				OrderID:     trade.TradeID,
				Time:        trade.Time.UTC(),
			})
		}

		syncedCount++
		if tm := trade.Time.UTC().UnixMilli(); tm > latestTradeTimeMs {
			latestTradeTimeMs = tm
		}
	}

	binanceSyncStateMutex.Lock()
	binanceSyncState[exchangeID] = latestTradeTimeMs + 1
	binanceSyncStateMutex.Unlock()

	if syncedCount > 0 || skippedCount > 0 {
		logger.Infof("✅ Binance WS order sync completed: %d new trades synced, %d skipped", syncedCount, skippedCount)
	}
	return nil
}

// getPositionSymbols returns list of symbols that have active positions.
func (t *FuturesTrader) getPositionSymbols() []string {
	positions, err := t.GetPositions()
	if err != nil {
		return nil
	}

	var symbols []string
	for _, pos := range positions {
		if symbol, ok := pos["symbol"].(string); ok && symbol != "" {
			symbols = append(symbols, symbol)
		}
	}
	return symbols
}

// determineOrderAction determines the order action based on trade data.
func (t *FuturesTrader) determineOrderAction(side, positionSide string, realizedPnL float64) string {
	side = strings.ToUpper(side)
	positionSide = strings.ToUpper(positionSide)

	isClose := realizedPnL != 0

	if positionSide == "LONG" || positionSide == "" {
		if side == "BUY" {
			if isClose {
				return "close_short"
			}
			return "open_long"
		}
		if isClose {
			return "close_long"
		}
		return "open_short"
	}
	if positionSide == "SHORT" {
		if side == "SELL" {
			if isClose {
				return "close_long"
			}
			return "open_short"
		}
		if isClose {
			return "close_short"
		}
		return "open_long"
	}

	if side == "BUY" {
		return "open_long"
	}
	return "open_short"
}

// StartOrderSync starts background WS-incremental order sync task for Binance.
func (t *FuturesTrader) StartOrderSync(traderID string, exchangeID string, exchangeType string, st *store.Store, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("❌ Binance WS order sync panic recovered (initial): %v", r)
			}
		}()
		if err := t.SyncOrdersFromBinance(traderID, exchangeID, exchangeType, st); err != nil {
			logger.Infof("⚠️ Initial Binance WS order sync failed: %v", err)
		}
	}()

	ticker := time.NewTicker(interval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("❌ Binance WS order sync panic recovered (ticker): %v", r)
				ticker.Stop()
				go t.StartOrderSync(traderID, exchangeID, exchangeType, st, interval)
			}
		}()
		for range ticker.C {
			if err := t.SyncOrdersFromBinance(traderID, exchangeID, exchangeType, st); err != nil {
				logger.Infof("⚠️ Binance WS order sync failed: %v", err)
			}
		}
	}()
	logger.Infof("🔄 Binance WS order sync started (interval: %v)", interval)
}
