package trader

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type RealtimePosition struct {
	Symbol           string
	PositionAmt      float64
	EntryPrice       float64
	MarkPrice        float64
	UnRealizedProfit float64
	Leverage         float64
	LiquidationPrice float64
	Side             string
}

type RealtimeAccountSnapshot struct {
	TotalWalletBalance    float64
	AvailableBalance      float64
	TotalUnrealizedProfit float64
	TotalEquity           float64
	Positions             []RealtimePosition
	UpdatedAt             time.Time
	Stale                 bool
}

type BinanceRealtimeAccountEngine struct {
	gateway *binanceWsReadGateway

	recalcInterval time.Duration
	maxStaleAge    time.Duration
	leverageLookup func(symbol string) (float64, bool)

	mu sync.RWMutex

	hasBaseline       bool
	baselineWallet    float64
	baselineAvailable float64
	baselineUnreal    float64
	baselinePositions map[string]RealtimePosition
	lastSnapshot      RealtimeAccountSnapshot

	stopCh chan struct{}
}

func newBinanceRealtimeAccountEngine(gateway *binanceWsReadGateway, recalcInterval time.Duration) *BinanceRealtimeAccountEngine {
	if recalcInterval <= 0 {
		recalcInterval = 3 * time.Second
	}
	eng := &BinanceRealtimeAccountEngine{
		gateway:           gateway,
		recalcInterval:    recalcInterval,
		maxStaleAge:       10 * time.Second,
		baselinePositions: make(map[string]RealtimePosition),
		stopCh:            make(chan struct{}),
	}
	go eng.loop()
	return eng
}

func (e *BinanceRealtimeAccountEngine) SetLeverageLookup(fn func(symbol string) (float64, bool)) {
	e.mu.Lock()
	e.leverageLookup = fn
	e.mu.Unlock()
}

func (e *BinanceRealtimeAccountEngine) loop() {
	ticker := time.NewTicker(e.recalcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.recalculate()
		case <-e.stopCh:
			return
		}
	}
}

func (e *BinanceRealtimeAccountEngine) Stop() {
	select {
	case <-e.stopCh:
		return
	default:
		close(e.stopCh)
	}
}

func (e *BinanceRealtimeAccountEngine) SetBaselineFromAccountSnapshot(snap *BinanceAccountSnapshot) {
	if snap == nil {
		return
	}

	positions := make(map[string]RealtimePosition)
	for _, p := range snap.Positions {
		if p.PositionAmt == 0 {
			continue
		}
		symbol := strings.ToUpper(p.Symbol)
		side := p.Side
		if side == "" {
			if p.PositionAmt >= 0 {
				side = "long"
			} else {
				side = "short"
			}
		}
		positions[symbol] = RealtimePosition{
			Symbol:           symbol,
			PositionAmt:      p.PositionAmt,
			EntryPrice:       p.EntryPrice,
			MarkPrice:        p.MarkPrice,
			UnRealizedProfit: p.UnRealizedProfit,
			Leverage:         p.Leverage,
			LiquidationPrice: p.LiquidationPrice,
			Side:             side,
		}
	}

	e.mu.Lock()
	e.hasBaseline = true
	e.baselineWallet = snap.TotalWalletBalance
	e.baselineAvailable = snap.AvailableBalance
	e.baselineUnreal = snap.TotalUnrealizedProfit
	e.baselinePositions = positions
	e.mu.Unlock()

	for symbol := range positions {
		_ = e.EnsureSymbol(symbol)
	}
	e.recalculate()
}

func (e *BinanceRealtimeAccountEngine) EnsureSymbol(symbol string) error {
	if e.gateway == nil {
		return fmt.Errorf("binance ws gateway not initialized")
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil
	}
	return e.gateway.ensureMarkPriceStream(symbol)
}

func (e *BinanceRealtimeAccountEngine) recalculate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.hasBaseline {
		return
	}

	totalUnrealized := 0.0
	stale := false
	positions := make([]RealtimePosition, 0, len(e.baselinePositions))

	for symbol, pos := range e.baselinePositions {
		if e.gateway != nil {
			if err := e.gateway.ensureMarkPriceStream(symbol); err != nil {
				stale = true
			}
			mark, ts, ok := e.gateway.getCachedMarkPrice(symbol)
			if ok {
				pos.MarkPrice = mark
				if pos.EntryPrice > 0 && pos.PositionAmt != 0 {
					qty := pos.PositionAmt
					if qty < 0 {
						qty = -qty
					}
					if pos.Side == "short" || pos.PositionAmt < 0 {
						pos.UnRealizedProfit = (pos.EntryPrice - mark) * qty
					} else {
						pos.UnRealizedProfit = (mark - pos.EntryPrice) * qty
					}
				}
				if time.Since(ts) > 2*e.recalcInterval {
					stale = true
				}
			} else {
				stale = true
			}
		} else {
			stale = true
		}

		if e.leverageLookup != nil {
			if lev, ok := e.leverageLookup(symbol); ok && lev > 0 {
				pos.Leverage = lev
			}
		}

		e.baselinePositions[symbol] = pos
		totalUnrealized += pos.UnRealizedProfit
		positions = append(positions, pos)
	}

	sort.Slice(positions, func(i, j int) bool { return positions[i].Symbol < positions[j].Symbol })

	available := e.baselineAvailable + (totalUnrealized - e.baselineUnreal)
	if available < 0 {
		available = 0
	}

	e.lastSnapshot = RealtimeAccountSnapshot{
		TotalWalletBalance:    e.baselineWallet,
		AvailableBalance:      available,
		TotalUnrealizedProfit: totalUnrealized,
		TotalEquity:           e.baselineWallet + totalUnrealized,
		Positions:             positions,
		UpdatedAt:             time.Now().UTC(),
		Stale:                 stale,
	}
}

func (e *BinanceRealtimeAccountEngine) Snapshot(maxAge time.Duration) (*RealtimeAccountSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.hasBaseline || e.lastSnapshot.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("realtime account snapshot not ready")
	}
	age := time.Since(e.lastSnapshot.UpdatedAt)
	if maxAge <= 0 {
		maxAge = e.maxStaleAge
	}
	if age > maxAge {
		return nil, fmt.Errorf("realtime account snapshot stale (age=%s)", age.Truncate(time.Millisecond))
	}
	cp := e.lastSnapshot
	cp.Positions = append([]RealtimePosition(nil), e.lastSnapshot.Positions...)
	return &cp, nil
}
