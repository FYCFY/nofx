package market

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"nofx/logger"

	"github.com/adshao/go-binance/v2/futures"
)

const (
	defaultWSStaleAfter   = 20 * time.Second
	defaultWSMaxClosedBar = 512
)

// WSFeedSnapshot is a Binance WS snapshot for one symbol+timeframe.
type WSFeedSnapshot struct {
	Symbol         string
	Timeframe      string
	BarsClosed     []Kline
	LiveBar        *KlineBar
	MarkPrice      float64
	FundingRate    float64
	HasMarkPrice   bool
	HasFundingRate bool
	UpdatedAt      time.Time
	Stale          bool
}

// BinanceWSFeed exposes thread-safe Binance WS market snapshots.
type BinanceWSFeed interface {
	EnsureSubscriptions(symbols []string, timeframes []string) error
	GetSnapshot(symbol string, timeframe string) (*WSFeedSnapshot, bool)
}

type wsTimeframeState struct {
	closed    []Kline
	live      *KlineBar
	updatedAt time.Time
}

type wsMarkState struct {
	markPrice      float64
	fundingRate    float64
	hasMarkPrice   bool
	hasFundingRate bool
	updatedAt      time.Time
}

type binanceWSFeed struct {
	mu sync.RWMutex

	staleAfter   time.Duration
	maxClosedBar int

	timeframesBySymbol map[string]map[string]*wsTimeframeState
	markBySymbol       map[string]*wsMarkState

	symbolSet    map[string]struct{}
	timeframeSet map[string]struct{}

	klineDone chan struct{}
	klineStop chan struct{}
	markDone  chan struct{}
	markStop  chan struct{}

	reconnecting bool
}

func newBinanceWSFeed() *binanceWSFeed {
	return &binanceWSFeed{
		staleAfter:         defaultWSStaleAfter,
		maxClosedBar:       defaultWSMaxClosedBar,
		timeframesBySymbol: make(map[string]map[string]*wsTimeframeState),
		markBySymbol:       make(map[string]*wsMarkState),
		symbolSet:          make(map[string]struct{}),
		timeframeSet:       make(map[string]struct{}),
	}
}

func (f *binanceWSFeed) EnsureSubscriptions(symbols []string, timeframes []string) error {
	newSymbols := normalizeSymbols(symbols)
	newTFs := normalizeTimeframes(timeframes)
	if len(newSymbols) == 0 || len(newTFs) == 0 {
		return fmt.Errorf("binance ws subscription requires symbols and timeframes")
	}

	f.mu.Lock()
	changed := false
	for _, s := range newSymbols {
		if _, ok := f.symbolSet[s]; !ok {
			f.symbolSet[s] = struct{}{}
			changed = true
		}
	}
	for _, tf := range newTFs {
		if _, ok := f.timeframeSet[tf]; !ok {
			f.timeframeSet[tf] = struct{}{}
			changed = true
		}
	}

	// First initialization or set changed -> restart streams with full set.
	if f.klineStop == nil || f.markStop == nil {
		changed = true
	}
	if !changed {
		f.mu.Unlock()
		return nil
	}

	symbolList := setToSortedSlice(f.symbolSet)
	tfList := setToSortedSlice(f.timeframeSet)
	if err := f.restartStreamsLocked(symbolList, tfList); err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	return nil
}

func (f *binanceWSFeed) GetSnapshot(symbol string, timeframe string) (*WSFeedSnapshot, bool) {
	symbol = Normalize(symbol)
	timeframe = strings.TrimSpace(timeframe)

	f.mu.RLock()
	defer f.mu.RUnlock()

	var tfState *wsTimeframeState
	if byTF, ok := f.timeframesBySymbol[symbol]; ok {
		tfState = byTF[timeframe]
	}
	markState := f.markBySymbol[symbol]
	if tfState == nil && markState == nil {
		return nil, false
	}

	snap := &WSFeedSnapshot{Symbol: symbol, Timeframe: timeframe}
	if tfState != nil {
		snap.BarsClosed = append(snap.BarsClosed, tfState.closed...)
		if tfState.live != nil {
			live := *tfState.live
			snap.LiveBar = &live
		}
		snap.UpdatedAt = tfState.updatedAt
	}
	if markState != nil {
		snap.MarkPrice = markState.markPrice
		snap.FundingRate = markState.fundingRate
		snap.HasMarkPrice = markState.hasMarkPrice
		snap.HasFundingRate = markState.hasFundingRate
		if markState.updatedAt.After(snap.UpdatedAt) {
			snap.UpdatedAt = markState.updatedAt
		}
	}
	if !snap.UpdatedAt.IsZero() {
		snap.Stale = time.Since(snap.UpdatedAt) > f.staleAfter
	}
	return snap, true
}

func (f *binanceWSFeed) restartStreamsLocked(symbols []string, timeframes []string) error {
	f.stopStreamsLocked()

	klineSubs := make(map[string][]string, len(symbols))
	for _, s := range symbols {
		klineSubs[s] = append([]string(nil), timeframes...)
	}

	kDone, kStop, err := futures.WsCombinedKlineServeMultiInterval(klineSubs,
		func(event *futures.WsKlineEvent) {
			if event == nil {
				return
			}
			f.handleKlineEvent(event)
		},
		func(err error) {
			logger.Infof("⚠️ Binance WS kline stream error: %v", err)
			f.scheduleReconnect()
		},
	)
	if err != nil {
		return fmt.Errorf("start binance ws kline stream failed: %w", err)
	}

	markLevels := make(map[string]time.Duration, len(symbols))
	for _, s := range symbols {
		markLevels[s] = 1 * time.Second
	}
	mDone, mStop, err := futures.WsCombinedMarkPriceServeWithRate(markLevels,
		func(event *futures.WsMarkPriceEvent) {
			if event == nil {
				return
			}
			f.handleMarkPriceEvent(event)
		},
		func(err error) {
			logger.Infof("⚠️ Binance WS mark-price stream error: %v", err)
			f.scheduleReconnect()
		},
	)
	if err != nil {
		close(kStop)
		return fmt.Errorf("start binance ws mark-price stream failed: %w", err)
	}

	f.klineDone, f.klineStop = kDone, kStop
	f.markDone, f.markStop = mDone, mStop
	logger.Infof("📡 Binance WS market feed subscribed: symbols=%d, timeframes=%v", len(symbols), timeframes)
	return nil
}

func (f *binanceWSFeed) stopStreamsLocked() {
	if f.klineStop != nil {
		close(f.klineStop)
		f.klineStop = nil
		f.klineDone = nil
	}
	if f.markStop != nil {
		close(f.markStop)
		f.markStop = nil
		f.markDone = nil
	}
}

func (f *binanceWSFeed) scheduleReconnect() {
	f.mu.Lock()
	if f.reconnecting {
		f.mu.Unlock()
		return
	}
	f.reconnecting = true
	symbols := setToSortedSlice(f.symbolSet)
	timeframes := setToSortedSlice(f.timeframeSet)
	f.mu.Unlock()

	go func() {
		defer func() {
			f.mu.Lock()
			f.reconnecting = false
			f.mu.Unlock()
		}()
		if len(symbols) == 0 || len(timeframes) == 0 {
			return
		}

		backoff := 1 * time.Second
		for attempt := 1; attempt <= 5; attempt++ {
			time.Sleep(backoff)
			f.mu.Lock()
			err := f.restartStreamsLocked(symbols, timeframes)
			f.mu.Unlock()
			if err == nil {
				logger.Infof("✅ Binance WS market feed reconnected on attempt %d", attempt)
				return
			}
			logger.Infof("⚠️ Binance WS market feed reconnect attempt %d failed: %v", attempt, err)
			backoff *= 2
		}
		logger.Infof("❌ Binance WS market feed reconnect exhausted after 5 attempts")
	}()
}

func (f *binanceWSFeed) handleKlineEvent(event *futures.WsKlineEvent) {
	symbol := Normalize(event.Symbol)
	timeframe := event.Kline.Interval

	open, err1 := strconv.ParseFloat(event.Kline.Open, 64)
	high, err2 := strconv.ParseFloat(event.Kline.High, 64)
	low, err3 := strconv.ParseFloat(event.Kline.Low, 64)
	closePx, err4 := strconv.ParseFloat(event.Kline.Close, 64)
	volume, err5 := strconv.ParseFloat(event.Kline.Volume, 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		return
	}

	now := time.Now().UTC()
	liveBar := &KlineBar{
		Time:   event.Kline.StartTime,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closePx,
		Volume: volume,
	}

	f.mu.Lock()
	byTF, ok := f.timeframesBySymbol[symbol]
	if !ok {
		byTF = make(map[string]*wsTimeframeState)
		f.timeframesBySymbol[symbol] = byTF
	}
	state, ok := byTF[timeframe]
	if !ok {
		state = &wsTimeframeState{}
		byTF[timeframe] = state
	}
	state.updatedAt = now

	if event.Kline.IsFinal {
		state.live = nil
		closed := Kline{
			OpenTime:  event.Kline.StartTime,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePx,
			Volume:    volume,
			CloseTime: event.Kline.EndTime,
		}
		state.closed = mergeClosedBars(state.closed, closed, f.maxClosedBar)
	} else {
		state.live = liveBar
	}
	f.mu.Unlock()
}

func (f *binanceWSFeed) handleMarkPriceEvent(event *futures.WsMarkPriceEvent) {
	symbol := Normalize(event.Symbol)
	mark, err1 := strconv.ParseFloat(event.MarkPrice, 64)
	funding, err2 := strconv.ParseFloat(event.FundingRate, 64)
	if err1 != nil || err2 != nil {
		return
	}

	f.mu.Lock()
	state, ok := f.markBySymbol[symbol]
	if !ok {
		state = &wsMarkState{}
		f.markBySymbol[symbol] = state
	}
	state.markPrice = mark
	state.fundingRate = funding
	state.hasMarkPrice = true
	state.hasFundingRate = true
	state.updatedAt = time.Now().UTC()
	f.mu.Unlock()
}

func mergeClosedBars(existing []Kline, bar Kline, max int) []Kline {
	if len(existing) == 0 {
		return []Kline{bar}
	}
	lastIdx := len(existing) - 1
	if existing[lastIdx].OpenTime == bar.OpenTime {
		existing[lastIdx] = bar
		return existing
	}
	if existing[lastIdx].OpenTime > bar.OpenTime {
		for i := range existing {
			if existing[i].OpenTime == bar.OpenTime {
				existing[i] = bar
				return existing
			}
		}
		return existing
	}
	existing = append(existing, bar)
	if max > 0 && len(existing) > max {
		existing = append([]Kline(nil), existing[len(existing)-max:]...)
	}
	return existing
}

func normalizeSymbols(symbols []string) []string {
	set := make(map[string]struct{})
	for _, s := range symbols {
		n := Normalize(s)
		if n == "" {
			continue
		}
		set[n] = struct{}{}
	}
	return setToSortedSlice(set)
}

func normalizeTimeframes(timeframes []string) []string {
	set := make(map[string]struct{})
	for _, tf := range timeframes {
		tf = strings.TrimSpace(tf)
		if tf == "" {
			continue
		}
		set[tf] = struct{}{}
	}
	return setToSortedSlice(set)
}

func setToSortedSlice(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var globalBinanceWSFeed BinanceWSFeed = newBinanceWSFeed()

// EnsureBinanceWSSubscriptions ensures Binance WS market subscriptions are active.
func EnsureBinanceWSSubscriptions(symbols []string, timeframes []string) error {
	return globalBinanceWSFeed.EnsureSubscriptions(symbols, timeframes)
}
