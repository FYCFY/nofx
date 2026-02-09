package trader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"nofx/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"golang.org/x/sync/singleflight"
)

type BinancePositionSnapshot struct {
	Symbol           string
	PositionAmt      float64
	EntryPrice       float64
	MarkPrice        float64
	UnRealizedProfit float64
	Leverage         float64
	LiquidationPrice float64
	Side             string
}

type BinanceAccountSnapshot struct {
	TotalWalletBalance    float64
	AvailableBalance      float64
	TotalUnrealizedProfit float64
	Positions             []BinancePositionSnapshot
	UpdateTime            time.Time
}

type BinanceWsReadOptions struct {
	Timeout time.Duration
}

type BinanceSymbolMeta struct {
	MinNotional    float64
	QtyPrecision   int
	PricePrecision int
	UpdatedAt      time.Time
}

type binanceWsReadGateway struct {
	futuresClient *futures.Client
	spotClient    *binance.Client
	isTestnet     bool
	closed        chan struct{}

	futuresConnMgr wsConnManager
	spotConnMgr    wsConnManager

	accountSF singleflight.Group
	metaSF    singleflight.Group

	metaMu     sync.RWMutex
	symbolMeta map[string]BinanceSymbolMeta

	priceMu    sync.RWMutex
	priceCache map[string]float64
	priceTime  map[string]time.Time
	priceSubs  map[string]chan struct{}

	depthMu    sync.RWMutex
	depthCache map[string]wsDepthSnapshot
	depthTime  map[string]time.Time
	depthSubs  map[string]chan struct{}
}

type wsDepthSnapshot struct {
	bids [][]float64
	asks [][]float64
}

func newBinanceWsReadGateway(futuresClient *futures.Client, spotClient *binance.Client, isTestnet bool) *binanceWsReadGateway {
	futuresEndpoint := futures.BaseWsApiMainURL
	if isTestnet {
		futuresEndpoint = futures.BaseWsApiTestnetURL
	}

	spotEndpoint := binance.BaseWsApiMainURL
	if binance.UseTestnet {
		spotEndpoint = binance.BaseWsApiTestnetURL
	}

	g := &binanceWsReadGateway{
		futuresClient: futuresClient,
		spotClient:    spotClient,
		isTestnet:     isTestnet,
		closed:        make(chan struct{}),
		symbolMeta:    make(map[string]BinanceSymbolMeta),
		priceCache:    make(map[string]float64),
		priceTime:     make(map[string]time.Time),
		priceSubs:     make(map[string]chan struct{}),
		depthCache:    make(map[string]wsDepthSnapshot),
		depthTime:     make(map[string]time.Time),
		depthSubs:     make(map[string]chan struct{}),
	}
	g.futuresConnMgr = newWSAPIConnManager(
		"futures-wsapi",
		futuresEndpoint,
		futuresClient.APIKey,
		futuresClient.SecretKey,
		func() int64 { return futuresClient.TimeOffset },
	)
	g.spotConnMgr = newWSAPIConnManager(
		"spot-wsapi",
		spotEndpoint,
		spotClient.APIKey,
		spotClient.SecretKey,
		func() int64 { return spotClient.TimeOffset },
	)
	g.futuresConnMgr.Start()
	g.spotConnMgr.Start()
	return g
}

func (g *binanceWsReadGateway) waitReady(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return g.futuresConnMgr.WaitReady(timeout)
}

func (g *binanceWsReadGateway) getFuturesAccountSnapshot(opts BinanceWsReadOptions) (*BinanceAccountSnapshot, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	v, err, _ := g.accountSF.Do("futures-account", func() (interface{}, error) {
		resp, callErr := g.futuresConnMgr.Send("v2/account.status", nil, true, timeout)
		if callErr != nil {
			return nil, callErr
		}
		resultRaw, ok := resp["result"]
		if !ok {
			return nil, fmt.Errorf("futures ws account response missing result")
		}
		raw, marshalErr := json.Marshal(resultRaw)
		if marshalErr != nil {
			return nil, marshalErr
		}
		var info futures.AccountV3
		if unmarshalErr := json.Unmarshal(raw, &info); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return buildSnapshotFromWsAccountInfo(info), nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*BinanceAccountSnapshot), nil
}

func buildSnapshotFromWsAccountInfo(info futures.AccountV3) *BinanceAccountSnapshot {
	wallet := parseFloatWS(info.TotalWalletBalance)
	available := parseFloatWS(info.AvailableBalance)
	unrealized := parseFloatWS(info.TotalUnrealizedProfit)

	positions := make([]BinancePositionSnapshot, 0, len(info.Positions))
	for _, p := range info.Positions {
		if p == nil {
			continue
		}
		amt := parseFloatWS(p.PositionAmt)
		if amt == 0 {
			continue
		}
		side := "long"
		if amt < 0 {
			side = "short"
		}
		notional := parseFloatWS(p.Notional)
		initialMargin := parseFloatWS(p.InitialMargin)
		markPrice := 0.0
		if amt != 0 {
			markPrice = math.Abs(notional / amt)
		}
		leverage := 10.0
		if initialMargin > 0 {
			leverage = math.Abs(notional / initialMargin)
			if leverage <= 0 {
				leverage = 10
			}
		}
		positions = append(positions, BinancePositionSnapshot{
			Symbol:           p.Symbol,
			PositionAmt:      amt,
			EntryPrice:       0,
			MarkPrice:        markPrice,
			UnRealizedProfit: parseFloatWS(p.UnrealizedProfit),
			Leverage:         leverage,
			LiquidationPrice: 0,
			Side:             side,
		})
	}

	return &BinanceAccountSnapshot{
		TotalWalletBalance:    wallet,
		AvailableBalance:      available,
		TotalUnrealizedProfit: unrealized,
		Positions:             positions,
		UpdateTime:            time.Now().UTC(),
	}
}

func (g *binanceWsReadGateway) getSpotUSDTBalance(opts BinanceWsReadOptions) (float64, error) {
	if g.isTestnet {
		return 0, fmt.Errorf("spot balance not supported in Binance testnet mode")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	params := map[string]interface{}{}
	resp, err := g.spotConnMgr.Send("account.status", params, true, timeout)
	if err != nil {
		return 0, err
	}

	resultRaw, ok := resp["result"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected spot ws account response")
	}
	balancesRaw, ok := resultRaw["balances"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("spot ws account missing balances")
	}
	for _, row := range balancesRaw {
		m, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		asset := strings.ToUpper(fmt.Sprintf("%v", m["asset"]))
		if asset != "USDT" {
			continue
		}
		free := parseAnyFloat(m["free"])
		return free, nil
	}
	return 0, nil
}

func (g *binanceWsReadGateway) getMarketPrice(symbol string, timeout time.Duration) (float64, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	symbol = strings.ToUpper(symbol)
	if err := g.ensureMarkPriceStream(symbol); err != nil {
		return 0, err
	}

	deadline := time.Now().Add(timeout)
	for {
		g.priceMu.RLock()
		cached, ok := g.priceCache[symbol]
		ts := g.priceTime[symbol]
		g.priceMu.RUnlock()
		if ok && cached > 0 && time.Since(ts) < 30*time.Second {
			return cached, nil
		}
		if time.Now().After(deadline) {
			if ok && cached > 0 {
				return cached, nil
			}
			return 0, fmt.Errorf("mark price timeout")
		}
		time.Sleep(120 * time.Millisecond)
	}
}

func (g *binanceWsReadGateway) getOrderBook(symbol string, depth, limit int, timeout time.Duration) ([][]float64, [][]float64, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if depth <= 0 {
		depth = 20
	}
	if limit <= 0 {
		limit = depth
	}
	symbol = strings.ToUpper(symbol)
	key := fmt.Sprintf("depth-%s-%d", symbol, depth)
	if err := g.ensureDepthStream(symbol, depth); err != nil {
		return nil, nil, err
	}

	deadline := time.Now().Add(timeout)
	for {
		g.depthMu.RLock()
		cached, ok := g.depthCache[key]
		ts := g.depthTime[key]
		g.depthMu.RUnlock()
		if ok && time.Since(ts) < 10*time.Second {
			bids := cached.bids
			asks := cached.asks
			if len(bids) > limit {
				bids = bids[:limit]
			}
			if len(asks) > limit {
				asks = asks[:limit]
			}
			return bids, asks, nil
		}
		if time.Now().After(deadline) {
			if ok {
				return cached.bids, cached.asks, nil
			}
			return nil, nil, fmt.Errorf("depth timeout")
		}
		time.Sleep(120 * time.Millisecond)
	}
}

func (g *binanceWsReadGateway) getSymbolMeta(symbol string, timeout time.Duration) (BinanceSymbolMeta, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	symbol = strings.ToUpper(symbol)

	g.metaMu.RLock()
	if m, ok := g.symbolMeta[symbol]; ok && time.Since(m.UpdatedAt) < 12*time.Hour {
		g.metaMu.RUnlock()
		return m, nil
	}
	g.metaMu.RUnlock()

	v, err, _ := g.metaSF.Do("meta-"+symbol, func() (interface{}, error) {
		resp, callErr := g.futuresConnMgr.Send("exchangeInfo", map[string]interface{}{"symbol": symbol}, false, timeout)
		if callErr != nil {
			return nil, callErr
		}

		resultRaw, ok := resp["result"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected exchangeInfo response")
		}
		symbols, ok := resultRaw["symbols"].([]interface{})
		if !ok || len(symbols) == 0 {
			return nil, fmt.Errorf("exchangeInfo symbols missing")
		}
		sym, ok := symbols[0].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("exchangeInfo symbol format invalid")
		}
		filters, ok := sym["filters"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("exchangeInfo filters missing")
		}

		meta := BinanceSymbolMeta{
			MinNotional:    10,
			QtyPrecision:   3,
			PricePrecision: 2,
			UpdatedAt:      time.Now().UTC(),
		}
		for _, f := range filters {
			fm, ok := f.(map[string]interface{})
			if !ok {
				continue
			}
			ft := strings.ToUpper(fmt.Sprintf("%v", fm["filterType"]))
			switch ft {
			case "LOT_SIZE":
				meta.QtyPrecision = calculatePrecision(fmt.Sprintf("%v", fm["stepSize"]))
			case "PRICE_FILTER":
				meta.PricePrecision = calculatePrecision(fmt.Sprintf("%v", fm["tickSize"]))
			case "MIN_NOTIONAL":
				meta.MinNotional = parseAnyFloat(fm["notional"])
				if meta.MinNotional <= 0 {
					meta.MinNotional = parseAnyFloat(fm["minNotional"])
				}
			}
		}

		g.metaMu.Lock()
		g.symbolMeta[symbol] = meta
		g.metaMu.Unlock()
		return meta, nil
	})
	if err != nil {
		return BinanceSymbolMeta{}, err
	}
	return v.(BinanceSymbolMeta), nil
}

func (g *binanceWsReadGateway) getOrderStatus(symbol string, orderID int64, timeout time.Duration) (map[string]interface{}, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	resp, err := g.futuresConnMgr.Send("order.status", map[string]interface{}{
		"symbol":  strings.ToUpper(symbol),
		"orderId": orderID,
	}, true, timeout)
	if err != nil {
		return nil, err
	}
	resultRaw, ok := resp["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected ws order status response")
	}
	orderIDVal := int64(parseAnyFloat(resultRaw["orderId"]))
	if orderIDVal == 0 {
		orderIDVal = int64(parseAnyFloat(resultRaw["orderID"]))
	}
	symbolVal := fmt.Sprintf("%v", resultRaw["symbol"])
	if symbolVal == "" {
		symbolVal = strings.ToUpper(symbol)
	}
	return map[string]interface{}{
		"orderId":     orderIDVal,
		"symbol":      symbolVal,
		"status":      fmt.Sprintf("%v", resultRaw["status"]),
		"avgPrice":    parseAnyFloat(resultRaw["avgPrice"]),
		"executedQty": parseAnyFloat(resultRaw["executedQty"]),
		"side":        fmt.Sprintf("%v", resultRaw["side"]),
		"type":        fmt.Sprintf("%v", resultRaw["type"]),
		"time":        int64(parseAnyFloat(resultRaw["time"])),
		"updateTime":  int64(parseAnyFloat(resultRaw["updateTime"])),
		"commission":  0.0,
	}, nil
}

func (g *binanceWsReadGateway) ensureMarkPriceStream(symbol string) error {
	symbol = strings.ToUpper(symbol)
	g.priceMu.Lock()
	if _, ok := g.priceSubs[symbol]; ok {
		g.priceMu.Unlock()
		return nil
	}
	stop := make(chan struct{})
	g.priceSubs[symbol] = stop
	g.priceMu.Unlock()

	go g.runMarkPriceStreamLoop(symbol, stop)
	return nil
}

func (g *binanceWsReadGateway) runMarkPriceStreamLoop(symbol string, stop <-chan struct{}) {
	backoff := time.Second
	for {
		select {
		case <-stop:
			return
		case <-g.closed:
			return
		default:
		}

		doneC, stopC, err := futures.WsMarkPriceServe(symbol, func(event *futures.WsMarkPriceEvent) {
			if event == nil {
				return
			}
			price := parseFloatWS(event.MarkPrice)
			if price <= 0 {
				return
			}
			g.priceMu.Lock()
			g.priceCache[symbol] = price
			g.priceTime[symbol] = time.Now().UTC()
			g.priceMu.Unlock()
		}, func(err error) {
			if err != nil {
				logger.Infof("⚠️ Binance mark price stream error (%s): %v", symbol, err)
			}
		})
		if err != nil {
			logger.Infof("⚠️ Failed to start Binance mark price stream (%s): %v", symbol, err)
			select {
			case <-time.After(backoff):
			case <-stop:
				return
			case <-g.closed:
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		select {
		case <-doneC:
		case <-stop:
			close(stopC)
			<-doneC
			return
		case <-g.closed:
			close(stopC)
			<-doneC
			return
		}
		select {
		case <-time.After(backoff):
		case <-stop:
			return
		case <-g.closed:
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (g *binanceWsReadGateway) ensureDepthStream(symbol string, depth int) error {
	key := fmt.Sprintf("depth-%s-%d", strings.ToUpper(symbol), depth)
	g.depthMu.Lock()
	if _, ok := g.depthSubs[key]; ok {
		g.depthMu.Unlock()
		return nil
	}
	stop := make(chan struct{})
	g.depthSubs[key] = stop
	g.depthMu.Unlock()

	go g.runDepthStreamLoop(strings.ToUpper(symbol), depth, key, stop)
	return nil
}

func (g *binanceWsReadGateway) runDepthStreamLoop(symbol string, depth int, key string, stop <-chan struct{}) {
	backoff := time.Second
	for {
		select {
		case <-stop:
			return
		case <-g.closed:
			return
		default:
		}

		doneC, stopC, err := futures.WsPartialDepthServe(symbol, depth, func(event *futures.WsDepthEvent) {
			if event == nil {
				return
			}
			bids := make([][]float64, 0, len(event.Bids))
			for _, b := range event.Bids {
				bids = append(bids, []float64{parseFloatWS(b.Price), parseFloatWS(b.Quantity)})
			}
			asks := make([][]float64, 0, len(event.Asks))
			for _, a := range event.Asks {
				asks = append(asks, []float64{parseFloatWS(a.Price), parseFloatWS(a.Quantity)})
			}
			g.depthMu.Lock()
			g.depthCache[key] = wsDepthSnapshot{bids: bids, asks: asks}
			g.depthTime[key] = time.Now().UTC()
			g.depthMu.Unlock()
		}, func(err error) {
			if err != nil {
				logger.Infof("⚠️ Binance depth stream error (%s): %v", key, err)
			}
		})
		if err != nil {
			logger.Infof("⚠️ Failed to start Binance depth stream (%s): %v", key, err)
			select {
			case <-time.After(backoff):
			case <-stop:
				return
			case <-g.closed:
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		select {
		case <-doneC:
		case <-stop:
			close(stopC)
			<-doneC
			return
		case <-g.closed:
			close(stopC)
			<-doneC
			return
		}

		select {
		case <-time.After(backoff):
		case <-stop:
			return
		case <-g.closed:
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func signQuery(secret string, params map[string]interface{}) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		if k == "signature" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := url.Values{}
	for _, k := range keys {
		values.Set(k, fmt.Sprintf("%v", params[k]))
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(values.Encode()))
	return hex.EncodeToString(mac.Sum(nil))
}

func parseAnyFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		f, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
		return f
	}
}
