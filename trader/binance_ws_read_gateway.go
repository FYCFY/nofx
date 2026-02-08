package trader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
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

	accountSF singleflight.Group
	metaSF    singleflight.Group
	priceSF   singleflight.Group
	depthSF   singleflight.Group

	metaMu     sync.RWMutex
	symbolMeta map[string]BinanceSymbolMeta

	priceMu    sync.RWMutex
	priceCache map[string]float64
	priceTime  map[string]time.Time

	depthMu    sync.RWMutex
	depthCache map[string]wsDepthSnapshot
	depthTime  map[string]time.Time
}

type wsDepthSnapshot struct {
	bids [][]float64
	asks [][]float64
}

func newBinanceWsReadGateway(futuresClient *futures.Client, spotClient *binance.Client, isTestnet bool) *binanceWsReadGateway {
	return &binanceWsReadGateway{
		futuresClient: futuresClient,
		spotClient:    spotClient,
		isTestnet:     isTestnet,
		symbolMeta:    make(map[string]BinanceSymbolMeta),
		priceCache:    make(map[string]float64),
		priceTime:     make(map[string]time.Time),
		depthCache:    make(map[string]wsDepthSnapshot),
		depthTime:     make(map[string]time.Time),
	}
}

func (g *binanceWsReadGateway) getFuturesAccountSnapshot(opts BinanceWsReadOptions) (*BinanceAccountSnapshot, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	v, err, _ := g.accountSF.Do("futures-account", func() (interface{}, error) {
		ch := make(chan struct {
			resp *futures.WsAccountV2InfoResponse
			err  error
		}, 1)
		go func() {
			resp, callErr := g.futuresClient.GetAccountInfoWs()
			ch <- struct {
				resp *futures.WsAccountV2InfoResponse
				err  error
			}{resp: resp, err: callErr}
		}()

		select {
		case out := <-ch:
			if out.err != nil {
				return nil, out.err
			}
			if out.resp == nil {
				return nil, fmt.Errorf("empty ws account response")
			}
			if out.resp.Error != nil {
				return nil, out.resp.Error
			}
			if out.resp.Status != 200 {
				return nil, fmt.Errorf("unexpected ws account status: %d", out.resp.Status)
			}
			return buildSnapshotFromWsAccountInfo(out.resp.Result), nil
		case <-time.After(timeout):
			return nil, fmt.Errorf("ws account snapshot timeout")
		}
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
	resp, err := g.callSpotSignedWS("account.status", params, timeout)
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
	key := "price-" + symbol
	v, err, _ := g.priceSF.Do(key, func() (interface{}, error) {
		eventCh := make(chan *futures.WsMarkPriceEvent, 1)
		errCh := make(chan error, 1)
		doneC, stopC, err := futures.WsMarkPriceServe(symbol, func(event *futures.WsMarkPriceEvent) {
			select {
			case eventCh <- event:
			default:
			}
		}, func(e error) {
			select {
			case errCh <- e:
			default:
			}
		})
		if err != nil {
			return nil, err
		}
		defer close(stopC)
		select {
		case evt := <-eventCh:
			if evt == nil {
				return nil, fmt.Errorf("empty mark price event")
			}
			price := parseFloatWS(evt.MarkPrice)
			if price <= 0 {
				return nil, fmt.Errorf("invalid mark price")
			}
			g.priceMu.Lock()
			g.priceCache[symbol] = price
			g.priceTime[symbol] = time.Now().UTC()
			g.priceMu.Unlock()
			return price, nil
		case e := <-errCh:
			return nil, e
		case <-doneC:
			return nil, fmt.Errorf("mark price stream closed")
		case <-time.After(timeout):
			return nil, fmt.Errorf("mark price timeout")
		}
	})
	if err != nil {
		g.priceMu.RLock()
		cached, ok := g.priceCache[symbol]
		ts := g.priceTime[symbol]
		g.priceMu.RUnlock()
		if ok && time.Since(ts) < 30*time.Second {
			return cached, nil
		}
		return 0, err
	}
	return v.(float64), nil
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
	v, err, _ := g.depthSF.Do(key, func() (interface{}, error) {
		eventCh := make(chan *futures.WsDepthEvent, 1)
		errCh := make(chan error, 1)
		doneC, stopC, err := futures.WsPartialDepthServe(symbol, depth, func(event *futures.WsDepthEvent) {
			select {
			case eventCh <- event:
			default:
			}
		}, func(e error) {
			select {
			case errCh <- e:
			default:
			}
		})
		if err != nil {
			return nil, err
		}
		defer close(stopC)

		select {
		case evt := <-eventCh:
			if evt == nil {
				return nil, fmt.Errorf("empty depth event")
			}
			bids := make([][]float64, 0, len(evt.Bids))
			for _, b := range evt.Bids {
				bids = append(bids, []float64{parseFloatWS(b.Price), parseFloatWS(b.Quantity)})
			}
			asks := make([][]float64, 0, len(evt.Asks))
			for _, a := range evt.Asks {
				asks = append(asks, []float64{parseFloatWS(a.Price), parseFloatWS(a.Quantity)})
			}
			if len(bids) > limit {
				bids = bids[:limit]
			}
			if len(asks) > limit {
				asks = asks[:limit]
			}
			g.depthMu.Lock()
			g.depthCache[key] = wsDepthSnapshot{bids: bids, asks: asks}
			g.depthTime[key] = time.Now().UTC()
			g.depthMu.Unlock()
			return wsDepthSnapshot{bids: bids, asks: asks}, nil
		case e := <-errCh:
			return nil, e
		case <-doneC:
			return nil, fmt.Errorf("depth stream closed")
		case <-time.After(timeout):
			return nil, fmt.Errorf("depth timeout")
		}
	})
	if err != nil {
		g.depthMu.RLock()
		cached, ok := g.depthCache[key]
		ts := g.depthTime[key]
		g.depthMu.RUnlock()
		if ok && time.Since(ts) < 10*time.Second {
			return cached.bids, cached.asks, nil
		}
		return nil, nil, err
	}
	depthSnap := v.(wsDepthSnapshot)
	return depthSnap.bids, depthSnap.asks, nil
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
		resp, callErr := g.callFuturesPublicWS("exchangeInfo", map[string]interface{}{"symbol": symbol}, timeout)
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
	svc, err := futures.NewOrderStatusWsService(g.futuresClient.APIKey, g.futuresClient.SecretKey)
	if err != nil {
		return nil, err
	}
	svc.TimeOffset = g.futuresClient.TimeOffset

	req := futures.NewOrderStatusWsRequest().Symbol(strings.ToUpper(symbol)).OrderID(orderID)
	ch := make(chan struct {
		resp *futures.QueryOrderWsResponse
		err  error
	}, 1)
	go func() {
		resp, callErr := svc.SyncDo(uuid.NewString(), req)
		ch <- struct {
			resp *futures.QueryOrderWsResponse
			err  error
		}{resp: resp, err: callErr}
	}()

	select {
	case out := <-ch:
		if out.err != nil {
			return nil, out.err
		}
		if out.resp == nil {
			return nil, fmt.Errorf("empty ws order status response")
		}
		if out.resp.Error != nil {
			return nil, out.resp.Error
		}
		if out.resp.Status != 200 {
			return nil, fmt.Errorf("unexpected ws order status: %d", out.resp.Status)
		}
		avgPrice, _ := strconv.ParseFloat(out.resp.Result.AvgPrice, 64)
		executedQty, _ := strconv.ParseFloat(out.resp.Result.ExecutedQty, 64)
		return map[string]interface{}{
			"orderId":     out.resp.Result.OrderID,
			"symbol":      out.resp.Result.Symbol,
			"status":      out.resp.Result.Status,
			"avgPrice":    avgPrice,
			"executedQty": executedQty,
			"side":        out.resp.Result.Side,
			"type":        out.resp.Result.Type,
			"time":        out.resp.Result.Time,
			"updateTime":  out.resp.Result.UpdateTime,
			"commission":  0.0,
		}, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("ws order status timeout")
	}
}

func (g *binanceWsReadGateway) callFuturesPublicWS(method string, params map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	endpoint := futures.BaseWsApiMainURL
	if g.isTestnet {
		endpoint = futures.BaseWsApiTestnetURL
	}
	return callWsAPI(endpoint, method, params, timeout)
}

func (g *binanceWsReadGateway) callSpotSignedWS(method string, params map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	if params == nil {
		params = make(map[string]interface{})
	}
	params["apiKey"] = g.spotClient.APIKey
	timestamp := time.Now().UnixMilli() - g.spotClient.TimeOffset
	params["timestamp"] = timestamp
	sig := signQuery(g.spotClient.SecretKey, params)
	params["signature"] = sig

	endpoint := binance.BaseWsApiMainURL
	if binance.UseTestnet {
		endpoint = binance.BaseWsApiTestnetURL
	}
	return callWsAPI(endpoint, method, params, timeout)
}

func callWsAPI(endpoint, method string, params map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))

	if params == nil {
		params = make(map[string]interface{})
	}

	req := map[string]interface{}{
		"id":     uuid.NewString(),
		"method": method,
		"params": params,
	}
	if err := conn.WriteJSON(req); err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	if err := conn.ReadJSON(&resp); err != nil {
		return nil, err
	}
	if errNode, ok := resp["error"].(map[string]interface{}); ok {
		code := fmt.Sprintf("%v", errNode["code"])
		msg := fmt.Sprintf("%v", errNode["msg"])
		return nil, fmt.Errorf("binance ws api error code=%s msg=%s", code, msg)
	}
	if status, ok := resp["status"].(float64); ok && int(status) >= 400 {
		return nil, fmt.Errorf("binance ws api status=%d", int(status))
	}
	return resp, nil
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
