package backtest

import (
	"testing"

	"nofx/kernel"
	"nofx/market"
)

func testFeedWithKline(symbol string, k market.Kline) *DataFeed {
	return &DataFeed{
		primaryTF: "1m",
		symbolSeries: map[string]*symbolSeries{
			symbol: {
				byTF: map[string]*timeframeSeries{
					"1m": {
						klines:     []market.Kline{k},
						closeTimes: []int64{k.CloseTime},
					},
				},
			},
		},
	}
}

func TestProcessPendingLimitOrdersFill(t *testing.T) {
	symbol := "BTCUSDT"
	ts := int64(1700000000000)
	bar := market.Kline{
		OpenTime:  ts - 60000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		CloseTime: ts,
	}
	r := &Runner{
		feed: testFeedWithKline(symbol, bar),
		account: NewBacktestAccount(10000, 0, 0),
		pendingOrders: []PendingLimitOrder{
			{
				OrderID:  "o1",
				Symbol:   symbol,
				Side:     "long",
				Price:    100,
				Quantity: 1,
				Leverage: 10,
				CreatedAt: ts - 60000,
			},
		},
	}

	events, _, err := r.processPendingLimitOrders(ts, 1)
	if err != nil {
		t.Fatalf("processPendingLimitOrders error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if len(r.pendingOrders) != 0 {
		t.Fatalf("expected pending orders cleared")
	}
	if len(r.account.Positions()) != 1 {
		t.Fatalf("expected position opened")
	}
}

func TestPickStopTakeTriggerByOpenDistance(t *testing.T) {
	pos := &position{
		Symbol:    "BTCUSDT",
		Side:      "long",
		StopLoss:  95,
		TakeProfit: 108,
	}
	trigger, _ := pickStopTakeTrigger(pos, 100, 110, 90)
	if trigger != "stop_loss" {
		t.Fatalf("expected stop_loss to be closer to open, got %s", trigger)
	}
}

func TestUpdateStopLossAction(t *testing.T) {
	r := &Runner{
		account: NewBacktestAccount(10000, 0, 0),
		feed:    testFeedWithKline("BTCUSDT", market.Kline{OpenTime: 1, Open: 100, High: 105, Low: 95, Close: 100, CloseTime: 1}),
	}
	_, _, _, err := r.account.Open("BTCUSDT", "long", 1, 10, 100, 1)
	if err != nil {
		t.Fatalf("open position: %v", err)
	}
	dec := kernel.Decision{
		Symbol: "BTCUSDT",
		Action: "update_stop_loss",
		Price:  95,
	}
	_, _, _, execErr := r.executeDecision(dec, map[string]float64{"BTCUSDT": 100}, 1, 1)
	if execErr != nil {
		t.Fatalf("executeDecision error: %v", execErr)
	}
	sl, _, err := r.account.GetStopLossTakeProfit("BTCUSDT", "long")
	if err != nil {
		t.Fatalf("get stop loss: %v", err)
	}
	if sl != 95 {
		t.Fatalf("expected stop loss 95, got %.4f", sl)
	}
}
