package backtest

import (
	"testing"

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
