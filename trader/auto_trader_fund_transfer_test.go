package trader

import (
	"testing"
	"time"

	"nofx/store"
)

func TestParseTriggerTimeHHMM(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: "03:30", wantErr: false},
		{name: "bad_format", input: "3:30", wantErr: true},
		{name: "bad_hour", input: "24:00", wantErr: true},
		{name: "bad_minute", input: "12:60", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseTriggerTimeHHMM(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseTriggerTimeHHMM(%q) error=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestNextBeijingTriggerTime(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	tests := []struct {
		name    string
		now     time.Time
		trigger string
		want    time.Time
	}{
		{
			name:    "same_day_future",
			now:     time.Date(2026, 2, 6, 10, 0, 0, 0, loc),
			trigger: "12:30",
			want:    time.Date(2026, 2, 6, 12, 30, 0, 0, loc),
		},
		{
			name:    "equal_moves_next_day",
			now:     time.Date(2026, 2, 6, 12, 30, 0, 0, loc),
			trigger: "12:30",
			want:    time.Date(2026, 2, 7, 12, 30, 0, 0, loc),
		},
		{
			name:    "past_moves_next_day",
			now:     time.Date(2026, 2, 6, 15, 0, 0, 0, loc),
			trigger: "12:30",
			want:    time.Date(2026, 2, 7, 12, 30, 0, 0, loc),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nextBeijingTriggerTime(tc.now, loc, tc.trigger)
			if err != nil {
				t.Fatalf("nextBeijingTriggerTime returned error: %v", err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("nextBeijingTriggerTime got=%s want=%s", got.Format(time.RFC3339), tc.want.Format(time.RFC3339))
			}
		})
	}
}

func TestDecideFundTransfer(t *testing.T) {
	tests := []struct {
		name          string
		futuresAvail  float64
		target        float64
		minTransfer   float64
		mode          string
		wantDirection string
		wantAmount    float64
	}{
		{
			name:          "futures_to_spot_when_excess",
			futuresAvail:  120,
			target:        100,
			minTransfer:   0.01,
			mode:          store.FundTransferModeFuturesToSpot,
			wantDirection: "futures_to_spot",
			wantAmount:    20,
		},
		{
			name:          "no_refill_in_futures_to_spot_mode",
			futuresAvail:  80,
			target:        100,
			minTransfer:   0.01,
			mode:          store.FundTransferModeFuturesToSpot,
			wantDirection: "",
			wantAmount:    0,
		},
		{
			name:          "refill_in_bidirectional_mode",
			futuresAvail:  80,
			target:        100,
			minTransfer:   0.01,
			mode:          store.FundTransferModeBidirectional,
			wantDirection: "spot_to_futures",
			wantAmount:    20,
		},
		{
			name:          "skip_when_within_threshold",
			futuresAvail:  100.005,
			target:        100,
			minTransfer:   0.01,
			mode:          store.FundTransferModeBidirectional,
			wantDirection: "",
			wantAmount:    0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decideFundTransfer(tc.futuresAvail, tc.target, tc.minTransfer, tc.mode)
			if got.direction != tc.wantDirection {
				t.Fatalf("direction=%s want=%s", got.direction, tc.wantDirection)
			}
			if got.amount != tc.wantAmount {
				t.Fatalf("amount=%.6f want=%.6f", got.amount, tc.wantAmount)
			}
		})
	}
}
