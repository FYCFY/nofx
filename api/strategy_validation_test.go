package api

import (
	"testing"

	"nofx/store"
)

func TestValidateFundTransferConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *store.StrategyConfig
		wantErr bool
	}{
		{
			name:    "nil_config",
			config:  nil,
			wantErr: false,
		},
		{
			name:    "missing_fund_transfer",
			config:  &store.StrategyConfig{},
			wantErr: false,
		},
		{
			name: "valid_config",
			config: &store.StrategyConfig{
				FundTransfer: &store.FundTransferConfig{
					Enabled:                       true,
					Mode:                          store.FundTransferModeBidirectional,
					TriggerTime:                   "03:30",
					TargetFuturesAvailableBalance: 100,
					MinTransferAmount:             0.01,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid_mode",
			config: &store.StrategyConfig{
				FundTransfer: &store.FundTransferConfig{
					Enabled: true,
					Mode:    "bad_mode",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_time",
			config: &store.StrategyConfig{
				FundTransfer: &store.FundTransferConfig{
					Enabled:     true,
					Mode:        store.FundTransferModeFuturesToSpot,
					TriggerTime: "3:30",
				},
			},
			wantErr: true,
		},
		{
			name: "negative_target",
			config: &store.StrategyConfig{
				FundTransfer: &store.FundTransferConfig{
					Enabled:                       true,
					Mode:                          store.FundTransferModeFuturesToSpot,
					TriggerTime:                   "00:00",
					TargetFuturesAvailableBalance: -1,
				},
			},
			wantErr: true,
		},
		{
			name: "negative_min_transfer",
			config: &store.StrategyConfig{
				FundTransfer: &store.FundTransferConfig{
					Enabled:           true,
					Mode:              store.FundTransferModeFuturesToSpot,
					TriggerTime:       "00:00",
					MinTransferAmount: -0.01,
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFundTransferConfig(tc.config)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateFundTransferConfig error=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}
