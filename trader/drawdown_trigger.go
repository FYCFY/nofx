package trader

import (
	"math"
	"strings"
)

// DrawdownTriggerInput defines input parameters for drawdown close evaluation.
type DrawdownTriggerInput struct {
	Side                 string
	EntryPrice           float64
	TakeProfitPrice      float64
	MarkPrice            float64
	Leverage             int
	ProgressPctThreshold float64
	DrawdownPctThreshold float64
	Armed                bool
}

// DrawdownTriggerResult contains computed thresholds and trigger decision.
type DrawdownTriggerResult struct {
	Armed          bool
	ShouldClose    bool
	SkipReason     string
	CurrentPnLPct  float64
	TargetPnLPct   float64
	BasePnLPct     float64
	DrawdownAbsPct float64
	TriggerPnLPct  float64
}

func normalizeDrawdownThreshold(v float64) float64 {
	if v <= 0 {
		return 40
	}
	if v > 100 {
		return 100
	}
	return v
}

// EvaluateDrawdownTrigger applies base-threshold drawdown logic:
// arm when currentPnL >= basePnL, close when armed and currentPnL <= triggerPnL.
func EvaluateDrawdownTrigger(in DrawdownTriggerInput) DrawdownTriggerResult {
	out := DrawdownTriggerResult{Armed: in.Armed}

	if in.EntryPrice <= 0 || in.TakeProfitPrice <= 0 || in.MarkPrice <= 0 {
		out.SkipReason = "invalid price"
		return out
	}
	if in.Leverage <= 0 {
		out.SkipReason = "invalid leverage"
		return out
	}

	side := strings.ToLower(strings.TrimSpace(in.Side))
	switch side {
	case "long":
		if in.TakeProfitPrice <= in.EntryPrice {
			out.SkipReason = "invalid take profit for long"
			return out
		}
		out.CurrentPnLPct = ((in.MarkPrice - in.EntryPrice) / in.EntryPrice) * float64(in.Leverage) * 100
	case "short":
		if in.TakeProfitPrice >= in.EntryPrice {
			out.SkipReason = "invalid take profit for short"
			return out
		}
		out.CurrentPnLPct = ((in.EntryPrice - in.MarkPrice) / in.EntryPrice) * float64(in.Leverage) * 100
	default:
		out.SkipReason = "unknown side"
		return out
	}

	out.TargetPnLPct = math.Abs((in.TakeProfitPrice-in.EntryPrice)/in.EntryPrice) * float64(in.Leverage) * 100
	if out.TargetPnLPct <= 0 {
		out.SkipReason = "invalid target pnl"
		return out
	}

	progressThreshold := normalizeDrawdownThreshold(in.ProgressPctThreshold)
	drawdownThreshold := normalizeDrawdownThreshold(in.DrawdownPctThreshold)

	out.BasePnLPct = out.TargetPnLPct * progressThreshold / 100
	out.DrawdownAbsPct = out.BasePnLPct * drawdownThreshold / 100
	out.TriggerPnLPct = out.BasePnLPct - out.DrawdownAbsPct

	if !out.Armed && out.CurrentPnLPct >= out.BasePnLPct {
		out.Armed = true
	}
	if out.Armed && out.CurrentPnLPct <= out.TriggerPnLPct {
		out.ShouldClose = true
	}

	return out
}
