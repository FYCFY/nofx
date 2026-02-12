package trader

import "testing"

func almostEqual(a, b float64) bool {
	const eps = 1e-9
	if a > b {
		return a-b < eps
	}
	return b-a < eps
}

func TestEvaluateDrawdownTrigger_BaseFormula(t *testing.T) {
	res := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "long",
		EntryPrice:           100,
		TakeProfitPrice:      110,
		MarkPrice:            104,
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
		Armed:                false,
	})
	if res.SkipReason != "" {
		t.Fatalf("unexpected skip: %s", res.SkipReason)
	}
	if !almostEqual(res.TargetPnLPct, 100) {
		t.Fatalf("target pnl mismatch: got %.8f", res.TargetPnLPct)
	}
	if !almostEqual(res.BasePnLPct, 40) {
		t.Fatalf("base pnl mismatch: got %.8f", res.BasePnLPct)
	}
	if !almostEqual(res.DrawdownAbsPct, 16) {
		t.Fatalf("drawdown abs mismatch: got %.8f", res.DrawdownAbsPct)
	}
	if !almostEqual(res.TriggerPnLPct, 24) {
		t.Fatalf("trigger pnl mismatch: got %.8f", res.TriggerPnLPct)
	}
}

func TestEvaluateDrawdownTrigger_ArmedStateSequence(t *testing.T) {
	step1 := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "long",
		EntryPrice:           100,
		TakeProfitPrice:      110,
		MarkPrice:            104.1, // 41%
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
		Armed:                false,
	})
	if !step1.Armed || step1.ShouldClose {
		t.Fatalf("step1 expected armed=true close=false, got armed=%t close=%t", step1.Armed, step1.ShouldClose)
	}

	step2 := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "long",
		EntryPrice:           100,
		TakeProfitPrice:      110,
		MarkPrice:            103, // 30%
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
		Armed:                step1.Armed,
	})
	if !step2.Armed || step2.ShouldClose {
		t.Fatalf("step2 expected armed=true close=false, got armed=%t close=%t", step2.Armed, step2.ShouldClose)
	}

	step3 := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "long",
		EntryPrice:           100,
		TakeProfitPrice:      110,
		MarkPrice:            102.4, // 24%
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
		Armed:                step2.Armed,
	})
	if !step3.Armed || !step3.ShouldClose {
		t.Fatalf("step3 expected armed=true close=true, got armed=%t close=%t", step3.Armed, step3.ShouldClose)
	}
}

func TestEvaluateDrawdownTrigger_ShortMirror(t *testing.T) {
	res := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "short",
		EntryPrice:           100,
		TakeProfitPrice:      90,
		MarkPrice:            97.6, // 24%
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
		Armed:                true,
	})
	if res.SkipReason != "" {
		t.Fatalf("unexpected skip: %s", res.SkipReason)
	}
	if !almostEqual(res.TriggerPnLPct, 24) {
		t.Fatalf("expected trigger=24, got %.8f", res.TriggerPnLPct)
	}
	if !res.ShouldClose {
		t.Fatalf("expected close=true for short mirror scenario")
	}
}

func TestEvaluateDrawdownTrigger_InvalidTakeProfit(t *testing.T) {
	longRes := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "long",
		EntryPrice:           100,
		TakeProfitPrice:      99,
		MarkPrice:            104,
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
	})
	if longRes.SkipReason == "" {
		t.Fatalf("expected invalid take profit skip for long")
	}

	shortRes := EvaluateDrawdownTrigger(DrawdownTriggerInput{
		Side:                 "short",
		EntryPrice:           100,
		TakeProfitPrice:      101,
		MarkPrice:            96,
		Leverage:             10,
		ProgressPctThreshold: 40,
		DrawdownPctThreshold: 40,
	})
	if shortRes.SkipReason == "" {
		t.Fatalf("expected invalid take profit skip for short")
	}
}
