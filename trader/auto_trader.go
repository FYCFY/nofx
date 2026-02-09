package trader

import (
	"encoding/json"
	"fmt"
	"math"
	"nofx/ai"
	"nofx/experience"
	"nofx/kernel"
	"nofx/logger"
	"nofx/market"
	"nofx/mcp"
	"nofx/notify"
	"nofx/store"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// AutoTraderConfig auto trading configuration (simplified version - AI makes all decisions)
type AutoTraderConfig struct {
	// Trader identification
	ID         string // Trader unique identifier (for log directory, etc.)
	Name       string // Trader display name
	AIModel    string // AI model provider key (e.g. deepseek/qwen/openai/claude/minmax)
	AIModelID  string // AI model config ID
	AIAuthMode string // AI auth mode (e.g., api_key, codex_oauth)

	// Trading platform selection
	Exchange   string // Exchange type: "binance", "bybit", "okx", "bitget", "hyperliquid", "aster" or "lighter"
	ExchangeID string // Exchange account UUID (for multi-account support)

	// Binance API configuration
	BinanceAPIKey    string
	BinanceSecretKey string
	BinanceTestnet   bool

	// Bybit API configuration
	BybitAPIKey    string
	BybitSecretKey string

	// OKX API configuration
	OKXAPIKey     string
	OKXSecretKey  string
	OKXPassphrase string

	// Bitget API configuration
	BitgetAPIKey     string
	BitgetSecretKey  string
	BitgetPassphrase string

	// Hyperliquid configuration
	HyperliquidPrivateKey string
	HyperliquidWalletAddr string
	HyperliquidTestnet    bool

	// Aster configuration
	AsterUser       string // Aster main wallet address
	AsterSigner     string // Aster API wallet address
	AsterPrivateKey string // Aster API wallet private key

	// LIGHTER configuration
	LighterWalletAddr       string // LIGHTER wallet address (L1 wallet)
	LighterPrivateKey       string // LIGHTER L1 private key (for account identification)
	LighterAPIKeyPrivateKey string // LIGHTER API Key private key (40 bytes, for transaction signing)
	LighterAPIKeyIndex      int    // LIGHTER API Key index (0-255)
	LighterTestnet          bool   // Whether to use testnet

	// AI configuration
	UseQwen     bool
	DeepSeekKey string
	QwenKey     string

	// Custom AI API configuration
	CustomAPIURL    string
	CustomAPIKey    string
	CustomModelName string

	// Scan configuration
	ScanInterval time.Duration // Scan interval (recommended 3 minutes)

	// Account configuration
	InitialBalance float64 // Initial balance (for P&L calculation, must be set manually)

	// Risk control (only as hints, AI can make autonomous decisions)
	MaxDailyLoss    float64       // Maximum daily loss percentage (hint)
	MaxDrawdown     float64       // Maximum drawdown percentage (hint)
	StopTradingTime time.Duration // Pause duration after risk control triggers

	// Position mode
	IsCrossMargin bool // true=cross margin mode, false=isolated margin mode

	// Competition visibility
	ShowInCompetition bool // Whether to show in competition page

	// Strategy configuration (use complete strategy config)
	StrategyConfig *store.StrategyConfig // Strategy configuration (includes coin sources, indicators, risk control, prompts, etc.)
}

type pendingLimitOrder struct {
	Key        string
	OrderID    string
	ClientID   string
	Symbol     string
	Side       string
	Price      float64
	Quantity   float64
	Leverage   int
	StopLoss   float64
	TakeProfit float64
	PostOnly   bool
	ReduceOnly bool
	CreatedAt  time.Time
}

// AutoTrader automatic trader
type AutoTrader struct {
	id                    string // Trader unique identifier
	name                  string // Trader display name
	aiModel               string // AI model name
	exchange              string // Trading platform type (binance/bybit/etc)
	exchangeID            string // Exchange account UUID
	showInCompetition     bool   // Whether to show in competition page
	config                AutoTraderConfig
	trader                Trader // Use Trader interface (supports multiple platforms)
	mcpClient             mcp.AIClient
	store                 *store.Store           // Data storage (decision records, etc.)
	strategyEngine        *kernel.StrategyEngine // Strategy engine (uses strategy configuration)
	cycleNumber           int                    // Current cycle number
	initialBalance        float64
	dailyPnL              float64
	customPrompt          string // Custom trading strategy prompt
	overrideBasePrompt    bool   // Whether to override base prompt
	lastResetTime         time.Time
	stopUntil             time.Time
	isRunning             bool
	isRunningMutex        sync.RWMutex       // Mutex to protect isRunning flag
	startTime             time.Time          // System start time
	callCount             int                // AI call count
	positionFirstSeenTime map[string]int64   // Position first seen time (symbol_side -> timestamp in milliseconds)
	stopMonitorCh         chan struct{}      // Used to stop monitoring goroutine
	monitorWg             sync.WaitGroup     // Used to wait for monitoring goroutine to finish
	peakPnLCache          map[string]float64 // Peak profit cache (symbol -> peak P&L percentage)
	peakPnLCacheMutex     sync.RWMutex       // Cache read-write lock
	lastBalanceSyncTime   time.Time          // Last balance sync time
	userID                string             // User ID
	gridState             *GridState         // Grid trading state (only used when StrategyType == "grid_trading")
	pendingLimitOrders    map[string]*pendingLimitOrder
	pendingLimitOrdersMu  sync.RWMutex
	pendingLimitSyncing   atomic.Bool
}

var futuresBalanceGuardStarted sync.Map

// NewAutoTrader creates an automatic trader
// st parameter is used to store decision records to database
func NewAutoTrader(config AutoTraderConfig, st *store.Store, userID string) (*AutoTrader, error) {
	// Set default values
	if config.ID == "" {
		config.ID = "default_trader"
	}
	if config.Name == "" {
		config.Name = "Default Trader"
	}
	if config.AIModel == "" {
		if config.UseQwen {
			config.AIModel = "qwen"
		} else {
			config.AIModel = "deepseek"
		}
	}

	// Initialize AI client based on provider
	var mcpClient mcp.AIClient
	aiModel := config.AIModel
	if config.UseQwen && aiModel == "" {
		aiModel = "qwen"
	}

	switch aiModel {
	case "claude":
		mcpClient = mcp.NewClaudeClient()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using Claude AI", config.Name)

	case "kimi":
		mcpClient = mcp.NewKimiClient()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using Kimi (Moonshot) AI", config.Name)

	case "gemini":
		mcpClient = mcp.NewGeminiClient()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using Google Gemini AI", config.Name)

	case "grok":
		mcpClient = mcp.NewGrokClient()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using xAI Grok AI", config.Name)

	case "minmax":
		mcpClient = mcp.NewMinmaxClient()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using MinMax AI", config.Name)

	case "openai":
		if config.AIAuthMode == "codex_oauth" {
			mcpClient = mcp.NewOpenAICodexClient()
			if concrete, ok := mcpClient.(*mcp.OpenAICodexClient); ok {
				concrete.SetTokenProvider(ai.OpenAICodexTokenProvider(st, userID, config.AIModelID))
				concrete.SetAPIKey(config.CustomAPIKey, "", config.CustomModelName)
			}
			logger.Infof("🤖 [%s] Using OpenAI Codex (OAuth)", config.Name)
		} else {
			mcpClient = mcp.NewOpenAIClient()
			mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
			logger.Infof("🤖 [%s] Using OpenAI", config.Name)
		}

	case "qwen":
		mcpClient = mcp.NewQwenClient()
		apiKey := config.QwenKey
		if apiKey == "" {
			apiKey = config.CustomAPIKey
		}
		mcpClient.SetAPIKey(apiKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using Alibaba Cloud Qwen AI", config.Name)

	case "custom":
		mcpClient = mcp.New()
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using custom AI API: %s (model: %s)", config.Name, config.CustomAPIURL, config.CustomModelName)

	default: // deepseek or empty
		mcpClient = mcp.NewDeepSeekClient()
		apiKey := config.DeepSeekKey
		if apiKey == "" {
			apiKey = config.CustomAPIKey
		}
		mcpClient.SetAPIKey(apiKey, config.CustomAPIURL, config.CustomModelName)
		logger.Infof("🤖 [%s] Using DeepSeek AI", config.Name)
	}

	if config.CustomAPIURL != "" || config.CustomModelName != "" {
		logger.Infof("🔧 [%s] Custom config - URL: %s, Model: %s", config.Name, config.CustomAPIURL, config.CustomModelName)
	}

	// Set default trading platform
	if config.Exchange == "" {
		config.Exchange = "binance"
	}

	// Create corresponding trader based on configuration
	var trader Trader
	var err error

	// Record position mode (general)
	marginModeStr := "Cross Margin"
	if !config.IsCrossMargin {
		marginModeStr = "Isolated Margin"
	}
	logger.Infof("📊 [%s] Position mode: %s", config.Name, marginModeStr)

	switch config.Exchange {
	case "binance":
		logger.Infof("🏦 [%s] Using Binance Futures trading", config.Name)
		trader = NewFuturesTrader(config.BinanceAPIKey, config.BinanceSecretKey, userID, config.BinanceTestnet)
	case "bybit":
		logger.Infof("🏦 [%s] Using Bybit Futures trading", config.Name)
		trader = NewBybitTrader(config.BybitAPIKey, config.BybitSecretKey)
	case "okx":
		logger.Infof("🏦 [%s] Using OKX Futures trading", config.Name)
		trader = NewOKXTrader(config.OKXAPIKey, config.OKXSecretKey, config.OKXPassphrase)
	case "bitget":
		logger.Infof("🏦 [%s] Using Bitget Futures trading", config.Name)
		trader = NewBitgetTrader(config.BitgetAPIKey, config.BitgetSecretKey, config.BitgetPassphrase)
	case "hyperliquid":
		logger.Infof("🏦 [%s] Using Hyperliquid trading", config.Name)
		trader, err = NewHyperliquidTrader(config.HyperliquidPrivateKey, config.HyperliquidWalletAddr, config.HyperliquidTestnet)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Hyperliquid trader: %w", err)
		}
	case "aster":
		logger.Infof("🏦 [%s] Using Aster trading", config.Name)
		trader, err = NewAsterTrader(config.AsterUser, config.AsterSigner, config.AsterPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Aster trader: %w", err)
		}
	case "lighter":
		logger.Infof("🏦 [%s] Using LIGHTER trading", config.Name)

		if config.LighterWalletAddr == "" || config.LighterAPIKeyPrivateKey == "" {
			return nil, fmt.Errorf("Lighter requires wallet address and API Key private key")
		}

		// Lighter only supports mainnet (testnet disabled)
		trader, err = NewLighterTraderV2(
			config.LighterWalletAddr,
			config.LighterAPIKeyPrivateKey,
			config.LighterAPIKeyIndex,
			false, // Always use mainnet for Lighter
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize LIGHTER trader: %w", err)
		}
		logger.Infof("✓ LIGHTER trader initialized successfully")
	default:
		return nil, fmt.Errorf("unsupported trading platform: %s", config.Exchange)
	}

	if config.Exchange == "binance" && st != nil {
		if binanceTrader, ok := trader.(*FuturesTrader); ok {
			openPositions, posErr := st.Position().GetOpenPositions(config.ID)
			if posErr != nil {
				logger.Infof("⚠️ [%s] Failed to seed Binance leverage from local positions: %v", config.Name, posErr)
			} else {
				seed := make(map[string]int)
				for _, p := range openPositions {
					if p == nil || p.Leverage <= 0 || p.Symbol == "" {
						continue
					}
					// Ignore sync-rebuilt positions with placeholder leverage (often 1x).
					if strings.EqualFold(p.Source, "sync") {
						continue
					}
					// For Binance futures, 1x from local seed is usually placeholder/noise.
					// Keep leverage truth from ws config/api set events instead.
					if p.Leverage <= 1 {
						continue
					}
					seed[p.Symbol] = p.Leverage
				}
				binanceTrader.SeedSymbolLeverage(seed)
				if len(seed) > 0 {
					logger.Infof("✓ [%s] Seeded %d Binance leverage values from local open positions", config.Name, len(seed))
				}
			}
		}
	}

	// Validate initial balance configuration, auto-fetch from exchange if 0
	if config.InitialBalance <= 0 {
		logger.Infof("📊 [%s] Initial balance not set, attempting to fetch current balance from exchange...", config.Name)
		account, err := trader.GetBalance()
		if err != nil {
			return nil, fmt.Errorf("initial balance not set and unable to fetch balance from exchange: %w", err)
		}
		// Try multiple balance field names (different exchanges return different formats)
		balanceKeys := []string{"total_equity", "totalWalletBalance", "wallet_balance", "totalEq", "balance"}
		var foundBalance float64
		for _, key := range balanceKeys {
			if balance, ok := account[key].(float64); ok && balance > 0 {
				foundBalance = balance
				break
			}
		}
		if foundBalance > 0 {
			config.InitialBalance = foundBalance
			logger.Infof("✓ [%s] Auto-fetched initial balance: %.2f USDT", config.Name, foundBalance)
			// Save to database so it persists across restarts
			if st != nil {
				if err := st.Trader().UpdateInitialBalance(userID, config.ID, foundBalance); err != nil {
					logger.Infof("⚠️  [%s] Failed to save initial balance to database: %v", config.Name, err)
				} else {
					logger.Infof("✓ [%s] Initial balance saved to database", config.Name)
				}
			}
		} else {
			return nil, fmt.Errorf("initial balance must be greater than 0, please set InitialBalance in config or ensure exchange account has balance")
		}
	}

	// Get last cycle number (for recovery)
	var cycleNumber int
	if st != nil {
		cycleNumber, _ = st.Decision().GetLastCycleNumber(config.ID)
		logger.Infof("📊 [%s] Decision records will be stored to database", config.Name)
	}

	// Create strategy engine (must have strategy config)
	if config.StrategyConfig == nil {
		return nil, fmt.Errorf("[%s] strategy not configured", config.Name)
	}
	strategyEngine := kernel.NewStrategyEngine(config.StrategyConfig)
	logger.Infof("✓ [%s] Using strategy engine (strategy configuration loaded)", config.Name)

	return &AutoTrader{
		id:                    config.ID,
		name:                  config.Name,
		aiModel:               config.AIModel,
		exchange:              config.Exchange,
		exchangeID:            config.ExchangeID,
		showInCompetition:     config.ShowInCompetition,
		config:                config,
		trader:                trader,
		mcpClient:             mcpClient,
		store:                 st,
		strategyEngine:        strategyEngine,
		cycleNumber:           cycleNumber,
		initialBalance:        config.InitialBalance,
		lastResetTime:         time.Now(),
		startTime:             time.Now(),
		callCount:             0,
		isRunning:             false,
		positionFirstSeenTime: make(map[string]int64),
		stopMonitorCh:         make(chan struct{}),
		monitorWg:             sync.WaitGroup{},
		peakPnLCache:          make(map[string]float64),
		peakPnLCacheMutex:     sync.RWMutex{},
		lastBalanceSyncTime:   time.Now(),
		userID:                userID,
		pendingLimitOrders:    make(map[string]*pendingLimitOrder),
		pendingLimitOrdersMu:  sync.RWMutex{},
	}, nil
}

// Run runs the automatic trading main loop
func (at *AutoTrader) Run() error {
	at.isRunningMutex.Lock()
	if at.isRunning {
		at.isRunningMutex.Unlock()
		return fmt.Errorf("trader already running")
	}
	at.isRunning = true
	at.isRunningMutex.Unlock()

	at.stopMonitorCh = make(chan struct{})
	at.startTime = time.Now()

	logger.Info("🚀 AI-driven automatic trading system started")
	logger.Infof("💰 Initial balance: %.2f USDT", at.initialBalance)
	logger.Infof("⚙️  Scan interval: %v", at.config.ScanInterval)
	logger.Info("🤖 AI will make full decisions on leverage, position size, stop loss/take profit, etc.")
	at.monitorWg.Add(1)
	defer at.monitorWg.Done()

	// Start drawdown monitoring
	at.startDrawdownMonitor()
	at.startPendingLimitOrderMonitor(5 * time.Second)
	at.startFuturesBalanceGuard()

	// Start Lighter order sync if using Lighter exchange
	if at.exchange == "lighter" {
		if lighterTrader, ok := at.trader.(*LighterTraderV2); ok && at.store != nil {
			lighterTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Lighter order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start Hyperliquid order sync if using Hyperliquid exchange
	if at.exchange == "hyperliquid" {
		if hyperliquidTrader, ok := at.trader.(*HyperliquidTrader); ok && at.store != nil {
			hyperliquidTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Hyperliquid order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start Bybit order sync if using Bybit exchange
	if at.exchange == "bybit" {
		if bybitTrader, ok := at.trader.(*BybitTrader); ok && at.store != nil {
			bybitTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Bybit order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start OKX order sync if using OKX exchange
	if at.exchange == "okx" {
		if okxTrader, ok := at.trader.(*OKXTrader); ok && at.store != nil {
			okxTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] OKX order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start Bitget order sync if using Bitget exchange
	if at.exchange == "bitget" {
		if bitgetTrader, ok := at.trader.(*BitgetTrader); ok && at.store != nil {
			bitgetTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Bitget order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start Aster order sync if using Aster exchange
	if at.exchange == "aster" {
		if asterTrader, ok := at.trader.(*AsterTrader); ok && at.store != nil {
			asterTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Aster order+position sync enabled (every 30s)", at.name)
		}
	}

	// Start Binance order sync if using Binance exchange
	if at.exchange == "binance" {
		if binanceTrader, ok := at.trader.(*FuturesTrader); ok && at.store != nil {
			binanceTrader.StartOrderSync(at.id, at.exchangeID, at.exchange, at.store, 30*time.Second)
			logger.Infof("🔄 [%s] Binance order+position sync enabled (every 30s)", at.name)
		}
	}

	ticker := time.NewTicker(at.config.ScanInterval)
	defer ticker.Stop()

	// Check if this is a grid trading strategy
	isGridStrategy := at.IsGridStrategy()
	if isGridStrategy {
		logger.Infof("🔲 [%s] Grid trading strategy detected, initializing grid...", at.name)
		if err := at.InitializeGrid(); err != nil {
			logger.Errorf("❌ [%s] Failed to initialize grid: %v", at.name, err)
			return fmt.Errorf("grid initialization failed: %w", err)
		}
	}

	// Execute immediately on first run
	if isGridStrategy {
		if err := at.RunGridCycle(); err != nil {
			logger.Infof("❌ Grid execution failed: %v", err)
		}
	} else {
		if err := at.runCycle(); err != nil {
			logger.Infof("❌ Execution failed: %v", err)
		}
	}

	for {
		at.isRunningMutex.RLock()
		running := at.isRunning
		at.isRunningMutex.RUnlock()

		if !running {
			break
		}

		select {
		case <-ticker.C:
			if isGridStrategy {
				if err := at.RunGridCycle(); err != nil {
					logger.Infof("❌ Grid execution failed: %v", err)
				}
			} else {
				if err := at.runCycle(); err != nil {
					logger.Infof("❌ Execution failed: %v", err)
				}
			}
		case <-at.stopMonitorCh:
			logger.Infof("[%s] ⏹ Stop signal received, exiting automatic trading main loop", at.name)
			return nil
		}
	}

	return nil
}

// Stop stops the automatic trading
func (at *AutoTrader) Stop() {
	at.isRunningMutex.Lock()
	if !at.isRunning {
		at.isRunningMutex.Unlock()
		return
	}
	at.isRunning = false
	at.isRunningMutex.Unlock()

	close(at.stopMonitorCh) // Notify monitoring goroutine to stop
	at.monitorWg.Wait()     // Wait for monitoring goroutine to finish
	logger.Info("⏹ Automatic trading system stopped")
}

// runCycle runs one trading cycle (using AI full decision-making)
func (at *AutoTrader) runCycle() error {
	at.callCount++

	logger.Info("\n" + strings.Repeat("=", 70) + "\n")
	logger.Infof("⏰ %s - AI decision cycle #%d", time.Now().Format("2006-01-02 15:04:05"), at.callCount)
	logger.Info(strings.Repeat("=", 70))

	// 0. Check if trader is stopped (early exit to prevent trades after Stop() is called)
	at.isRunningMutex.RLock()
	running := at.isRunning
	at.isRunningMutex.RUnlock()
	if !running {
		logger.Infof("⏹ Trader is stopped, aborting cycle #%d", at.callCount)
		return nil
	}

	// Sync pending limit orders (set SL/TP after fill, clean up canceled orders)
	at.syncPendingLimitOrders()

	// Create decision record
	record := &store.DecisionRecord{
		ExecutionLog: []string{},
		Success:      true,
	}

	// 1. Check if trading needs to be stopped
	if time.Now().Before(at.stopUntil) {
		remaining := at.stopUntil.Sub(time.Now())
		logger.Infof("⏸ Risk control: Trading paused, remaining %.0f minutes", remaining.Minutes())
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("Risk control paused, remaining %.0f minutes", remaining.Minutes())
		at.saveDecision(record)
		return nil
	}

	// 2. Reset daily P&L (reset every day)
	if time.Since(at.lastResetTime) > 24*time.Hour {
		at.dailyPnL = 0
		at.lastResetTime = time.Now()
		logger.Info("📅 Daily P&L reset")
	}

	// 4. Collect trading context
	ctx, err := at.buildTradingContext()
	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("Failed to build trading context: %v", err)
		at.saveDecision(record)
		return fmt.Errorf("failed to build trading context: %w", err)
	}
	notify.CheckEquityTarget(at.userID, at.id, ctx.Account.TotalEquity)

	// 如果没有候选币种，友好提示并跳过本周期
	if len(ctx.CandidateCoins) == 0 {
		logger.Infof("ℹ️  No candidate coins available, skipping this cycle")
		return nil
	}

	// Save equity snapshot independently (decoupled from AI decision, used for drawing profit curve)
	at.saveEquitySnapshot(ctx)

	logger.Info(strings.Repeat("=", 70))
	for _, coin := range ctx.CandidateCoins {
		record.CandidateCoins = append(record.CandidateCoins, coin.Symbol)
	}

	logger.Infof("📊 Account equity: %.2f USDT | Available: %.2f USDT | Positions: %d",
		ctx.Account.TotalEquity, ctx.Account.AvailableBalance, ctx.Account.PositionCount)

	// 5. Use strategy engine to call AI for decision
	logger.Infof("🤖 Requesting AI analysis and decision... [Strategy Engine]")
	aiDecision, err := kernel.GetFullDecisionWithStrategy(ctx, at.mcpClient, at.strategyEngine, "balanced")

	if aiDecision != nil && aiDecision.AIRequestDurationMs > 0 {
		record.AIRequestDurationMs = aiDecision.AIRequestDurationMs
		logger.Infof("⏱️ AI call duration: %.2f seconds", float64(record.AIRequestDurationMs)/1000)
		record.ExecutionLog = append(record.ExecutionLog,
			fmt.Sprintf("AI call duration: %d ms", record.AIRequestDurationMs))
	}

	// Save chain of thought, decisions, and input prompt even if there's an error (for debugging)
	if aiDecision != nil {
		record.SystemPrompt = aiDecision.SystemPrompt // Save system prompt
		record.InputPrompt = aiDecision.UserPrompt
		record.CoTTrace = aiDecision.CoTTrace
		record.RawResponse = aiDecision.RawResponse // Save raw AI response for debugging
		if len(aiDecision.Decisions) > 0 {
			decisionJSON, _ := json.MarshalIndent(aiDecision.Decisions, "", "  ")
			record.DecisionJSON = string(decisionJSON)
		}
	}

	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("Failed to get AI decision: %v", err)

		// Print system prompt and AI chain of thought (output even with errors for debugging)
		if aiDecision != nil {
			logger.Info("\n" + strings.Repeat("=", 70) + "\n")
			logger.Infof("📋 System prompt (error case)")
			logger.Info(strings.Repeat("=", 70))
			logger.Info(aiDecision.SystemPrompt)
			logger.Info(strings.Repeat("=", 70))

			if aiDecision.CoTTrace != "" {
				logger.Info("\n" + strings.Repeat("-", 70) + "\n")
				logger.Info("💭 AI chain of thought analysis (error case):")
				logger.Info(strings.Repeat("-", 70))
				logger.Info(aiDecision.CoTTrace)
				logger.Info(strings.Repeat("-", 70))
			}
		}

		at.saveDecision(record)
		return fmt.Errorf("failed to get AI decision: %w", err)
	}

	// // 5. Print system prompt
	// logger.Infof("\n" + strings.Repeat("=", 70))
	// logger.Infof("📋 System prompt [template: %s]", at.systemPromptTemplate)
	// logger.Info(strings.Repeat("=", 70))
	// logger.Info(decision.SystemPrompt)
	// logger.Infof(strings.Repeat("=", 70) + "\n")

	// 6. Print AI chain of thought
	// logger.Infof("\n" + strings.Repeat("-", 70))
	// logger.Info("💭 AI chain of thought analysis:")
	// logger.Info(strings.Repeat("-", 70))
	// logger.Info(decision.CoTTrace)
	// logger.Infof(strings.Repeat("-", 70) + "\n")

	// 7. Print AI decisions
	// logger.Infof("📋 AI decision list (%d items):\n", len(kernel.Decisions))
	// for i, d := range kernel.Decisions {
	//     logger.Infof("  [%d] %s: %s - %s", i+1, d.Symbol, d.Action, d.Reasoning)
	//     if d.Action == "open_long" || d.Action == "open_short" {
	//        logger.Infof("      Leverage: %dx | Position: %.2f USDT | Stop loss: %.4f | Take profit: %.4f",
	//           d.Leverage, d.PositionSizeUSD, d.StopLoss, d.TakeProfit)
	//     }
	// }
	logger.Info()
	logger.Info(strings.Repeat("-", 70))
	// 8. Sort decisions: ensure close positions first, then open positions (prevent position stacking overflow)
	logger.Info(strings.Repeat("-", 70))

	// 8. Sort decisions: ensure close positions first, then open positions (prevent position stacking overflow)
	sortedDecisions := sortDecisionsByPriority(aiDecision.Decisions)

	logger.Info("🔄 Execution order (optimized): Close positions first → Open positions later")
	for i, d := range sortedDecisions {
		logger.Infof("  [%d] %s %s", i+1, d.Symbol, d.Action)
	}
	logger.Info()

	// Check if trader is stopped before executing any decisions (prevent trades after Stop())
	at.isRunningMutex.RLock()
	running = at.isRunning
	at.isRunningMutex.RUnlock()
	if !running {
		logger.Infof("⏹ Trader stopped before decision execution, aborting cycle #%d", at.callCount)
		return nil
	}

	// Execute decisions and record results
	for _, d := range sortedDecisions {
		// Check if trader is stopped before each decision (allow immediate stop during execution)
		at.isRunningMutex.RLock()
		running = at.isRunning
		at.isRunningMutex.RUnlock()
		if !running {
			logger.Infof("⏹ Trader stopped during decision execution, aborting remaining decisions")
			break
		}

		actionRecord := store.DecisionAction{
			Action:     d.Action,
			Symbol:     d.Symbol,
			Quantity:   0,
			Leverage:   d.Leverage,
			Price:      0,
			StopLoss:   d.StopLoss,
			TakeProfit: d.TakeProfit,
			Confidence: d.Confidence,
			Reasoning:  d.Reasoning,
			Timestamp:  time.Now().UTC(),
			Success:    false,
		}

		if err := at.executeDecisionWithRecord(&d, &actionRecord); err != nil {
			logger.Infof("❌ Failed to execute decision (%s %s): %v", d.Symbol, d.Action, err)
			actionRecord.Error = err.Error()
			record.ExecutionLog = append(record.ExecutionLog, fmt.Sprintf("❌ %s %s failed: %v", d.Symbol, d.Action, err))
		} else {
			actionRecord.Success = true
			record.ExecutionLog = append(record.ExecutionLog, fmt.Sprintf("✓ %s %s succeeded", d.Symbol, d.Action))
			// Brief delay after successful execution
			time.Sleep(1 * time.Second)
		}

		record.Decisions = append(record.Decisions, actionRecord)
	}

	// 9. Save decision record
	if err := at.saveDecision(record); err != nil {
		logger.Infof("⚠ Failed to save decision record: %v", err)
	}

	return nil
}

// buildTradingContext builds trading context
func (at *AutoTrader) buildTradingContext() (*kernel.Context, error) {
	// 1. Get account information
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("failed to get account balance: %w", err)
	}

	// Get account fields
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0
	totalEquity := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Use totalEquity directly if provided by trader (more accurate)
	if eq, ok := balance["totalEquity"].(float64); ok && eq > 0 {
		totalEquity = eq
	} else {
		// Fallback: Total Equity = Wallet balance + Unrealized profit
		totalEquity = totalWalletBalance + totalUnrealizedProfit
	}

	// 2. Get position information
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	var positionInfos []kernel.PositionInfo
	totalMarginUsed := 0.0

	// Current position key set (for cleaning up closed position records)
	currentPositionKeys := make(map[string]bool)

	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity // Short position quantity is negative, convert to positive
		}

		// Skip closed positions (quantity = 0), prevent "ghost positions" from being passed to AI
		if quantity == 0 {
			continue
		}

		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		// Calculate margin used (estimated)
		leverage := 10 // Default value, should actually be fetched from position info
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev + 0.5)
		}
		marginUsed := (quantity * markPrice) / float64(leverage)
		totalMarginUsed += marginUsed

		// Calculate P&L percentage (based on margin, considering leverage)
		pnlPct := calculatePnLPercentage(unrealizedPnl, marginUsed)

		// Get position open time from exchange (preferred) or fallback to local tracking
		posKey := symbol + "_" + side
		currentPositionKeys[posKey] = true

		var updateTime int64
		// Priority 1: Get from database (trader_positions table) - most accurate
		if at.store != nil {
			dbSide := strings.ToUpper(side)
			if dbPos, err := at.store.Position().GetOpenPositionBySymbol(at.id, symbol, dbSide); err == nil && dbPos != nil {
				if dbPos.EntryTime > 0 {
					updateTime = dbPos.EntryTime
				}
			}
		}
		// Priority 2: Get from exchange API (Bybit: createdTime, OKX: createdTime)
		if updateTime == 0 {
			if createdTime, ok := pos["createdTime"].(int64); ok && createdTime > 0 {
				updateTime = createdTime
			}
		}
		// Priority 3: Fallback to local tracking
		if updateTime == 0 {
			if _, exists := at.positionFirstSeenTime[posKey]; !exists {
				at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()
			}
			updateTime = at.positionFirstSeenTime[posKey]
		}

		// Get peak profit rate for this position
		at.peakPnLCacheMutex.RLock()
		peakPnlPct := at.peakPnLCache[posKey]
		at.peakPnLCacheMutex.RUnlock()

		positionInfos = append(positionInfos, kernel.PositionInfo{
			Symbol:           symbol,
			Side:             side,
			EntryPrice:       entryPrice,
			MarkPrice:        markPrice,
			Quantity:         quantity,
			Leverage:         leverage,
			UnrealizedPnL:    unrealizedPnl,
			UnrealizedPnLPct: pnlPct,
			PeakPnLPct:       peakPnlPct,
			LiquidationPrice: liquidationPrice,
			MarginUsed:       marginUsed,
			UpdateTime:       updateTime,
		})
	}

	// Clean up closed position records
	for key := range at.positionFirstSeenTime {
		if !currentPositionKeys[key] {
			delete(at.positionFirstSeenTime, key)
		}
	}

	// Enrich positions with stop-loss / take-profit from exchange open orders
	at.applyStopTargetsToPositions(positionInfos)

	// 3. Use strategy engine to get candidate coins (must have strategy engine)
	if at.strategyEngine == nil {
		return nil, fmt.Errorf("trader has no strategy engine configured")
	}
	candidateCoins, err := at.strategyEngine.GetCandidateCoins()
	if err != nil {
		return nil, fmt.Errorf("failed to get candidate coins: %w", err)
	}
	logger.Infof("📋 [%s] Strategy engine fetched candidate coins: %d", at.name, len(candidateCoins))

	// 4. Collect pending limit orders for AI context
	openOrders := at.collectOpenLimitOrders(positionInfos, candidateCoins)

	// 5. Calculate total P&L
	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	// 6. Get leverage from strategy config
	strategyConfig := at.strategyEngine.GetConfig()
	btcEthLeverage := strategyConfig.RiskControl.BTCETHMaxLeverage
	altcoinLeverage := strategyConfig.RiskControl.AltcoinMaxLeverage
	logger.Infof("📋 [%s] Strategy leverage config: BTC/ETH=%dx, Altcoin=%dx", at.name, btcEthLeverage, altcoinLeverage)

	// 7. Build context
	ctx := &kernel.Context{
		CurrentTime:     time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		RuntimeMinutes:  int(time.Since(at.startTime).Minutes()),
		CallCount:       at.callCount,
		Exchange:        at.exchange,
		BTCETHLeverage:  btcEthLeverage,
		AltcoinLeverage: altcoinLeverage,
		Account: kernel.AccountInfo{
			TotalEquity:      totalEquity,
			AvailableBalance: availableBalance,
			UnrealizedPnL:    totalUnrealizedProfit,
			TotalPnL:         totalPnL,
			TotalPnLPct:      totalPnLPct,
			MarginUsed:       totalMarginUsed,
			MarginUsedPct:    marginUsedPct,
			PositionCount:    len(positionInfos),
		},
		Positions:      positionInfos,
		CandidateCoins: candidateCoins,
		OpenOrders:     openOrders,
	}

	// 8. Add recent closed trades (if store is available)
	if at.store != nil {
		// Get recent 10 closed trades for AI context
		recentTrades, err := at.store.Position().GetRecentTrades(at.id, 10)
		if err != nil {
			logger.Infof("⚠️ [%s] Failed to get recent trades: %v", at.name, err)
		} else {
			logger.Infof("📊 [%s] Found %d recent closed trades for AI context", at.name, len(recentTrades))
			for _, trade := range recentTrades {
				// Convert Unix timestamps to formatted strings for AI readability
				entryTimeStr := ""
				if trade.EntryTime > 0 {
					entryTimeStr = time.Unix(trade.EntryTime, 0).UTC().Format("01-02 15:04 UTC")
				}
				exitTimeStr := ""
				if trade.ExitTime > 0 {
					exitTimeStr = time.Unix(trade.ExitTime, 0).UTC().Format("01-02 15:04 UTC")
				}

				ctx.RecentOrders = append(ctx.RecentOrders, kernel.RecentOrder{
					Symbol:       trade.Symbol,
					Side:         trade.Side,
					EntryPrice:   trade.EntryPrice,
					ExitPrice:    trade.ExitPrice,
					RealizedPnL:  trade.RealizedPnL,
					PnLPct:       trade.PnLPct,
					EntryTime:    entryTimeStr,
					ExitTime:     exitTimeStr,
					HoldDuration: trade.HoldDuration,
				})
			}
		}
		// Get trading statistics for AI context
		stats, err := at.store.Position().GetFullStats(at.id)
		if err != nil {
			logger.Infof("⚠️ [%s] Failed to get trading stats: %v", at.name, err)
		} else if stats == nil {
			logger.Infof("⚠️ [%s] GetFullStats returned nil", at.name)
		} else if stats.TotalTrades == 0 {
			logger.Infof("⚠️ [%s] GetFullStats returned 0 trades (traderID=%s)", at.name, at.id)
		} else {
			ctx.TradingStats = &kernel.TradingStats{
				TotalTrades:    stats.TotalTrades,
				WinRate:        stats.WinRate,
				ProfitFactor:   stats.ProfitFactor,
				SharpeRatio:    stats.SharpeRatio,
				TotalPnL:       stats.TotalPnL,
				AvgWin:         stats.AvgWin,
				AvgLoss:        stats.AvgLoss,
				MaxDrawdownPct: stats.MaxDrawdownPct,
			}
			logger.Infof("📈 [%s] Trading stats: %d trades, %.1f%% win rate, PF=%.2f, Sharpe=%.2f, DD=%.1f%%",
				at.name, stats.TotalTrades, stats.WinRate, stats.ProfitFactor, stats.SharpeRatio, stats.MaxDrawdownPct)
		}
	} else {
		logger.Infof("⚠️ [%s] Store is nil, cannot get recent trades", at.name)
	}

	// 9. Get quantitative data (if enabled in strategy config)
	if strategyConfig.Indicators.EnableQuantData {
		// Collect symbols to query (candidate coins + position coins)
		symbolsToQuery := make(map[string]bool)
		for _, coin := range candidateCoins {
			symbolsToQuery[coin.Symbol] = true
		}
		for _, pos := range positionInfos {
			symbolsToQuery[pos.Symbol] = true
		}

		symbols := make([]string, 0, len(symbolsToQuery))
		for sym := range symbolsToQuery {
			symbols = append(symbols, sym)
		}

		logger.Infof("📊 [%s] Fetching quantitative data for %d symbols...", at.name, len(symbols))
		ctx.QuantDataMap = at.strategyEngine.FetchQuantDataBatch(symbols)
		logger.Infof("📊 [%s] Successfully fetched quantitative data for %d symbols", at.name, len(ctx.QuantDataMap))
	}

	// 9. Get OI ranking data (market-wide position changes)
	if strategyConfig.Indicators.EnableOIRanking {
		logger.Infof("📊 [%s] Fetching OI ranking data...", at.name)
		ctx.OIRankingData = at.strategyEngine.FetchOIRankingData()
		if ctx.OIRankingData != nil {
			logger.Infof("📊 [%s] OI ranking data ready: %d top, %d low positions",
				at.name, len(ctx.OIRankingData.TopPositions), len(ctx.OIRankingData.LowPositions))
		}
	}

	// 10. Get NetFlow ranking data (market-wide fund flow)
	if strategyConfig.Indicators.EnableNetFlowRanking {
		logger.Infof("💰 [%s] Fetching NetFlow ranking data...", at.name)
		ctx.NetFlowRankingData = at.strategyEngine.FetchNetFlowRankingData()
		if ctx.NetFlowRankingData != nil {
			logger.Infof("💰 [%s] NetFlow ranking data ready: inst_in=%d, inst_out=%d",
				at.name, len(ctx.NetFlowRankingData.InstitutionFutureTop), len(ctx.NetFlowRankingData.InstitutionFutureLow))
		}
	}

	// 11. Get Price ranking data (market-wide gainers/losers)
	if strategyConfig.Indicators.EnablePriceRanking {
		logger.Infof("📈 [%s] Fetching Price ranking data...", at.name)
		ctx.PriceRankingData = at.strategyEngine.FetchPriceRankingData()
		if ctx.PriceRankingData != nil {
			logger.Infof("📈 [%s] Price ranking data ready for %d durations",
				at.name, len(ctx.PriceRankingData.Durations))
		}
	}

	return ctx, nil
}

func (at *AutoTrader) collectOpenLimitOrders(positions []kernel.PositionInfo, candidates []kernel.CandidateCoin) []kernel.PendingOrder {
	symbolSet := make(map[string]bool)
	for _, pos := range positions {
		symbolSet[pos.Symbol] = true
	}
	for _, coin := range candidates {
		symbolSet[coin.Symbol] = true
	}
	for _, pending := range at.listPendingLimitOrders() {
		if pending.Symbol != "" {
			symbolSet[pending.Symbol] = true
		}
	}

	if allGetter, ok := at.trader.(OpenOrdersAllGetter); ok {
		openOrders, err := allGetter.GetOpenOrdersAll()
		if err == nil {
			return at.collectOpenLimitOrdersFromExchange(openOrders)
		}
		logger.Warnf("[%s] Failed to get all open orders: %v", at.name, err)
	}

	return at.collectOpenLimitOrdersForSymbols(symbolSet)
}

func (at *AutoTrader) collectOpenLimitOrdersForSymbols(symbolSet map[string]bool) []kernel.PendingOrder {
	openOrders := make([]OpenOrder, 0)

	for symbol := range symbolSet {
		orders, err := at.trader.GetOpenOrders(symbol)
		if err != nil {
			logger.Warnf("[%s] Failed to get open orders for %s: %v", at.name, symbol, err)
			continue
		}
		openOrders = append(openOrders, orders...)
	}

	return at.collectOpenLimitOrdersFromExchange(openOrders)
}

func (at *AutoTrader) collectOpenLimitOrdersFromExchange(openOrders []OpenOrder) []kernel.PendingOrder {
	orders := make([]kernel.PendingOrder, 0, len(openOrders))
	seen := make(map[string]bool)

	for _, order := range openOrders {
		if !isLimitOrderType(order.Type) {
			if order.Price <= 0 || order.StopPrice > 0 {
				continue
			}
		}
		pending := kernel.PendingOrder{
			OrderID:      order.OrderID,
			Symbol:       order.Symbol,
			Side:         order.Side,
			PositionSide: order.PositionSide,
			Type:         order.Type,
			Price:        order.Price,
			StopPrice:    order.StopPrice,
			Quantity:     order.Quantity,
		}

		if meta := at.getPendingLimitOrder(order.OrderID); meta != nil {
			pending.ClientID = meta.ClientID
			pending.PostOnly = meta.PostOnly
			pending.AgeSeconds = int64(time.Since(meta.CreatedAt).Seconds())
			pending.StopLoss = meta.StopLoss
			pending.TakeProfit = meta.TakeProfit
		}

		orders = append(orders, pending)
		if order.OrderID != "" {
			seen[order.OrderID] = true
		}
	}

	for _, meta := range at.listPendingLimitOrders() {
		if meta.OrderID != "" && seen[meta.OrderID] {
			continue
		}
		positionSide := "LONG"
		if strings.ToUpper(meta.Side) == "SELL" {
			positionSide = "SHORT"
		}
		orders = append(orders, kernel.PendingOrder{
			OrderID:      meta.OrderID,
			ClientID:     meta.ClientID,
			Symbol:       meta.Symbol,
			Side:         meta.Side,
			PositionSide: positionSide,
			Type:         "LIMIT",
			Price:        meta.Price,
			Quantity:     meta.Quantity,
			PostOnly:     meta.PostOnly,
			AgeSeconds:   int64(time.Since(meta.CreatedAt).Seconds()),
		})
	}

	return orders
}

func (at *AutoTrader) applyStopTargetsToPositions(positions []kernel.PositionInfo) {
	if len(positions) == 0 {
		return
	}

	symbolSet := make(map[string]bool)
	for _, pos := range positions {
		symbolSet[market.Normalize(pos.Symbol)] = true
	}

	openOrders := at.getOpenOrdersRawForSymbols(symbolSet)
	if len(openOrders) == 0 {
		return
	}

	slMap, tpMap := buildStopTargetMaps(openOrders)
	for i := range positions {
		key := stopTargetKey(positions[i].Symbol, positions[i].Side)
		if price, ok := slMap[key]; ok {
			positions[i].StopLoss = price
		}
		if price, ok := tpMap[key]; ok {
			positions[i].TakeProfit = price
		}
	}
}

func (at *AutoTrader) getOpenOrdersRawForSymbols(symbolSet map[string]bool) []OpenOrder {
	openOrders := make([]OpenOrder, 0)

	if allGetter, ok := at.trader.(OpenOrdersAllGetter); ok {
		orders, err := allGetter.GetOpenOrdersAll()
		if err == nil {
			return orders
		}
		logger.Warnf("[%s] Failed to get all open orders: %v", at.name, err)
	}

	for symbol := range symbolSet {
		orders, err := at.trader.GetOpenOrders(symbol)
		if err != nil {
			logger.Warnf("[%s] Failed to get open orders for %s: %v", at.name, symbol, err)
			continue
		}
		openOrders = append(openOrders, orders...)
	}

	return openOrders
}

func buildStopTargetMaps(openOrders []OpenOrder) (map[string]float64, map[string]float64) {
	slMap := make(map[string]float64)
	tpMap := make(map[string]float64)

	for _, order := range openOrders {
		orderType := strings.ToUpper(order.Type)
		if !isStopLossOrderType(orderType) && !isTakeProfitOrderType(orderType) {
			continue
		}

		positionSide := strings.ToUpper(order.PositionSide)
		if positionSide == "" {
			positionSide = inferPositionSideFromOrder(order)
		}
		if positionSide != "LONG" && positionSide != "SHORT" {
			continue
		}

		price := order.StopPrice
		if price <= 0 {
			price = order.Price
		}
		if price <= 0 {
			continue
		}

		key := stopTargetKey(order.Symbol, positionSide)
		if isTakeProfitOrderType(orderType) {
			tpMap[key] = price
		} else if isStopLossOrderType(orderType) {
			slMap[key] = price
		}
	}

	return slMap, tpMap
}

func stopTargetKey(symbol, side string) string {
	return market.Normalize(symbol) + "|" + strings.ToUpper(side)
}

func isStopLossOrderType(orderType string) bool {
	orderType = strings.ToUpper(orderType)
	return strings.Contains(orderType, "STOP") && !strings.Contains(orderType, "TAKE_PROFIT")
}

func isTakeProfitOrderType(orderType string) bool {
	orderType = strings.ToUpper(orderType)
	return strings.Contains(orderType, "TAKE_PROFIT")
}

func inferPositionSideFromOrder(order OpenOrder) string {
	side := strings.ToUpper(order.Side)
	if side == "SELL" {
		return "LONG"
	}
	if side == "BUY" {
		return "SHORT"
	}
	return ""
}

func (at *AutoTrader) GetOpenLimitOrdersSnapshot(symbol string) ([]kernel.PendingOrder, error) {
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	symbolSet := make(map[string]bool)
	if symbol != "" {
		symbolSet[market.Normalize(symbol)] = true
	}
	for _, pos := range positions {
		if symbol, ok := pos["symbol"].(string); ok && symbol != "" {
			symbolSet[symbol] = true
		}
	}
	for _, pending := range at.listPendingLimitOrders() {
		if pending.Symbol != "" {
			symbolSet[pending.Symbol] = true
		}
	}

	if symbol == "" {
		if allGetter, ok := at.trader.(OpenOrdersAllGetter); ok {
			openOrders, err := allGetter.GetOpenOrdersAll()
			if err == nil {
				return at.collectOpenLimitOrdersFromExchange(openOrders), nil
			}
			logger.Warnf("[%s] Failed to get all open orders: %v", at.name, err)
		}
	}

	return at.collectOpenLimitOrdersForSymbols(symbolSet), nil
}

func (at *AutoTrader) limitOrdersEnabled() bool {
	if at.config.StrategyConfig == nil {
		return true
	}
	return at.config.StrategyConfig.LimitOrdersEnabled()
}

// executeDecisionWithRecord executes AI decision and records detailed information
func (at *AutoTrader) executeDecisionWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	switch decision.Action {
	case "open_long":
		return at.executeOpenLongWithRecord(decision, actionRecord)
	case "open_short":
		return at.executeOpenShortWithRecord(decision, actionRecord)
	case "close_long":
		return at.executeCloseLongWithRecord(decision, actionRecord)
	case "close_short":
		return at.executeCloseShortWithRecord(decision, actionRecord)
	case "place_limit_buy":
		return at.executePlaceLimitOrderWithRecord(decision, actionRecord, "BUY")
	case "place_limit_sell":
		return at.executePlaceLimitOrderWithRecord(decision, actionRecord, "SELL")
	case "cancel_order":
		return at.executeCancelOrderWithRecord(decision, actionRecord)
	case "cancel_all_orders":
		return at.executeCancelAllOrdersWithRecord(decision, actionRecord)
	case "update_stop_loss":
		return at.executeUpdateStopLossWithRecord(decision, actionRecord)
	case "update_take_profit":
		return at.executeUpdateTakeProfitWithRecord(decision, actionRecord)
	case "hold", "wait":
		// No execution needed, just record
		return nil
	default:
		return fmt.Errorf("unknown action: %s", decision.Action)
	}
}

// ExecuteDecision executes a trading decision from external sources (e.g., debate consensus)
// This is a public method that can be called by other modules
func (at *AutoTrader) ExecuteDecision(d *kernel.Decision) error {
	logger.Infof("[%s] Executing external decision: %s %s", at.name, d.Action, d.Symbol)

	// Create a minimal action record for tracking
	actionRecord := &store.DecisionAction{
		Symbol:     d.Symbol,
		Action:     d.Action,
		Leverage:   d.Leverage,
		StopLoss:   d.StopLoss,
		TakeProfit: d.TakeProfit,
		Confidence: d.Confidence,
		Reasoning:  d.Reasoning,
	}

	// Execute the decision
	err := at.executeDecisionWithRecord(d, actionRecord)
	if err != nil {
		logger.Errorf("[%s] External decision execution failed: %v", at.name, err)
		return err
	}

	logger.Infof("[%s] External decision executed successfully: %s %s", at.name, d.Action, d.Symbol)
	return nil
}

// executeOpenLongWithRecord executes open long position and records detailed information
func (at *AutoTrader) executeOpenLongWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  📈 Open long: %s", decision.Symbol)

	// ⚠️ Get current positions for multiple checks
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	// [CODE ENFORCED] Check max positions limit
	if err := at.enforceMaxPositions(len(positions)); err != nil {
		return err
	}

	// Check if there's already a position in the same symbol and direction
	for _, pos := range positions {
		if pos["symbol"] == decision.Symbol && pos["side"] == "long" {
			return fmt.Errorf("❌ %s already has long position, close it first", decision.Symbol)
		}
	}

	// Get current price
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}

	// Get balance (needed for multiple checks)
	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("failed to get account balance: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Get equity for position value ratio check
	equity := 0.0
	if eq, ok := balance["totalEquity"].(float64); ok && eq > 0 {
		equity = eq
	} else if eq, ok := balance["totalWalletBalance"].(float64); ok && eq > 0 {
		equity = eq
	} else {
		equity = availableBalance // Fallback to available balance
	}

	// [CODE ENFORCED] Position Value Ratio Check: position_value <= equity × ratio
	adjustedPositionSize, wasCapped := at.enforcePositionValueRatio(decision.PositionSizeUSD, equity, decision.Symbol)
	if wasCapped {
		decision.PositionSizeUSD = adjustedPositionSize
	}

	// ⚠️ Auto-adjust position size if insufficient margin
	// Formula: totalRequired = positionSize/leverage + positionSize*0.001 + positionSize/leverage*0.01
	//        = positionSize * (1.01/leverage + 0.001)
	marginFactor := 1.01/float64(decision.Leverage) + 0.001
	maxAffordablePositionSize := availableBalance / marginFactor

	actualPositionSize := decision.PositionSizeUSD
	if actualPositionSize > maxAffordablePositionSize {
		// Use 98% of max to leave buffer for price fluctuation
		adjustedSize := maxAffordablePositionSize * 0.98
		logger.Infof("  ⚠️ Position size %.2f exceeds max affordable %.2f, auto-reducing to %.2f",
			actualPositionSize, maxAffordablePositionSize, adjustedSize)
		actualPositionSize = adjustedSize
		decision.PositionSizeUSD = actualPositionSize
	}

	// [CODE ENFORCED] Minimum position size check
	if err := at.enforceMinPositionSize(decision.PositionSizeUSD); err != nil {
		return err
	}

	// Calculate quantity with adjusted position size
	quantity := actualPositionSize / marketData.CurrentPrice
	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// Set margin mode
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		logger.Infof("  ⚠️ Failed to set margin mode: %v", err)
		// Continue execution, doesn't affect trading
	}

	// Open position
	order, err := at.trader.OpenLong(decision.Symbol, quantity, decision.Leverage)
	if err != nil {
		return err
	}

	// Record order ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	logger.Infof("  ✓ Position opened successfully, order ID: %v, quantity: %.4f", order["orderId"], quantity)

	// Record order to database and poll for confirmation
	at.recordAndConfirmOrder(order, decision.Symbol, "open_long", quantity, marketData.CurrentPrice, decision.Leverage, 0)

	// Record position opening time
	posKey := decision.Symbol + "_long"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// Set stop loss and take profit
	if err := at.trader.SetStopLoss(decision.Symbol, "LONG", quantity, decision.StopLoss); err != nil {
		logger.Infof("  ⚠ Failed to set stop loss: %v", err)
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "LONG", quantity, decision.TakeProfit); err != nil {
		logger.Infof("  ⚠ Failed to set take profit: %v", err)
	}

	if at.userID != "" {
		notify.NotifyOpenTrade(at.userID, at.id, notify.OpenTradeInfo{
			Symbol:   decision.Symbol,
			Side:     "BUY",
			Price:    marketData.CurrentPrice,
			Quantity: quantity,
			Leverage: decision.Leverage,
			Time:     time.Now(),
		})
	}

	return nil
}

// executeOpenShortWithRecord executes open short position and records detailed information
func (at *AutoTrader) executeOpenShortWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  📉 Open short: %s", decision.Symbol)

	// ⚠️ Get current positions for multiple checks
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	// [CODE ENFORCED] Check max positions limit
	if err := at.enforceMaxPositions(len(positions)); err != nil {
		return err
	}

	// Check if there's already a position in the same symbol and direction
	for _, pos := range positions {
		if pos["symbol"] == decision.Symbol && pos["side"] == "short" {
			return fmt.Errorf("❌ %s already has short position, close it first", decision.Symbol)
		}
	}

	// Get current price
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}

	// Get balance (needed for multiple checks)
	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("failed to get account balance: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Get equity for position value ratio check
	equity := 0.0
	if eq, ok := balance["totalEquity"].(float64); ok && eq > 0 {
		equity = eq
	} else if eq, ok := balance["totalWalletBalance"].(float64); ok && eq > 0 {
		equity = eq
	} else {
		equity = availableBalance // Fallback to available balance
	}

	// [CODE ENFORCED] Position Value Ratio Check: position_value <= equity × ratio
	adjustedPositionSize, wasCapped := at.enforcePositionValueRatio(decision.PositionSizeUSD, equity, decision.Symbol)
	if wasCapped {
		decision.PositionSizeUSD = adjustedPositionSize
	}

	// ⚠️ Auto-adjust position size if insufficient margin
	// Formula: totalRequired = positionSize/leverage + positionSize*0.001 + positionSize/leverage*0.01
	//        = positionSize * (1.01/leverage + 0.001)
	marginFactor := 1.01/float64(decision.Leverage) + 0.001
	maxAffordablePositionSize := availableBalance / marginFactor

	actualPositionSize := decision.PositionSizeUSD
	if actualPositionSize > maxAffordablePositionSize {
		// Use 98% of max to leave buffer for price fluctuation
		adjustedSize := maxAffordablePositionSize * 0.98
		logger.Infof("  ⚠️ Position size %.2f exceeds max affordable %.2f, auto-reducing to %.2f",
			actualPositionSize, maxAffordablePositionSize, adjustedSize)
		actualPositionSize = adjustedSize
		decision.PositionSizeUSD = actualPositionSize
	}

	// [CODE ENFORCED] Minimum position size check
	if err := at.enforceMinPositionSize(decision.PositionSizeUSD); err != nil {
		return err
	}

	// Calculate quantity with adjusted position size
	quantity := actualPositionSize / marketData.CurrentPrice
	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// Set margin mode
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		logger.Infof("  ⚠️ Failed to set margin mode: %v", err)
		// Continue execution, doesn't affect trading
	}

	// Open position
	order, err := at.trader.OpenShort(decision.Symbol, quantity, decision.Leverage)
	if err != nil {
		return err
	}

	// Record order ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	logger.Infof("  ✓ Position opened successfully, order ID: %v, quantity: %.4f", order["orderId"], quantity)

	// Record order to database and poll for confirmation
	at.recordAndConfirmOrder(order, decision.Symbol, "open_short", quantity, marketData.CurrentPrice, decision.Leverage, 0)

	// Record position opening time
	posKey := decision.Symbol + "_short"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// Set stop loss and take profit
	if err := at.trader.SetStopLoss(decision.Symbol, "SHORT", quantity, decision.StopLoss); err != nil {
		logger.Infof("  ⚠ Failed to set stop loss: %v", err)
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "SHORT", quantity, decision.TakeProfit); err != nil {
		logger.Infof("  ⚠ Failed to set take profit: %v", err)
	}

	if at.userID != "" {
		notify.NotifyOpenTrade(at.userID, at.id, notify.OpenTradeInfo{
			Symbol:   decision.Symbol,
			Side:     "SELL",
			Price:    marketData.CurrentPrice,
			Quantity: quantity,
			Leverage: decision.Leverage,
			Time:     time.Now(),
		})
	}

	return nil
}

func (at *AutoTrader) executePlaceLimitOrderWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction, side string) error {
	logger.Infof("  🧾 Place limit %s: %s", strings.ToLower(side), decision.Symbol)

	if !at.limitOrdersEnabled() {
		return fmt.Errorf("limit order entry disabled by strategy config")
	}

	gridTrader, ok := at.trader.(GridTrader)
	if !ok {
		return fmt.Errorf("limit orders not supported by exchange: %s", at.exchange)
	}

	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	if err := at.enforceMaxPositions(len(positions)); err != nil {
		return err
	}

	existingSide := "long"
	if side == "SELL" {
		existingSide = "short"
	}
	for _, pos := range positions {
		if pos["symbol"] == decision.Symbol && pos["side"] == existingSide {
			return fmt.Errorf("❌ %s already has %s position, close it first", decision.Symbol, existingSide)
		}
	}

	if decision.Price <= 0 {
		return fmt.Errorf("limit order price must be greater than 0")
	}
	if decision.Leverage <= 0 {
		return fmt.Errorf("limit order leverage must be greater than 0")
	}
	if decision.StopLoss <= 0 || decision.TakeProfit <= 0 {
		return fmt.Errorf("limit orders require stop loss and take profit")
	}

	if exists, id := at.hasOpenLimitOrder(decision.Symbol, side); exists {
		return fmt.Errorf("❌ %s %s 已有未成交限价单(%s)，同方向仅允许一个", decision.Symbol, side, id)
	}

	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("failed to get account balance: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	equity := 0.0
	if eq, ok := balance["totalEquity"].(float64); ok && eq > 0 {
		equity = eq
	} else if eq, ok := balance["totalWalletBalance"].(float64); ok && eq > 0 {
		equity = eq
	} else {
		equity = availableBalance
	}

	positionSizeUSD := decision.PositionSizeUSD
	if decision.Quantity > 0 {
		positionSizeUSD = decision.Quantity * decision.Price
	}
	if positionSizeUSD <= 0 {
		return fmt.Errorf("position size must be greater than 0 for limit order")
	}
	decision.PositionSizeUSD = positionSizeUSD

	adjustedPositionSize, wasCapped := at.enforcePositionValueRatio(positionSizeUSD, equity, decision.Symbol)
	if wasCapped {
		positionSizeUSD = adjustedPositionSize
		decision.PositionSizeUSD = adjustedPositionSize
	}

	marginFactor := 1.01/float64(decision.Leverage) + 0.001
	maxAffordablePositionSize := availableBalance / marginFactor
	if positionSizeUSD > maxAffordablePositionSize {
		adjustedSize := maxAffordablePositionSize * 0.98
		logger.Infof("  ⚠️ Position size %.2f exceeds max affordable %.2f, auto-reducing to %.2f",
			positionSizeUSD, maxAffordablePositionSize, adjustedSize)
		positionSizeUSD = adjustedSize
		decision.PositionSizeUSD = adjustedSize
	}

	if err := at.enforceMinPositionSize(positionSizeUSD); err != nil {
		return err
	}

	quantity := positionSizeUSD / decision.Price
	decision.Quantity = quantity

	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		logger.Infof("  ⚠️ Failed to set margin mode: %v", err)
	}

	clientID := decision.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("ai-limit-%d", time.Now().UnixNano()%1000000)
	}

	postOnly := decision.PostOnly
	if !postOnly {
		postOnly = true
	}

	req := &LimitOrderRequest{
		Symbol:       decision.Symbol,
		Side:         side,
		PositionSide: strings.ToUpper(existingSide),
		Price:        decision.Price,
		Quantity:     quantity,
		Leverage:     decision.Leverage,
		PostOnly:     postOnly,
		ReduceOnly:   decision.ReduceOnly,
		ClientID:     clientID,
	}

	result, err := gridTrader.PlaceLimitOrder(req)
	if err != nil {
		return fmt.Errorf("failed to place limit order: %w", err)
	}

	actionRecord.Price = decision.Price
	actionRecord.Quantity = quantity
	actionRecord.Leverage = decision.Leverage
	actionRecord.StopLoss = decision.StopLoss
	actionRecord.TakeProfit = decision.TakeProfit

	if result.OrderID != "" {
		if id, err := strconv.ParseInt(result.OrderID, 10, 64); err == nil {
			actionRecord.OrderID = id
		}
	}

	at.recordPendingLimitOrder(result, req, decision.StopLoss, decision.TakeProfit)
	logger.Infof("  ✓ Limit order placed: %s %s @ %.4f, qty=%.4f", decision.Symbol, side, decision.Price, quantity)
	if at.userID != "" {
		notify.NotifyLimitOrderPlaced(at.userID, at.id, decision.Symbol, side, decision.Price, quantity)
	}

	return nil
}

func (at *AutoTrader) executeCancelOrderWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  🧹 Cancel order: %s", decision.Symbol)

	gridTrader, ok := at.trader.(GridTrader)
	if !ok {
		return fmt.Errorf("cancel order not supported by exchange: %s", at.exchange)
	}

	orderID := decision.OrderID.String()
	if orderID == "" && decision.ClientID != "" {
		for _, pending := range at.listPendingLimitOrders() {
			if pending.ClientID == decision.ClientID {
				orderID = pending.OrderID
				break
			}
		}
	}
	if orderID == "" {
		return fmt.Errorf("cancel_order requires order_id or client_id")
	}

	if err := gridTrader.CancelOrder(decision.Symbol, orderID); err != nil {
		if !isOrderAlreadyClosed(err) {
			return err
		}
		logger.Infof("[%s] Order already closed on exchange, clearing pending: %s", at.name, orderID)
	}

	at.pendingLimitOrdersMu.Lock()
	for key, pending := range at.pendingLimitOrders {
		if pending.OrderID == orderID || (decision.ClientID != "" && pending.ClientID == decision.ClientID) {
			delete(at.pendingLimitOrders, key)
		}
	}
	at.pendingLimitOrdersMu.Unlock()

	actionRecord.OrderID, _ = strconv.ParseInt(orderID, 10, 64)
	return nil
}

func (at *AutoTrader) executeCancelAllOrdersWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  🧹 Cancel all orders: %s", decision.Symbol)

	if err := at.trader.CancelAllOrders(decision.Symbol); err != nil {
		return err
	}

	at.pendingLimitOrdersMu.Lock()
	for key, pending := range at.pendingLimitOrders {
		if pending.Symbol == decision.Symbol {
			delete(at.pendingLimitOrders, key)
		}
	}
	at.pendingLimitOrdersMu.Unlock()

	return nil
}

func (at *AutoTrader) executeUpdateStopLossWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	positionSide, qty, err := at.getPositionSideAndQuantity(decision)
	if err != nil {
		return err
	}
	stopLossPrice := decision.Price
	if stopLossPrice <= 0 {
		stopLossPrice = decision.StopLoss
	}
	if stopLossPrice <= 0 {
		return fmt.Errorf("stop-loss price is required")
	}
	previousStopLoss := at.getExistingStopLossPrice(decision.Symbol, positionSide)
	if err := at.trader.CancelStopLossOrders(decision.Symbol); err != nil {
		return fmt.Errorf("failed to cancel existing stop-loss orders: %w", err)
	}
	if err := at.trader.SetStopLoss(decision.Symbol, positionSide, qty, stopLossPrice); err != nil {
		if previousStopLoss > 0 {
			if restoreErr := at.trader.SetStopLoss(decision.Symbol, positionSide, qty, previousStopLoss); restoreErr != nil {
				return fmt.Errorf("failed to set stop-loss: %w (restore failed: %v)", err, restoreErr)
			}
			logger.Warnf("⚠️ [%s] Stop-loss update failed, restored previous stop-loss %.6f for %s %s",
				at.name, previousStopLoss, decision.Symbol, positionSide)
			return fmt.Errorf("failed to set stop-loss: %w (restored previous stop-loss)", err)
		}
		return err
	}
	actionRecord.Price = stopLossPrice
	actionRecord.StopLoss = stopLossPrice
	actionRecord.Quantity = qty
	if at.userID != "" {
		notify.NotifyUpdateStopLoss(at.userID, at.id, decision.Symbol, stopLossPrice, qty)
	}
	return nil
}

func (at *AutoTrader) getExistingStopLossPrice(symbol, positionSide string) float64 {
	symbolSet := map[string]bool{market.Normalize(symbol): true}
	openOrders := at.getOpenOrdersRawForSymbols(symbolSet)
	if len(openOrders) == 0 {
		return 0
	}
	slMap, _ := buildStopTargetMaps(openOrders)
	key := stopTargetKey(symbol, positionSide)
	if price, ok := slMap[key]; ok {
		return price
	}
	return 0
}

func (at *AutoTrader) executeUpdateTakeProfitWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	positionSide, qty, err := at.getPositionSideAndQuantity(decision)
	if err != nil {
		return err
	}
	takeProfitPrice := decision.Price
	if takeProfitPrice <= 0 {
		takeProfitPrice = decision.TakeProfit
	}
	if takeProfitPrice <= 0 {
		return fmt.Errorf("take-profit price is required")
	}
	if err := at.trader.CancelTakeProfitOrders(decision.Symbol); err != nil {
		return fmt.Errorf("failed to cancel existing take-profit orders: %w", err)
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, positionSide, qty, takeProfitPrice); err != nil {
		return err
	}
	actionRecord.Price = takeProfitPrice
	actionRecord.TakeProfit = takeProfitPrice
	actionRecord.Quantity = qty
	if at.userID != "" {
		notify.NotifyUpdateTakeProfit(at.userID, at.id, decision.Symbol, takeProfitPrice, qty)
	}
	return nil
}

// executeCloseLongWithRecord executes close long position and records detailed information
func (at *AutoTrader) executeCloseLongWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  🔄 Close long: %s", decision.Symbol)

	// Get current price
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// Normalize symbol for database lookup
	normalizedSymbol := market.Normalize(decision.Symbol)

	// Get entry price and quantity - prioritize local database for accurate quantity
	var entryPrice float64
	var quantity float64

	// First try to get from local database (more accurate for quantity)
	if at.store != nil {
		if openPos, err := at.store.Position().GetOpenPositionBySymbol(at.id, normalizedSymbol, "LONG"); err == nil && openPos != nil {
			quantity = openPos.Quantity
			entryPrice = openPos.EntryPrice
			logger.Infof("  📊 Using local position data: qty=%.8f, entry=%.2f", quantity, entryPrice)
		}
	}

	// Fallback to exchange API if local data not found
	if quantity == 0 {
		positions, err := at.trader.GetPositions()
		if err == nil {
			for _, pos := range positions {
				if pos["symbol"] == decision.Symbol && pos["side"] == "long" {
					if ep, ok := pos["entryPrice"].(float64); ok {
						entryPrice = ep
					}
					if amt, ok := pos["positionAmt"].(float64); ok && amt > 0 {
						quantity = amt
					}
					break
				}
			}
		}
		logger.Infof("  📊 Using exchange position data: qty=%.8f, entry=%.2f", quantity, entryPrice)
	}

	// Close position
	order, err := at.trader.CloseLong(decision.Symbol, 0) // 0 = close all
	if err != nil {
		return err
	}

	// Record order ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	// Record order to database and poll for confirmation
	at.recordAndConfirmOrder(order, decision.Symbol, "close_long", quantity, marketData.CurrentPrice, 0, entryPrice)

	logger.Infof("  ✓ Position closed successfully")
	return nil
}

// executeCloseShortWithRecord executes close short position and records detailed information
func (at *AutoTrader) executeCloseShortWithRecord(decision *kernel.Decision, actionRecord *store.DecisionAction) error {
	logger.Infof("  🔄 Close short: %s", decision.Symbol)

	// Get current price
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// Normalize symbol for database lookup
	normalizedSymbol := market.Normalize(decision.Symbol)

	// Get entry price and quantity - prioritize local database for accurate quantity
	var entryPrice float64
	var quantity float64

	// First try to get from local database (more accurate for quantity)
	if at.store != nil {
		if openPos, err := at.store.Position().GetOpenPositionBySymbol(at.id, normalizedSymbol, "SHORT"); err == nil && openPos != nil {
			quantity = openPos.Quantity
			entryPrice = openPos.EntryPrice
			logger.Infof("  📊 Using local position data: qty=%.8f, entry=%.2f", quantity, entryPrice)
		}
	}

	// Fallback to exchange API if local data not found
	if quantity == 0 {
		positions, err := at.trader.GetPositions()
		if err == nil {
			for _, pos := range positions {
				if pos["symbol"] == decision.Symbol && pos["side"] == "short" {
					if ep, ok := pos["entryPrice"].(float64); ok {
						entryPrice = ep
					}
					if amt, ok := pos["positionAmt"].(float64); ok {
						quantity = -amt // positionAmt is negative for short
					}
					break
				}
			}
		}
		logger.Infof("  📊 Using exchange position data: qty=%.8f, entry=%.2f", quantity, entryPrice)
	}

	// Close position
	order, err := at.trader.CloseShort(decision.Symbol, 0) // 0 = close all
	if err != nil {
		return err
	}

	// Record order ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	// Record order to database and poll for confirmation
	at.recordAndConfirmOrder(order, decision.Symbol, "close_short", quantity, marketData.CurrentPrice, 0, entryPrice)

	logger.Infof("  ✓ Position closed successfully")
	return nil
}

// GetID gets trader ID
func (at *AutoTrader) GetID() string {
	return at.id
}

// GetUnderlyingTrader returns the underlying Trader interface implementation
// This is used by grid trading and other components that need direct exchange access
func (at *AutoTrader) GetUnderlyingTrader() Trader {
	return at.trader
}

// GetName gets trader name
func (at *AutoTrader) GetName() string {
	return at.name
}

// GetAIModel gets AI model
func (at *AutoTrader) GetAIModel() string {
	return at.aiModel
}

// GetExchange gets exchange
func (at *AutoTrader) GetExchange() string {
	return at.exchange
}

// GetShowInCompetition returns whether trader should be shown in competition
func (at *AutoTrader) GetShowInCompetition() bool {
	return at.showInCompetition
}

// SetShowInCompetition sets whether trader should be shown in competition
func (at *AutoTrader) SetShowInCompetition(show bool) {
	at.showInCompetition = show
}

// SetCustomPrompt sets custom trading strategy prompt
func (at *AutoTrader) SetCustomPrompt(prompt string) {
	at.customPrompt = prompt
}

// SetOverrideBasePrompt sets whether to override base prompt
func (at *AutoTrader) SetOverrideBasePrompt(override bool) {
	at.overrideBasePrompt = override
}

// GetSystemPromptTemplate gets current system prompt template name (from strategy config)
func (at *AutoTrader) GetSystemPromptTemplate() string {
	if at.strategyEngine != nil {
		config := at.strategyEngine.GetConfig()
		if config.CustomPrompt != "" {
			return "custom"
		}
	}
	return "strategy"
}

// saveEquitySnapshot saves equity snapshot independently (for drawing profit curve, decoupled from AI decision)
func (at *AutoTrader) saveEquitySnapshot(ctx *kernel.Context) {
	if at.store == nil || ctx == nil {
		return
	}

	snapshot := &store.EquitySnapshot{
		TraderID:      at.id,
		Timestamp:     time.Now().UTC(),
		TotalEquity:   ctx.Account.TotalEquity,
		Balance:       ctx.Account.TotalEquity - ctx.Account.UnrealizedPnL,
		UnrealizedPnL: ctx.Account.UnrealizedPnL,
		PositionCount: ctx.Account.PositionCount,
		MarginUsedPct: ctx.Account.MarginUsedPct,
	}

	if err := at.store.Equity().Save(snapshot); err != nil {
		logger.Infof("⚠️ Failed to save equity snapshot: %v", err)
	}
}

// saveDecision saves AI decision log to database (only records AI input/output, for debugging)
func (at *AutoTrader) saveDecision(record *store.DecisionRecord) error {
	if at.store == nil {
		return nil
	}

	at.cycleNumber++
	record.CycleNumber = at.cycleNumber
	record.TraderID = at.id

	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	if err := at.store.Decision().LogDecision(record); err != nil {
		logger.Infof("⚠️ Failed to save decision record: %v", err)
		return err
	}

	logger.Infof("📝 Decision record saved: trader=%s, cycle=%d", at.id, at.cycleNumber)
	return nil
}

// GetStore gets data store (for external access to decision records, etc.)
func (at *AutoTrader) GetStore() *store.Store {
	return at.store
}

// GetStatus gets system status (for API)
func (at *AutoTrader) GetStatus() map[string]interface{} {
	aiProvider := "DeepSeek"
	if at.config.UseQwen {
		aiProvider = "Qwen"
	}

	at.isRunningMutex.RLock()
	isRunning := at.isRunning
	at.isRunningMutex.RUnlock()

	result := map[string]interface{}{
		"trader_id":       at.id,
		"trader_name":     at.name,
		"ai_model":        at.aiModel,
		"exchange":        at.exchange,
		"is_running":      isRunning,
		"start_time":      at.startTime.Format(time.RFC3339),
		"runtime_minutes": int(time.Since(at.startTime).Minutes()),
		"call_count":      at.callCount,
		"initial_balance": at.initialBalance,
		"scan_interval":   at.config.ScanInterval.String(),
		"stop_until":      at.stopUntil.Format(time.RFC3339),
		"last_reset_time": at.lastResetTime.Format(time.RFC3339),
		"ai_provider":     aiProvider,
	}

	// Add strategy info
	if at.config.StrategyConfig != nil {
		result["strategy_type"] = at.config.StrategyConfig.StrategyType
		if at.config.StrategyConfig.GridConfig != nil {
			result["grid_symbol"] = at.config.StrategyConfig.GridConfig.Symbol
		}
	}

	return result
}

// GetAccountInfo gets account information (for API)
func (at *AutoTrader) GetAccountInfo() (map[string]interface{}, error) {
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}

	// Get account fields
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0
	totalEquity := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Use totalEquity directly if provided by trader (more accurate)
	if eq, ok := balance["totalEquity"].(float64); ok && eq > 0 {
		totalEquity = eq
	} else {
		// Fallback: Total Equity = Wallet balance + Unrealized profit
		totalEquity = totalWalletBalance + totalUnrealizedProfit
	}

	// Get positions to calculate total margin
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	totalMarginUsed := 0.0
	totalUnrealizedPnLCalculated := 0.0
	for _, pos := range positions {
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		totalUnrealizedPnLCalculated += unrealizedPnl

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev + 0.5)
		}
		marginUsed := (quantity * markPrice) / float64(leverage)
		totalMarginUsed += marginUsed
	}

	// Verify unrealized P&L consistency (API value vs calculated from positions)
	// Note: Lighter API may return 0 for unrealized PnL, this is a known limitation
	diff := math.Abs(totalUnrealizedProfit - totalUnrealizedPnLCalculated)
	if diff > 5.0 { // Only warn if difference is significant (> 5 USDT)
		logger.Infof("⚠️ Unrealized P&L inconsistency (Lighter API limitation): API=%.4f, Calculated=%.4f, Diff=%.4f",
			totalUnrealizedProfit, totalUnrealizedPnLCalculated, diff)
	}

	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	} else {
		logger.Infof("⚠️ Initial Balance abnormal: %.2f, cannot calculate P&L percentage", at.initialBalance)
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	return map[string]interface{}{
		// Core fields
		"total_equity":      totalEquity,           // Account equity = wallet + unrealized
		"wallet_balance":    totalWalletBalance,    // Wallet balance (excluding unrealized P&L)
		"unrealized_profit": totalUnrealizedProfit, // Unrealized P&L (official value from exchange API)
		"available_balance": availableBalance,      // Available balance

		// P&L statistics
		"total_pnl":       totalPnL,          // Total P&L = equity - initial
		"total_pnl_pct":   totalPnLPct,       // Total P&L percentage
		"initial_balance": at.initialBalance, // Initial balance
		"daily_pnl":       at.dailyPnL,       // Daily P&L

		// Position information
		"position_count":          len(positions),  // Position count
		"margin_used":             totalMarginUsed, // Margin used
		"margin_used_pct":         marginUsedPct,   // Margin usage rate
		"account_baseline_age_ms": balance["accountBaselineAgeMs"],
	}, nil
}

// GetPositions gets position list (for API)
func (at *AutoTrader) GetPositions() ([]map[string]interface{}, error) {
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	var result []map[string]interface{}
	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev + 0.5)
		}

		// Calculate margin used
		marginUsed := (quantity * markPrice) / float64(leverage)

		// Calculate P&L percentage (based on margin)
		pnlPct := calculatePnLPercentage(unrealizedPnl, marginUsed)

		result = append(result, map[string]interface{}{
			"symbol":               symbol,
			"side":                 side,
			"entry_price":          entryPrice,
			"mark_price":           markPrice,
			"quantity":             quantity,
			"leverage":             leverage,
			"unrealized_pnl":       unrealizedPnl,
			"unrealized_pnl_pct":   pnlPct,
			"liquidation_price":    liquidationPrice,
			"margin_used":          marginUsed,
			"position_data_source": pos["position_data_source"],
		})
	}

	return result, nil
}

// calculatePnLPercentage calculates P&L percentage (based on margin, automatically considers leverage)
// Return rate = Unrealized P&L / Margin × 100%
func calculatePnLPercentage(unrealizedPnl, marginUsed float64) float64 {
	if marginUsed > 0 {
		return (unrealizedPnl / marginUsed) * 100
	}
	return 0.0
}

// sortDecisionsByPriority sorts decisions: close positions first, then open positions, finally hold/wait
// This avoids position stacking overflow when changing positions
func sortDecisionsByPriority(decisions []kernel.Decision) []kernel.Decision {
	if len(decisions) <= 1 {
		return decisions
	}

	// Define priority
	getActionPriority := func(action string) int {
		switch action {
		case "close_long", "close_short":
			return 1 // Highest priority: close positions first
		case "cancel_order", "cancel_all_orders":
			return 2 // Cancel stale orders before opening new ones
		case "open_long", "open_short", "place_limit_buy", "place_limit_sell":
			return 3 // Open positions later
		case "update_stop_loss", "update_take_profit":
			return 4 // Update risk controls after open/cancel
		case "hold", "wait":
			return 5 // Lowest priority: wait
		default:
			return 999 // Unknown actions at the end
		}
	}

	// Copy decision list
	sorted := make([]kernel.Decision, len(decisions))
	copy(sorted, decisions)

	// Sort by priority
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if getActionPriority(sorted[i].Action) > getActionPriority(sorted[j].Action) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// startDrawdownMonitor starts drawdown monitoring
func (at *AutoTrader) startDrawdownMonitor() {
	at.monitorWg.Add(1)
	go func() {
		defer at.monitorWg.Done()

		ticker := time.NewTicker(1 * time.Minute) // Check every minute
		defer ticker.Stop()

		logger.Info("📊 Started position drawdown monitoring (check every minute)")

		for {
			select {
			case <-ticker.C:
				at.checkPositionDrawdown()
			case <-at.stopMonitorCh:
				logger.Info("⏹ Stopped position drawdown monitoring")
				return
			}
		}
	}()
}

func (at *AutoTrader) startFuturesBalanceGuard() {
	if at.config.Exchange != "binance" {
		return
	}
	if at.config.BinanceTestnet {
		logger.Infof("⚠️ [%s] Futures balance guard disabled on Binance testnet", at.name)
		return
	}
	fundTransferCfg, ok := at.getEnabledFundTransferConfig()
	if !ok {
		logger.Infof("ℹ️ [%s] Futures balance guard disabled: strategy fund_transfer not enabled", at.name)
		return
	}

	if _, loaded := futuresBalanceGuardStarted.LoadOrStore(at.config.ExchangeID, struct{}{}); loaded {
		return
	}

	at.monitorWg.Add(1)
	go func() {
		defer at.monitorWg.Done()
		defer futuresBalanceGuardStarted.Delete(at.config.ExchangeID)

		location, err := time.LoadLocation("Asia/Shanghai")
		if err != nil {
			logger.Infof("⚠️ [%s] Failed to load Beijing time zone: %v", at.name, err)
			location = time.FixedZone("CST", 8*3600)
		}

		logger.Infof("🏦 [%s] Futures balance guard enabled (mode %s, target %.2f USDT, daily %s Beijing)",
			at.name, fundTransferCfg.Mode, fundTransferCfg.TargetFuturesAvailableBalance, fundTransferCfg.TriggerTime)

		for {
			next, err := nextBeijingTriggerTime(time.Now(), location, fundTransferCfg.TriggerTime)
			if err != nil {
				logger.Infof("⚠️ [%s] Invalid fund_transfer trigger_time %q: %v", at.name, fundTransferCfg.TriggerTime, err)
				return
			}
			wait := time.Until(next)
			timer := time.NewTimer(wait)

			select {
			case <-timer.C:
				at.rebalanceFuturesBalance(fundTransferCfg)
			case <-at.stopMonitorCh:
				timer.Stop()
				logger.Infof("[%s] ⏹ Futures balance guard stopped", at.name)
				return
			}
		}
	}()
}

func (at *AutoTrader) getEnabledFundTransferConfig() (*store.FundTransferConfig, bool) {
	if at.config.StrategyConfig == nil || at.config.StrategyConfig.FundTransfer == nil {
		return nil, false
	}
	ft := *at.config.StrategyConfig.FundTransfer
	if !ft.Enabled {
		return nil, false
	}
	if ft.Mode == "" {
		ft.Mode = store.FundTransferModeFuturesToSpot
	}
	if ft.TriggerTime == "" {
		ft.TriggerTime = "00:00"
	}
	if ft.MinTransferAmount <= 0 {
		ft.MinTransferAmount = 0.01
	}
	if ft.TargetFuturesAvailableBalance < 0 {
		logger.Infof("⚠️ [%s] Invalid fund_transfer target_futures_available_balance %.4f", at.name, ft.TargetFuturesAvailableBalance)
		return nil, false
	}
	switch ft.Mode {
	case store.FundTransferModeFuturesToSpot, store.FundTransferModeBidirectional:
	default:
		logger.Infof("⚠️ [%s] Invalid fund_transfer mode: %s", at.name, ft.Mode)
		return nil, false
	}
	if _, _, err := parseTriggerTimeHHMM(ft.TriggerTime); err != nil {
		logger.Infof("⚠️ [%s] Invalid fund_transfer trigger_time: %s", at.name, ft.TriggerTime)
		return nil, false
	}
	return &ft, true
}

func parseTriggerTimeHHMM(triggerTime string) (int, int, error) {
	if len(triggerTime) != 5 || triggerTime[2] != ':' {
		return 0, 0, fmt.Errorf("expected HH:mm")
	}
	hour, err := strconv.Atoi(triggerTime[:2])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hour")
	}
	minute, err := strconv.Atoi(triggerTime[3:])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minute")
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, 0, fmt.Errorf("out of range")
	}
	return hour, minute, nil
}

func nextBeijingTriggerTime(now time.Time, location *time.Location, triggerTime string) (time.Time, error) {
	hour, minute, err := parseTriggerTimeHHMM(triggerTime)
	if err != nil {
		return time.Time{}, err
	}
	local := now.In(location)
	year, month, day := local.Date()
	next := time.Date(year, month, day, hour, minute, 0, 0, location)
	if !next.After(local) {
		next = next.Add(24 * time.Hour)
	}
	return next, nil
}

type fundTransferDecision struct {
	direction string
	amount    float64
	reason    string
}

func decideFundTransfer(futuresAvailable, target, minTransfer float64, mode string) fundTransferDecision {
	diff := target - futuresAvailable
	if math.Abs(diff) < minTransfer {
		return fundTransferDecision{reason: "within_threshold"}
	}
	if diff < 0 {
		return fundTransferDecision{
			direction: "futures_to_spot",
			amount:    -diff,
			reason:    "excess_futures_available",
		}
	}
	if mode == store.FundTransferModeBidirectional {
		return fundTransferDecision{
			direction: "spot_to_futures",
			amount:    diff,
			reason:    "futures_available_below_target",
		}
	}
	return fundTransferDecision{reason: "mode_futures_to_spot_only"}
}

func (at *AutoTrader) rebalanceFuturesBalance(ft *store.FundTransferConfig) {
	bt, ok := at.trader.(*FuturesTrader)
	if !ok {
		logger.Infof("⚠️ [%s] Futures balance guard only supports Binance futures", at.name)
		return
	}

	futuresAvailable, err := bt.GetFuturesAvailableBalanceUSDT()
	if err != nil {
		logger.Infof("⚠️ [%s] Failed to get futures available balance: %v", at.name, err)
		return
	}
	decision := decideFundTransfer(futuresAvailable, ft.TargetFuturesAvailableBalance, ft.MinTransferAmount, ft.Mode)
	if decision.direction == "" {
		logger.Infof("🏦 [%s] Fund transfer skipped (%s): futures available %.2f, target %.2f, min %.2f, mode %s",
			at.name, decision.reason, futuresAvailable, ft.TargetFuturesAvailableBalance, ft.MinTransferAmount, ft.Mode)
		return
	}
	if decision.direction == "spot_to_futures" {
		spotAvailable, err := bt.GetSpotUSDTBalance()
		if err != nil {
			logger.Infof("⚠️ [%s] Failed to get spot balance: %v", at.name, err)
			return
		}
		transferAmount := math.Min(decision.amount, spotAvailable)
		if transferAmount <= 0 {
			logger.Infof("⚠️ [%s] Spot balance insufficient: need %.2f, available %.2f", at.name, decision.amount, spotAvailable)
			return
		}
		if err := bt.TransferUSDTSpotToFutures(transferAmount); err != nil {
			logger.Infof("❌ [%s] Spot→Futures transfer failed: %v", at.name, err)
			return
		}
		logger.Infof("✅ [%s] Spot→Futures transfer success: %.2f USDT (futures available %.2f -> target %.2f)",
			at.name, transferAmount, futuresAvailable, ft.TargetFuturesAvailableBalance)
		return
	}
	transferAmount := math.Min(decision.amount, futuresAvailable)
	if transferAmount <= 0 {
		logger.Infof("⚠️ [%s] Futures available balance insufficient: need %.2f, available %.2f", at.name, decision.amount, futuresAvailable)
		return
	}
	if err := bt.TransferUSDTFuturesToSpot(transferAmount); err != nil {
		logger.Infof("❌ [%s] Futures→Spot transfer failed: %v", at.name, err)
		return
	}
	logger.Infof("✅ [%s] Futures→Spot transfer success: %.2f USDT (futures available %.2f -> target %.2f)",
		at.name, transferAmount, futuresAvailable, ft.TargetFuturesAvailableBalance)
}
func (at *AutoTrader) getTakeProfitMapForPositions(positions []map[string]interface{}) map[string]float64 {
	if len(positions) == 0 {
		return nil
	}

	symbolSet := make(map[string]bool)
	for _, pos := range positions {
		if symbol, ok := pos["symbol"].(string); ok && symbol != "" {
			symbolSet[market.Normalize(symbol)] = true
		}
	}

	if len(symbolSet) == 0 {
		return nil
	}

	openOrders := at.getOpenOrdersRawForSymbols(symbolSet)
	if len(openOrders) == 0 {
		return nil
	}

	_, tpMap := buildStopTargetMaps(openOrders)
	if len(tpMap) == 0 {
		return nil
	}

	return tpMap
}

// checkPositionDrawdown checks position drawdown situation
func (at *AutoTrader) checkPositionDrawdown() {
	riskControl := at.config.StrategyConfig.RiskControl
	if !riskControl.EnableDrawdownClose {
		return
	}
	progressPctThreshold := riskControl.DrawdownCloseProgressPct
	if progressPctThreshold <= 0 {
		progressPctThreshold = 40
	}
	drawdownPctThreshold := riskControl.DrawdownClosePct
	if drawdownPctThreshold <= 0 {
		drawdownPctThreshold = 40
	}

	// Get current positions
	positions, err := at.trader.GetPositions()
	if err != nil {
		logger.Infof("❌ Drawdown monitoring: failed to get positions: %v", err)
		return
	}

	tpMap := at.getTakeProfitMapForPositions(positions)
	if len(tpMap) == 0 {
		logger.Infof("📊 Drawdown monitoring: no take profit orders found, drawdown close disabled this cycle")
		return
	}

	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity // Short position quantity is negative, convert to positive
		}

		tpPrice, ok := tpMap[stopTargetKey(symbol, side)]
		if !ok || tpPrice <= 0 {
			logger.Infof("📊 Drawdown monitoring: %s %s skip (no take profit order)", symbol, side)
			continue
		}

		// Calculate current P&L percentage
		leverage := 10 // Default value
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev + 0.5)
		}

		var currentPnLPct float64
		if side == "long" {
			currentPnLPct = ((markPrice - entryPrice) / entryPrice) * float64(leverage) * 100
		} else {
			currentPnLPct = ((entryPrice - markPrice) / entryPrice) * float64(leverage) * 100
		}

		progress := 0.0
		if side == "long" {
			denom := tpPrice - entryPrice
			if denom <= 0 {
				logger.Infof("📊 Drawdown monitoring: %s %s skip (invalid take profit price)", symbol, side)
				continue
			}
			progress = (markPrice - entryPrice) / denom
		} else {
			denom := entryPrice - tpPrice
			if denom <= 0 {
				logger.Infof("📊 Drawdown monitoring: %s %s skip (invalid take profit price)", symbol, side)
				continue
			}
			progress = (entryPrice - markPrice) / denom
		}
		if progress*100 < progressPctThreshold {
			continue
		}

		// Construct unique position identifier (distinguish long/short)
		posKey := symbol + "_" + side

		// Get historical peak profit for this position
		at.peakPnLCacheMutex.RLock()
		peakPnLPct, exists := at.peakPnLCache[posKey]
		at.peakPnLCacheMutex.RUnlock()

		if !exists {
			// If no historical peak record, use current P&L as initial value
			peakPnLPct = currentPnLPct
			at.UpdatePeakPnL(symbol, side, currentPnLPct)
		} else {
			// Update peak cache
			at.UpdatePeakPnL(symbol, side, currentPnLPct)
		}

		// Calculate drawdown (magnitude of decline from peak)
		var drawdownPct float64
		if peakPnLPct > 0 && currentPnLPct < peakPnLPct {
			drawdownPct = ((peakPnLPct - currentPnLPct) / peakPnLPct) * 100
		}

		// Check close position condition: progress >= threshold of TP target and drawdown >= threshold
		if drawdownPct >= drawdownPctThreshold {
			logger.Infof("🚨 Drawdown close position condition triggered: %s %s | Current profit: %.2f%% | Peak profit: %.2f%% | Drawdown: %.2f%% | TP progress: %.2f%%",
				symbol, side, currentPnLPct, peakPnLPct, drawdownPct, progress*100)

			// Execute close position
			if err := at.emergencyClosePosition(symbol, side); err != nil {
				logger.Infof("❌ Drawdown close position failed (%s %s): %v", symbol, side, err)
			} else {
				logger.Infof("✅ Drawdown close position succeeded: %s %s", symbol, side)
				// Clear cache for this position after closing
				at.ClearPeakPnLCache(symbol, side)
			}
		} else if currentPnLPct > 0 {
			// Record situations close to close position condition (for debugging)
			logger.Infof("📊 Drawdown monitoring: %s %s | Profit: %.2f%% | Peak: %.2f%% | Drawdown: %.2f%% | TP progress: %.2f%%",
				symbol, side, currentPnLPct, peakPnLPct, drawdownPct, progress*100)
		}
	}
}

// emergencyClosePosition emergency close position function
func (at *AutoTrader) emergencyClosePosition(symbol, side string) error {
	switch side {
	case "long":
		order, err := at.trader.CloseLong(symbol, 0) // 0 = close all
		if err != nil {
			return err
		}
		logger.Infof("✅ Emergency close long position succeeded, order ID: %v", order["orderId"])
	case "short":
		order, err := at.trader.CloseShort(symbol, 0) // 0 = close all
		if err != nil {
			return err
		}
		logger.Infof("✅ Emergency close short position succeeded, order ID: %v", order["orderId"])
	default:
		return fmt.Errorf("unknown position direction: %s", side)
	}

	return nil
}

// GetPeakPnLCache gets peak profit cache
func (at *AutoTrader) GetPeakPnLCache() map[string]float64 {
	at.peakPnLCacheMutex.RLock()
	defer at.peakPnLCacheMutex.RUnlock()

	// Return a copy of the cache
	cache := make(map[string]float64)
	for k, v := range at.peakPnLCache {
		cache[k] = v
	}
	return cache
}

// UpdatePeakPnL updates peak profit cache
func (at *AutoTrader) UpdatePeakPnL(symbol, side string, currentPnLPct float64) {
	at.peakPnLCacheMutex.Lock()
	defer at.peakPnLCacheMutex.Unlock()

	posKey := symbol + "_" + side
	if peak, exists := at.peakPnLCache[posKey]; exists {
		// Update peak (if long, take larger value; if short, currentPnLPct is negative, also compare)
		if currentPnLPct > peak {
			at.peakPnLCache[posKey] = currentPnLPct
		}
	} else {
		// First time recording
		at.peakPnLCache[posKey] = currentPnLPct
	}
}

// ClearPeakPnLCache clears peak cache for specified position
func (at *AutoTrader) ClearPeakPnLCache(symbol, side string) {
	at.peakPnLCacheMutex.Lock()
	defer at.peakPnLCacheMutex.Unlock()

	posKey := symbol + "_" + side
	delete(at.peakPnLCache, posKey)
}

// recordAndConfirmOrder polls order status for actual fill data and records position
// action: open_long, open_short, close_long, close_short
// entryPrice: entry price when closing (0 when opening)
func (at *AutoTrader) recordAndConfirmOrder(orderResult map[string]interface{}, symbol, action string, quantity float64, price float64, leverage int, entryPrice float64) {
	if at.store == nil {
		return
	}

	// Get order ID (supports multiple types)
	var orderID string
	switch v := orderResult["orderId"].(type) {
	case int64:
		orderID = fmt.Sprintf("%d", v)
	case float64:
		orderID = fmt.Sprintf("%.0f", v)
	case string:
		orderID = v
	default:
		orderID = fmt.Sprintf("%v", v)
	}

	if orderID == "" || orderID == "0" {
		logger.Infof("  ⚠️ Order ID is empty, skipping record")
		return
	}

	// Determine positionSide
	var positionSide string
	switch action {
	case "open_long", "close_long":
		positionSide = "LONG"
	case "open_short", "close_short":
		positionSide = "SHORT"
	}

	var actualPrice = price
	var actualQty = quantity
	var fee float64

	// Exchanges with OrderSync: Skip immediate order recording, let OrderSync handle it
	// This ensures accurate data from GetTrades API and avoids duplicate records
	switch at.exchange {
	case "binance", "lighter", "hyperliquid", "bybit", "okx", "bitget", "aster":
		logger.Infof("  📝 Order submitted (id: %s), will be synced by OrderSync", orderID)
		return
	}

	// For exchanges without OrderSync (e.g., Binance): record immediately and poll for fill data
	orderRecord := at.createOrderRecord(orderID, symbol, action, positionSide, quantity, price, leverage)
	if err := at.store.Order().CreateOrder(orderRecord); err != nil {
		logger.Infof("  ⚠️ Failed to record order: %v", err)
	} else {
		logger.Infof("  📝 Order recorded: %s [%s] %s", orderID, action, symbol)
	}

	// Wait for order to be filled and get actual fill data
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		status, err := at.trader.GetOrderStatus(symbol, orderID)
		if err == nil {
			statusStr, _ := status["status"].(string)
			if statusStr == "FILLED" {
				// Get actual fill price
				if avgPrice, ok := status["avgPrice"].(float64); ok && avgPrice > 0 {
					actualPrice = avgPrice
				}
				// Get actual executed quantity
				if execQty, ok := status["executedQty"].(float64); ok && execQty > 0 {
					actualQty = execQty
				}
				// Get commission/fee
				if commission, ok := status["commission"].(float64); ok {
					fee = commission
				}
				logger.Infof("  ✅ Order filled: avgPrice=%.6f, qty=%.6f, fee=%.6f", actualPrice, actualQty, fee)

				// Update order status to FILLED
				if err := at.store.Order().UpdateOrderStatus(orderRecord.ID, "FILLED", actualQty, actualPrice, fee); err != nil {
					logger.Infof("  ⚠️ Failed to update order status: %v", err)
				}

				// Record fill details
				at.recordOrderFill(orderRecord.ID, orderID, symbol, action, actualPrice, actualQty, fee)
				break
			} else if statusStr == "CANCELED" || statusStr == "EXPIRED" || statusStr == "REJECTED" {
				logger.Infof("  ⚠️ Order %s, skipping position record", statusStr)

				// Update order status
				if err := at.store.Order().UpdateOrderStatus(orderRecord.ID, statusStr, 0, 0, 0); err != nil {
					logger.Infof("  ⚠️ Failed to update order status: %v", err)
				}
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Normalize symbol for position record consistency
	normalizedSymbolForPosition := market.Normalize(symbol)

	logger.Infof("  📝 Recording position (ID: %s, action: %s, price: %.6f, qty: %.6f, fee: %.4f)",
		orderID, action, actualPrice, actualQty, fee)

	// Record position change with actual fill data (use normalized symbol)
	at.recordPositionChange(orderID, normalizedSymbolForPosition, positionSide, action, actualQty, actualPrice, leverage, entryPrice, fee)

	// Send anonymous trade statistics for experience improvement (async, non-blocking)
	// This helps us understand overall product usage across all deployments
	experience.TrackTrade(experience.TradeEvent{
		Exchange:  at.exchange,
		TradeType: action,
		Symbol:    symbol,
		AmountUSD: actualPrice * actualQty,
		Leverage:  leverage,
		UserID:    at.userID,
		TraderID:  at.id,
	})
}

// recordPositionChange records position change (create record on open, update record on close)
func (at *AutoTrader) recordPositionChange(orderID, symbol, side, action string, quantity, price float64, leverage int, entryPrice float64, fee float64) {
	if at.store == nil {
		return
	}

	switch action {
	case "open_long", "open_short":
		// Open position: create new position record
		nowMs := time.Now().UTC().UnixMilli()
		pos := &store.TraderPosition{
			TraderID:     at.id,
			ExchangeID:   at.exchangeID, // Exchange account UUID
			ExchangeType: at.exchange,   // Exchange type: binance/bybit/okx/etc
			Symbol:       symbol,
			Side:         side, // LONG or SHORT
			Quantity:     quantity,
			EntryPrice:   price,
			EntryOrderID: orderID,
			EntryTime:    nowMs,
			Leverage:     leverage,
			Status:       "OPEN",
			CreatedAt:    nowMs,
			UpdatedAt:    nowMs,
		}
		if err := at.store.Position().Create(pos); err != nil {
			logger.Infof("  ⚠️ Failed to record position: %v", err)
		} else {
			logger.Infof("  📊 Position recorded [%s] %s %s @ %.4f", at.id[:8], symbol, side, price)
		}

	case "close_long", "close_short":
		// Close position using PositionBuilder for consistent handling
		// PositionBuilder will handle both cases:
		// 1. If open position exists: close it properly
		// 2. If no open position (e.g., table cleared): create a closed position record
		posBuilder := store.NewPositionBuilder(at.store.Position())
		if err := posBuilder.ProcessTrade(
			at.id, at.exchangeID, at.exchange,
			symbol, side, action,
			quantity, price, fee, 0, // realizedPnL will be calculated
			time.Now().UTC().UnixMilli(), orderID,
		); err != nil {
			logger.Infof("  ⚠️ Failed to process close position: %v", err)
		} else {
			logger.Infof("  ✅ Position closed [%s] %s %s @ %.4f", at.id[:8], symbol, side, price)
		}
	}
}

// createOrderRecord creates an order record struct from order details
func (at *AutoTrader) createOrderRecord(orderID, symbol, action, positionSide string, quantity, price float64, leverage int) *store.TraderOrder {
	// Determine order type (market for auto trader)
	orderType := "MARKET"

	// Determine side (BUY/SELL)
	var side string
	switch action {
	case "open_long", "close_short":
		side = "BUY"
	case "open_short", "close_long":
		side = "SELL"
	}

	// Use action as orderAction directly (keep lowercase format)
	orderAction := action

	// Determine if it's a reduce only order
	reduceOnly := (action == "close_long" || action == "close_short")

	// Normalize symbol for consistency
	normalizedSymbol := market.Normalize(symbol)

	return &store.TraderOrder{
		TraderID:        at.id,
		ExchangeID:      at.exchangeID,
		ExchangeType:    at.exchange,
		ExchangeOrderID: orderID,
		Symbol:          normalizedSymbol,
		Side:            side,
		PositionSide:    positionSide,
		Type:            orderType,
		TimeInForce:     "GTC",
		Quantity:        quantity,
		Price:           price,
		Status:          "NEW",
		FilledQuantity:  0,
		AvgFillPrice:    0,
		Commission:      0,
		CommissionAsset: "USDT",
		Leverage:        leverage,
		ReduceOnly:      reduceOnly,
		ClosePosition:   reduceOnly,
		OrderAction:     orderAction,
		CreatedAt:       time.Now().UTC().UnixMilli(),
		UpdatedAt:       time.Now().UTC().UnixMilli(),
	}
}

// recordOrderFill records order fill/trade details
func (at *AutoTrader) recordOrderFill(orderRecordID int64, exchangeOrderID, symbol, action string, price, quantity, fee float64) {
	if at.store == nil {
		return
	}

	// Determine side (BUY/SELL)
	var side string
	switch action {
	case "open_long", "close_short":
		side = "BUY"
	case "open_short", "close_long":
		side = "SELL"
	}

	// Generate a simple trade ID (exchange doesn't always provide one)
	tradeID := fmt.Sprintf("%s-%d", exchangeOrderID, time.Now().UnixNano())

	// Normalize symbol for consistency
	normalizedSymbol := market.Normalize(symbol)

	fill := &store.TraderFill{
		TraderID:        at.id,
		ExchangeID:      at.exchangeID,
		ExchangeType:    at.exchange,
		OrderID:         orderRecordID,
		ExchangeOrderID: exchangeOrderID,
		ExchangeTradeID: tradeID,
		Symbol:          normalizedSymbol,
		Side:            side,
		Price:           price,
		Quantity:        quantity,
		QuoteQuantity:   price * quantity,
		Commission:      fee,
		CommissionAsset: "USDT",
		RealizedPnL:     0,     // Will be calculated for close orders
		IsMaker:         false, // Market orders are usually taker
		CreatedAt:       time.Now().UTC().UnixMilli(),
	}

	// Calculate realized PnL for close orders
	if action == "close_long" || action == "close_short" {
		// Try to get the entry price from the open position
		var positionSide string
		if action == "close_long" {
			positionSide = "LONG"
		} else {
			positionSide = "SHORT"
		}

		if openPos, err := at.store.Position().GetOpenPositionBySymbol(at.id, symbol, positionSide); err == nil && openPos != nil {
			if positionSide == "LONG" {
				fill.RealizedPnL = (price - openPos.EntryPrice) * quantity
			} else {
				fill.RealizedPnL = (openPos.EntryPrice - price) * quantity
			}
		}
	}

	if err := at.store.Order().CreateFill(fill); err != nil {
		logger.Infof("  ⚠️ Failed to record fill: %v", err)
	} else {
		logger.Infof("  📋 Fill recorded: %.4f @ %.6f, fee: %.4f", quantity, price, fee)
	}
}

func pendingLimitOrderKey(orderID, clientID, symbol string) string {
	if orderID != "" {
		return "oid:" + orderID
	}
	if clientID != "" {
		return "cid:" + clientID
	}
	return fmt.Sprintf("sym:%s:%d", symbol, time.Now().UnixNano())
}

func (at *AutoTrader) recordPendingLimitOrder(result *LimitOrderResult, req *LimitOrderRequest, stopLoss, takeProfit float64) {
	clientID := result.ClientID
	if clientID == "" {
		clientID = req.ClientID
	}

	key := pendingLimitOrderKey(result.OrderID, clientID, req.Symbol)
	order := &pendingLimitOrder{
		Key:        key,
		OrderID:    result.OrderID,
		ClientID:   clientID,
		Symbol:     req.Symbol,
		Side:       req.Side,
		Price:      req.Price,
		Quantity:   req.Quantity,
		Leverage:   req.Leverage,
		StopLoss:   stopLoss,
		TakeProfit: takeProfit,
		PostOnly:   req.PostOnly,
		ReduceOnly: req.ReduceOnly,
		CreatedAt:  time.Now().UTC(),
	}

	at.pendingLimitOrdersMu.Lock()
	at.pendingLimitOrders[key] = order
	at.pendingLimitOrdersMu.Unlock()
}

func (at *AutoTrader) removePendingLimitOrder(key string) {
	at.pendingLimitOrdersMu.Lock()
	delete(at.pendingLimitOrders, key)
	at.pendingLimitOrdersMu.Unlock()
}

func (at *AutoTrader) listPendingLimitOrders() []*pendingLimitOrder {
	at.pendingLimitOrdersMu.RLock()
	defer at.pendingLimitOrdersMu.RUnlock()

	orders := make([]*pendingLimitOrder, 0, len(at.pendingLimitOrders))
	for _, order := range at.pendingLimitOrders {
		orders = append(orders, order)
	}
	return orders
}

func (at *AutoTrader) hasPendingLimitOrders() bool {
	at.pendingLimitOrdersMu.RLock()
	defer at.pendingLimitOrdersMu.RUnlock()
	return len(at.pendingLimitOrders) > 0
}

func (at *AutoTrader) getPendingLimitOrder(orderID string) *pendingLimitOrder {
	if orderID == "" {
		return nil
	}
	at.pendingLimitOrdersMu.RLock()
	defer at.pendingLimitOrdersMu.RUnlock()

	return at.pendingLimitOrders[pendingLimitOrderKey(orderID, "", "")]
}

func isClosePositionOrderConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "-4130") ||
		strings.Contains(msg, "closePosition") ||
		strings.Contains(msg, "open stop or take profit")
}

func isOrderAlreadyClosed(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "-2011") ||
		strings.Contains(msg, "unknown order") ||
		strings.Contains(msg, "order does not exist") ||
		strings.Contains(msg, "order not found") ||
		strings.Contains(msg, "already filled") ||
		strings.Contains(msg, "order filled") ||
		strings.Contains(msg, "already canceled") ||
		strings.Contains(msg, "already cancelled") ||
		strings.Contains(msg, "already closed")
}

func (at *AutoTrader) cancelClosePositionOrders(symbol string) {
	if err := at.trader.CancelStopLossOrders(symbol); err != nil {
		logger.Warnf("[%s] Failed to cancel stop-loss orders for %s: %v", at.name, symbol, err)
	}
	if err := at.trader.CancelTakeProfitOrders(symbol); err != nil {
		logger.Warnf("[%s] Failed to cancel take-profit orders for %s: %v", at.name, symbol, err)
	}
}

func (at *AutoTrader) setStopLossWithRetry(symbol, positionSide string, qty, stopLoss float64) error {
	if stopLoss <= 0 {
		return nil
	}
	if err := at.trader.SetStopLoss(symbol, positionSide, qty, stopLoss); err != nil {
		if isClosePositionOrderConflict(err) {
			at.cancelClosePositionOrders(symbol)
			return at.trader.SetStopLoss(symbol, positionSide, qty, stopLoss)
		}
		return err
	}
	return nil
}

func (at *AutoTrader) setTakeProfitWithRetry(symbol, positionSide string, qty, takeProfit float64) error {
	if takeProfit <= 0 {
		return nil
	}
	if err := at.trader.SetTakeProfit(symbol, positionSide, qty, takeProfit); err != nil {
		if isClosePositionOrderConflict(err) {
			at.cancelClosePositionOrders(symbol)
			return at.trader.SetTakeProfit(symbol, positionSide, qty, takeProfit)
		}
		return err
	}
	return nil
}

func (at *AutoTrader) syncPendingLimitOrders() {
	if !at.pendingLimitSyncing.CompareAndSwap(false, true) {
		return
	}
	defer at.pendingLimitSyncing.Store(false)

	orders := at.listPendingLimitOrders()
	if len(orders) == 0 {
		return
	}

	for _, order := range orders {
		if order.OrderID == "" {
			logger.Warnf("[%s] Pending limit order missing order ID, removing: %s", at.name, order.Key)
			at.removePendingLimitOrder(order.Key)
			continue
		}

		status, err := at.trader.GetOrderStatus(order.Symbol, order.OrderID)
		if err != nil {
			logger.Warnf("[%s] Failed to query order status (%s): %v", at.name, order.OrderID, err)
			openOrders, openErr := at.trader.GetOpenOrders(order.Symbol)
			if openErr == nil {
				stillOpen := false
				for _, openOrder := range openOrders {
					if openOrder.OrderID == order.OrderID {
						stillOpen = true
						break
					}
				}
				if stillOpen {
					continue
				}
			}
			if qty, ok := at.findPositionQtyForSide(order.Symbol, order.Side); ok {
				positionSide := "LONG"
				if strings.ToUpper(order.Side) == "SELL" {
					positionSide = "SHORT"
				}
				slNeeded := order.StopLoss > 0
				tpNeeded := order.TakeProfit > 0
				slOk := false
				tpOk := false
				if err := at.setStopLossWithRetry(order.Symbol, positionSide, qty, order.StopLoss); err != nil {
					logger.Warnf("[%s] Failed to set stop loss for %s: %v", at.name, order.Symbol, err)
				} else if slNeeded {
					slOk = true
				}
				if err := at.setTakeProfitWithRetry(order.Symbol, positionSide, qty, order.TakeProfit); err != nil {
					logger.Warnf("[%s] Failed to set take profit for %s: %v", at.name, order.Symbol, err)
				} else if tpNeeded {
					tpOk = true
				}
				if (slNeeded && slOk) || (tpNeeded && tpOk) {
					notify.NotifyLimitFill(at.userID, at.id, order.Symbol, order.Side, order.Price, qty)
					at.removePendingLimitOrder(order.Key)
					logger.Infof("[%s] Pending limit order matched position, SL/TP set (SL=%t, TP=%t): %s %s", at.name, slOk, tpOk, order.Symbol, order.OrderID)
				} else {
					at.removePendingLimitOrder(order.Key)
					logger.Warnf("[%s] Pending limit order matched position but SL/TP both failed, tracking removed: %s %s", at.name, order.Symbol, order.OrderID)
				}
				continue
			}

			at.removePendingLimitOrder(order.Key)
			continue
		}

		statusStr, _ := status["status"].(string)
		statusStr = strings.ToUpper(statusStr)
		switch statusStr {
		case "FILLED":
			execQty := order.Quantity
			if qty, ok := status["executedQty"].(float64); ok && qty > 0 {
				execQty = qty
			} else if qtyStr, ok := status["executedQty"].(string); ok {
				if qtyParsed, err := strconv.ParseFloat(qtyStr, 64); err == nil && qtyParsed > 0 {
					execQty = qtyParsed
				}
			}

			positionSide := "LONG"
			if strings.ToUpper(order.Side) == "SELL" {
				positionSide = "SHORT"
			}

			slNeeded := order.StopLoss > 0
			tpNeeded := order.TakeProfit > 0
			slOk := false
			tpOk := false
			if err := at.setStopLossWithRetry(order.Symbol, positionSide, execQty, order.StopLoss); err != nil {
				logger.Warnf("[%s] Failed to set stop loss for %s: %v", at.name, order.Symbol, err)
			} else if slNeeded {
				slOk = true
			}
			if err := at.setTakeProfitWithRetry(order.Symbol, positionSide, execQty, order.TakeProfit); err != nil {
				logger.Warnf("[%s] Failed to set take profit for %s: %v", at.name, order.Symbol, err)
			} else if tpNeeded {
				tpOk = true
			}

			if (slNeeded && slOk) || (tpNeeded && tpOk) {
				notify.NotifyLimitFill(at.userID, at.id, order.Symbol, order.Side, order.Price, execQty)
				at.removePendingLimitOrder(order.Key)
				logger.Infof("[%s] Pending limit order filled, SL/TP set (SL=%t, TP=%t): %s %s", at.name, slOk, tpOk, order.Symbol, order.OrderID)
			} else {
				at.removePendingLimitOrder(order.Key)
				logger.Warnf("[%s] Pending limit order filled but SL/TP both failed, tracking removed: %s %s", at.name, order.Symbol, order.OrderID)
			}
		case "CANCELED", "CANCELLED", "EXPIRED", "REJECTED":
			at.removePendingLimitOrder(order.Key)
			logger.Infof("[%s] Pending limit order closed: %s %s", at.name, order.Symbol, order.OrderID)
		default:
			continue
		}
	}
}

func (at *AutoTrader) startPendingLimitOrderMonitor(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	at.monitorWg.Add(1)
	go func() {
		defer at.monitorWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		logger.Infof("🔄 [%s] Pending limit order sync enabled (every %s)", at.name, interval)
		for {
			select {
			case <-ticker.C:
				if !at.hasPendingLimitOrders() {
					continue
				}
				at.syncPendingLimitOrders()
			case <-at.stopMonitorCh:
				logger.Infof("[%s] ⏹ Pending limit order sync stopped", at.name)
				return
			}
		}
	}()
}

func isLimitOrderType(orderType string) bool {
	orderType = strings.ToLower(orderType)
	if strings.Contains(orderType, "limit") || strings.Contains(orderType, "post") {
		return true
	}
	return false
}

func (at *AutoTrader) getPositionSideAndQuantity(decision *kernel.Decision) (string, float64, error) {
	positionSide := strings.ToUpper(decision.PositionSide)
	if positionSide != "" && positionSide != "LONG" && positionSide != "SHORT" {
		return "", 0, fmt.Errorf("invalid position_side: %s", decision.PositionSide)
	}

	qty := decision.Quantity
	if qty > 0 && positionSide != "" {
		return positionSide, qty, nil
	}

	positions, err := at.trader.GetPositions()
	if err != nil {
		return "", 0, fmt.Errorf("failed to get positions: %w", err)
	}

	var matchedSide string
	var matchedQty float64
	for _, pos := range positions {
		if pos["symbol"] != decision.Symbol {
			continue
		}
		rawSide, ok := pos["side"].(string)
		if !ok {
			continue
		}
		side := ""
		if rawSide == "long" {
			side = "LONG"
		} else if rawSide == "short" {
			side = "SHORT"
		}
		if side == "" {
			continue
		}
		if positionSide != "" && side != positionSide {
			continue
		}

		if qtyVal, ok := pos["positionAmt"].(float64); ok {
			if qtyVal < 0 {
				qtyVal = -qtyVal
			}
			matchedQty = qtyVal
		}

		if matchedSide != "" && matchedSide != side {
			return "", 0, fmt.Errorf("multiple position sides for %s, specify position_side", decision.Symbol)
		}
		matchedSide = side
	}

	if matchedSide == "" {
		return "", 0, fmt.Errorf("no open position found for %s", decision.Symbol)
	}

	if qty <= 0 {
		qty = matchedQty
	}
	if qty <= 0 {
		return "", 0, fmt.Errorf("position quantity unavailable for %s", decision.Symbol)
	}

	return matchedSide, qty, nil
}

func (at *AutoTrader) findPositionQtyForSide(symbol, side string) (float64, bool) {
	positions, err := at.trader.GetPositions()
	if err != nil {
		return 0, false
	}

	targetSide := "long"
	if strings.ToUpper(side) == "SELL" {
		targetSide = "short"
	}

	for _, pos := range positions {
		if pos["symbol"] != symbol {
			continue
		}
		if posSide, ok := pos["side"].(string); ok && posSide == targetSide {
			if qty, ok := pos["positionAmt"].(float64); ok {
				if qty < 0 {
					qty = -qty
				}
				if qty > 0 {
					return qty, true
				}
			}
		}
	}

	return 0, false
}

func (at *AutoTrader) hasOpenLimitOrder(symbol, side string) (bool, string) {
	targetSide := strings.ToUpper(side)

	for _, pending := range at.listPendingLimitOrders() {
		if pending.Symbol == symbol && strings.ToUpper(pending.Side) == targetSide {
			id := pending.OrderID
			if id == "" {
				id = pending.ClientID
			}
			return true, id
		}
	}

	openOrders, err := at.trader.GetOpenOrders(symbol)
	if err != nil {
		logger.Warnf("[%s] Failed to get open orders for %s: %v", at.name, symbol, err)
		return false, ""
	}
	for _, order := range openOrders {
		if !isLimitOrderType(order.Type) {
			continue
		}
		if strings.ToUpper(order.Side) != targetSide {
			continue
		}
		id := order.OrderID
		if id == "" {
			id = order.Symbol
		}
		return true, id
	}

	return false, ""
}

// ============================================================================
// Risk Control Helpers
// ============================================================================

// isBTCETH checks if a symbol is BTC or ETH
func isBTCETH(symbol string) bool {
	symbol = strings.ToUpper(symbol)
	return strings.HasPrefix(symbol, "BTC") || strings.HasPrefix(symbol, "ETH")
}

// enforcePositionValueRatio checks and enforces position value ratio limits (CODE ENFORCED)
// Returns the adjusted position size (capped if necessary) and whether the position was capped
// positionSizeUSD: the original position size in USD
// equity: the account equity
// symbol: the trading symbol
func (at *AutoTrader) enforcePositionValueRatio(positionSizeUSD float64, equity float64, symbol string) (float64, bool) {
	if at.config.StrategyConfig == nil {
		return positionSizeUSD, false
	}

	riskControl := at.config.StrategyConfig.RiskControl

	// Get the appropriate position value ratio limit
	var maxPositionValueRatio float64
	if isBTCETH(symbol) {
		maxPositionValueRatio = riskControl.BTCETHMaxPositionValueRatio
		if maxPositionValueRatio <= 0 {
			maxPositionValueRatio = 5.0 // Default: 5x for BTC/ETH
		}
	} else {
		maxPositionValueRatio = riskControl.AltcoinMaxPositionValueRatio
		if maxPositionValueRatio <= 0 {
			maxPositionValueRatio = 1.0 // Default: 1x for altcoins
		}
	}

	// Calculate max allowed position value = equity × ratio
	maxPositionValue := equity * maxPositionValueRatio

	// Check if position size exceeds limit
	if positionSizeUSD > maxPositionValue {
		logger.Infof("  ⚠️ [RISK CONTROL] Position %.2f USDT exceeds limit (equity %.2f × %.1fx = %.2f USDT max for %s), capping",
			positionSizeUSD, equity, maxPositionValueRatio, maxPositionValue, symbol)
		return maxPositionValue, true
	}

	return positionSizeUSD, false
}

// enforceMinPositionSize checks minimum position size (CODE ENFORCED)
func (at *AutoTrader) enforceMinPositionSize(positionSizeUSD float64) error {
	if at.config.StrategyConfig == nil {
		return nil
	}

	minSize := at.config.StrategyConfig.RiskControl.MinPositionSize
	if minSize <= 0 {
		minSize = 12 // Default: 12 USDT
	}

	if positionSizeUSD < minSize {
		return fmt.Errorf("❌ [RISK CONTROL] Position %.2f USDT below minimum (%.2f USDT)", positionSizeUSD, minSize)
	}
	return nil
}

// enforceMaxPositions checks maximum positions count (CODE ENFORCED)
func (at *AutoTrader) enforceMaxPositions(currentPositionCount int) error {
	if at.config.StrategyConfig == nil {
		return nil
	}

	maxPositions := at.config.StrategyConfig.RiskControl.MaxPositions
	if maxPositions <= 0 {
		maxPositions = 3 // Default: 3 positions
	}

	if currentPositionCount >= maxPositions {
		return fmt.Errorf("❌ [RISK CONTROL] Already at max positions (%d/%d)", currentPositionCount, maxPositions)
	}
	return nil
}

// getSideFromAction converts order action to side (BUY/SELL)
func getSideFromAction(action string) string {
	switch action {
	case "open_long", "close_short":
		return "BUY"
	case "open_short", "close_long":
		return "SELL"
	default:
		return "BUY"
	}
}

// GetOpenOrders returns open orders (pending SL/TP) from exchange
func (at *AutoTrader) GetOpenOrders(symbol string) ([]OpenOrder, error) {
	return at.trader.GetOpenOrders(symbol)
}
