# Meme Engine Architecture

## Overview
Multi-chain meme coin trading engine that scouts early opportunities, apes in, and scales out with a hybrid exit strategy.

## Supported Chains
| Chain | DEX | New Pair Detection | Swap Router |
|-------|-----|-------------------|-------------|
| Solana | Pump.fun / PumpSwap / Raydium / Jupiter | WebSocket logsSubscribe + PumpPortal API | Jupiter Aggregator |
| Base | Uniswap V3 / Aerodrome | Uniswap V3 Events + DexScreener | 1inch / Paraswap |
| Robinhood Chain | Uniswap (Arbitrum L2) | Uniswap V3 Events | 1inch |

## Modules

### 1. Scout Module (`src/scout/`)
Detects early coin opportunities via three signals:

**new_pair_detector.py** - Listens for newly created liquidity pools
- Solana: `logsSubscribe` on Pump.fun program `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
- Base/Robinhood: Uniswap V3 `PairCreated` events
- DexScreener new pairs API (fallback)

**volume_scanner.py** - Flags tokens with sudden volume spikes
- Polls Birdeye/DexScreener for volume data
- Detects 5x-50x volume increases vs 1h average
- Tracks buy/sell ratio for momentum

**smart_money_tracker.py** - Follows profitable wallets
- Monitors known profitable wallets (configurable list)
- Detects when smart money buys a new token
- Cross-references with new pair data for confirmation

### 2. Execution Module (`src/execution/`)
Handles buy/sell across chains:

**swap_executor.py** - Executes trades
- Solana: Jupiter API v6 quote + swap
- Base: 1inch / Uniswap Router
- Robinhood Chain: 1inch (Arbitrum)

**slippage_guard.py** - Protects against MEV
- Calculates optimal slippage per token
- Monitors pending transactions for sandwich attacks

### 3. Exit Module (`src/exit/`)
Hybrid exit strategy:

**tiered_exits.py** - Scale out at profit milestones
- Configurable tiers (default: 2x/25%, 5x/25%, 10x/25%, hold 25%)
- Each tier sells a % of remaining position

**trailing_stop.py** - Follow price up, sell on drop
- Tracks peak price after each tier exit
- Triggers sell when price drops X% from peak (configurable, default 20%)

**time_exits.py** - Time-based exits
- Emergency exit if position is down after X hours
- Configurable max hold time

### 4. Portfolio Module (`src/portfolio/`)
Tracks positions and performance:

**position_tracker.py** - Tracks open positions
- Entry price, size, current PnL
- Per-chain position aggregation

**pnl_logger.py** - Logs all trades
- SQLite database for trade history
- Win rate, average hold time, best/worst trades

## Config (`config/config.yaml`)
```yaml
chains:
  solana:
    enabled: true
    rpc_url: "https://api.mainnet-beta.solana.com"
    ws_url: "wss://api.mainnet-beta.solana.com"
    private_key: "${SOLANA_PRIVATE_KEY}"
    jupiter_api: "https://quote-api.jup.ag/v6"
    
  base:
    enabled: true
    rpc_url: "https://mainnet.base.org"
    private_key: "${BASE_PRIVATE_KEY}"
    uniswap_router: "0x..."
    
  robinhood:
    enabled: false  # Enable when meme coins activity picks up
    rpc_url: "https://..."
    private_key: "${ROBINHOOD_PRIVATE_KEY}"

scout:
  new_pair:
    enabled: true
    min_liquidity_usd: 5000
    max_buy_tax: 10
    max_sell_tax: 10
    require_verified_contract: false
    
  volume_scan:
    enabled: true
    scan_interval_seconds: 30
    volume_spike_threshold: 5  # 5x average
    min_volume_usd: 10000
    
  smart_money:
    enabled: true
    tracked_wallets:
      - "0x..."  # Add profitable wallets
    min_confidence: 0.7

execution:
  default_buy_amount_usd: 100
  max_buy_amount_usd: 500
  max_slippage_percent: 5
  gas_price_multiplier: 1.2
  retry_attempts: 3
  confirmation_blocks:
    solana: 1
    base: 2
    robinhood: 2

exit:
  tiers:
    - multiplier: 2
      sell_percent: 25
    - multiplier: 5
      sell_percent: 25
    - multiplier: 10
      sell_percent: 25
    # Remaining 25% is the moon bag
    
  trailing_stop:
    enabled: true
    callback_percent: 20  # Sell if price drops 20% from peak
    
  time_exit:
    enabled: true
    max_hold_hours: 24
    emergency_exit_loss_percent: 50  # Cut loss at -50%

portfolio:
  max_concurrent_positions: 10
  min_position_size_usd: 10
  database_path: "data/trades.db"
```

## Data Flow
```
Scout detects token
    ↓
Filter (tax, liquidity, contract safety)
    ↓
Execute buy (Jupiter/1inch/Uniswap)
    ↓
Track position (entry price, size)
    ↓
Monitor price loop
    ↓
Exit check:
  - Tier hit? → Sell portion
  - Trailing stop hit? → Sell remaining
  - Time exit hit? → Emergency exit
    ↓
Log trade to database
```

## API Keys Needed
- **Solana**: Helius/QuickNode RPC, PumpPortal API (optional)
- **Base**: Alchemy/Infura RPC, DexScreener API (free)
- **Data**: Birdeye API (volume), DexScreener API (pairs)
- **Execution**: Jupiter (free), 1inch (free tier)
