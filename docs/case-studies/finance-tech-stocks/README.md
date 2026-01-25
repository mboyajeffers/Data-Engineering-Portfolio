# Tech Portfolio Analytics: NVDA, MSFT, NFLX

## Overview

Financial analytics pipeline applied to a 3-stock technology portfolio:
- **NVIDIA (NVDA)** - Semiconductor / AI compute
- **Microsoft (MSFT)** - Enterprise software / cloud
- **Netflix (NFLX)** - Digital media / streaming

## Dataset

| Property | Value |
|----------|-------|
| Tickers | NVDA, MSFT, NFLX |
| Period | ~50 trading days |
| Rows | 150 (50 days x 3 tickers) |
| Source | Yahoo Finance (historical OHLCV) |

## Pipeline Stages

### 1. Ingestion
- CSV loading with type coercion
- Date parsing, ticker filtering, sorting

### 2. Validation
- Schema contract (7 required columns)
- Price consistency (high >= low)
- Duplicate detection (ticker + date)
- Statistical bounds (returns within -50% to +100%)

### 3. Transformation
- Daily returns, 20-day volatility
- Moving averages (20/50-day)
- Volume averages, intraday range

### 4. KPI Computation

#### Portfolio-Level
| KPI | Method |
|-----|--------|
| Total Return | Cumulative product of (1+r) |
| Annualized Return | Geometric scaling to 252 days |
| Volatility | std(returns) * sqrt(252) |
| Sharpe Ratio | (excess return) / std, annualized |
| Sortino Ratio | Downside deviation only |
| Max Drawdown | Peak-to-trough |
| VaR (95%) | 5th percentile of daily returns |
| Avg Correlation | Upper triangle of correlation matrix |

#### Per-Ticker
| KPI | Unit |
|-----|------|
| Total Return | % |
| Annualized Volatility | % |
| Sharpe Ratio | ratio |
| Max Drawdown | % |
| Avg Daily Volume | shares |
| Price High/Low | USD |

## Technical Notes

- Risk-free rate: 5% (T-bill proxy)
- Trading days: 252/year (US convention)
- Sharpe uses sample std (ddof=1)
- VaR: historical non-parametric
- Equal-weight portfolio (no drift adjustment)
