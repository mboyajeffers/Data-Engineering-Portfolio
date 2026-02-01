#!/bin/bash
# FIN-01: Major Bank Stock Analysis Runner
# Run this script to pull data and generate reports

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/Finance"

echo "=============================================="
echo "FIN-01: Major Bank Stock Analysis"
echo "=============================================="
echo ""

# Check for required packages
echo "Checking dependencies..."
pip3 install -q yfinance pandas numpy weasyprint 2>/dev/null || pip install -q yfinance pandas numpy weasyprint

# Step 1: Pull data
echo ""
echo "Step 1: Pulling data from Yahoo Finance..."
python3 FIN01_pull_data.py

# Step 2: Generate reports
echo ""
echo "Step 2: Generating reports..."
python3 FIN01_generate_reports.py

echo ""
echo "=============================================="
echo "FIN-01 Complete!"
echo "=============================================="
echo ""
echo "Outputs:"
echo "  - data/FIN01_stock_data.csv"
echo "  - data/FIN01_correlations.csv"
echo "  - data/FIN01_risk_metrics.csv"
echo "  - data/FIN01_summary.json"
echo "  - reports/FIN01_Executive_Summary_v1.0.pdf"
echo "  - reports/FIN01_Technical_Analysis_v1.0.pdf"
echo "  - posts/FIN01_LinkedIn_Post_v1.0.md"
