#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Betting Analytics Reports
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026

Data Source: TheSportsDB API + Derived Betting Metrics
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from weasyprint import HTML, CSS
from jinja2 import Template
import os

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Betting"
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"
DATE = datetime.now().strftime("%B %d, %Y")

# TheSportsDB API
API_KEY = "3"
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

LEAGUES = {
    'Premier League': 4328,
    'NFL': 4391,
    'NBA': 4387
}

# ============================================================================
# CSS
# ============================================================================

BASE_CSS = """
@page { size: letter; margin: 0.75in; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.5; }
.header { border-bottom: 3px solid #10b981; padding-bottom: 10px; margin-bottom: 20px; }
.header h1 { color: #1a1a2e; margin: 0; font-size: 28px; }
.header .subtitle { color: #666; font-size: 14px; margin-top: 5px; }
.header .date { color: #999; font-size: 12px; float: right; }
.kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }
.kpi-card { background: #ecfdf5; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #10b981; }
.kpi-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
.kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; margin: 8px 0; }
.kpi-card .value.profit { color: #10b981; }
.kpi-card .value.loss { color: #ef4444; }
.kpi-card .value.neutral { color: #6b7280; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
th { background: #10b981; color: white; padding: 12px 10px; text-align: left; }
td { padding: 10px; border-bottom: 1px solid #eee; }
tr:nth-child(even) { background: #ecfdf5; }
.section { margin: 30px 0; }
.section h2 { color: #10b981; font-size: 18px; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.highlight-box { background: #d1fae5; border-left: 4px solid #059669; padding: 15px; margin: 15px 0; }
.highlight-box.warning { background: #fef3c7; border-color: #f59e0b; }
.methodology { background: #f8f9fa; padding: 15px; border-radius: 8px; font-size: 12px; color: #666; margin-top: 30px; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; font-size: 11px; color: #999; }
.footer .author { color: #10b981; font-weight: bold; }
.positive { color: #10b981; font-weight: bold; }
.negative { color: #ef4444; }
"""

REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{{ title }}</title></head>
<body>
<div class="header">
    <span class="date">{{ date }}</span>
    <h1>{{ title }}</h1>
    <div class="subtitle">{{ subtitle }}</div>
</div>
{{ content }}
<div class="footer">
    Report prepared by <span class="author">{{ author }}</span> | {{ date }}<br>
    Data Source: {{ data_source }}
</div>
</body>
</html>
"""

# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_league_standings(league_id, season='2024-2025'):
    url = f"{BASE_URL}/lookuptable.php?l={league_id}&s={season}"
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        return data.get('table', [])
    except:
        return []

def fetch_past_events(league_id):
    url = f"{BASE_URL}/eventspastleague.php?id={league_id}"
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        return data.get('events', [])
    except:
        return []

# ============================================================================
# BETTING CALCULATIONS
# ============================================================================

def compute_implied_probability(standings):
    """Convert team records to implied win probabilities"""
    results = []
    for team in standings:
        try:
            played = int(team.get('intPlayed', 0))
            wins = int(team.get('intWin', 0))
            draws = int(team.get('intDraw', 0))
            losses = int(team.get('intLoss', 0))

            if played > 0:
                win_pct = wins / played
                # Convert to implied probability (simplified)
                # Add regression to mean for stability
                implied_prob = (win_pct * played + 0.33 * 10) / (played + 10)

                # Convert to American odds
                if implied_prob >= 0.5:
                    american_odds = -100 * implied_prob / (1 - implied_prob)
                else:
                    american_odds = 100 * (1 - implied_prob) / implied_prob

                results.append({
                    'team': team.get('strTeam', 'Unknown'),
                    'played': played,
                    'wins': wins,
                    'losses': losses,
                    'win_pct': win_pct * 100,
                    'implied_prob': implied_prob * 100,
                    'american_odds': american_odds,
                    'decimal_odds': 1 / implied_prob if implied_prob > 0 else 0
                })
        except (ValueError, TypeError):
            continue

    return sorted(results, key=lambda x: x['implied_prob'], reverse=True)

def analyze_home_away_edge(events):
    """Analyze home/away performance for betting edge"""
    home_wins = 0
    away_wins = 0
    draws = 0
    total = 0

    for event in events:
        try:
            home_score = int(event.get('intHomeScore', 0))
            away_score = int(event.get('intAwayScore', 0))
            total += 1
            if home_score > away_score:
                home_wins += 1
            elif away_score > home_score:
                away_wins += 1
            else:
                draws += 1
        except (ValueError, TypeError):
            continue

    return {
        'total_games': total,
        'home_wins': home_wins,
        'away_wins': away_wins,
        'draws': draws,
        'home_win_pct': home_wins / total * 100 if total > 0 else 0,
        'away_win_pct': away_wins / total * 100 if total > 0 else 0,
        'draw_pct': draws / total * 100 if total > 0 else 0
    }

def simulate_kelly_bankroll(events, initial_bankroll=1000, kelly_fraction=0.25):
    """Simulate bankroll using Kelly criterion on home favorites"""
    bankroll = initial_bankroll
    history = [bankroll]
    bets = []

    for event in events[:30]:  # Last 30 games
        try:
            home_score = int(event.get('intHomeScore', 0))
            away_score = int(event.get('intAwayScore', 0))

            # Simple strategy: bet on home team with edge
            implied_home_prob = 0.45  # Market price
            true_home_prob = 0.48    # Our estimate (slight edge)
            odds = 1 / implied_home_prob  # ~2.22

            # Kelly formula: f = (bp - q) / b
            b = odds - 1
            p = true_home_prob
            q = 1 - p
            kelly_bet = (b * p - q) / b if b > 0 else 0
            kelly_bet = max(0, min(kelly_bet * kelly_fraction, 0.1))  # Cap at 10%

            bet_amount = bankroll * kelly_bet
            home_won = home_score > away_score

            if home_won:
                profit = bet_amount * (odds - 1)
            else:
                profit = -bet_amount

            bankroll += profit
            history.append(bankroll)
            bets.append({
                'game': f"{event.get('strHomeTeam', 'H')} vs {event.get('strAwayTeam', 'A')}",
                'bet': bet_amount,
                'result': 'Win' if home_won else 'Loss',
                'profit': profit,
                'bankroll': bankroll
            })
        except:
            continue

    return {
        'final_bankroll': bankroll,
        'total_return': (bankroll / initial_bankroll - 1) * 100,
        'max_drawdown': (min(history) / initial_bankroll - 1) * 100 if history else 0,
        'win_rate': sum(1 for b in bets if b['result'] == 'Win') / len(bets) * 100 if bets else 0,
        'bets': bets[:10]  # First 10 for display
    }

def identify_public_teams(standings):
    """Identify high-profile teams that attract public betting"""
    # These typically have name recognition, large fanbases
    public_teams = []
    for team in standings:
        team_name = team.get('strTeam', '')
        # Simple heuristic: top teams and historically popular
        popular_keywords = ['United', 'City', 'Arsenal', 'Liverpool', 'Chelsea',
                          'Lakers', 'Celtics', 'Warriors', 'Cowboys', 'Patriots']
        is_public = any(kw in team_name for kw in popular_keywords)
        try:
            rank = int(team.get('intRank', 99))
            wins = int(team.get('intWin', 0))
            played = int(team.get('intPlayed', 1))
            win_pct = wins / played * 100

            public_teams.append({
                'team': team_name,
                'rank': rank,
                'win_pct': win_pct,
                'is_public_team': is_public or rank <= 5,
                'public_rating': 'High' if is_public else 'Medium' if rank <= 10 else 'Low'
            })
        except:
            continue

    return sorted(public_teams, key=lambda x: (not x['is_public_team'], x['rank']))

# ============================================================================
# REPORT 1: IMPLIED PROBABILITY MODEL
# ============================================================================

def generate_implied_prob_report(standings, league_name):
    """Generate Implied Probability Model PDF"""
    print(f"\n🎰 Generating {league_name} Implied Probability Model...")

    if not standings:
        return

    probs = compute_implied_probability(standings)
    if not probs:
        return

    avg_prob = np.mean([p['implied_prob'] for p in probs])

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Top Favorite</div>
            <div class="value" style="font-size: 18px;">{probs[0]['team']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Implied Win Prob</div>
            <div class="value">{probs[0]['implied_prob']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Decimal Odds</div>
            <div class="value">{probs[0]['decimal_odds']:.2f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Teams Analyzed</div>
            <div class="value">{len(probs)}</div>
        </div>
    </div>

    <div class="section">
        <h2>Win Probability Rankings</h2>
        <table>
            <tr>
                <th>Team</th>
                <th>Record</th>
                <th>Win %</th>
                <th>Implied Prob</th>
                <th>Decimal Odds</th>
                <th>American Odds</th>
            </tr>
    """

    for p in probs[:12]:
        record = f"{p['wins']}-{p['losses']}"
        odds_class = 'positive' if p['american_odds'] < 0 else 'negative'
        content += f"""
            <tr>
                <td><strong>{p['team']}</strong></td>
                <td>{record}</td>
                <td>{p['win_pct']:.1f}%</td>
                <td>{p['implied_prob']:.1f}%</td>
                <td>{p['decimal_odds']:.2f}</td>
                <td class="{odds_class}">{p['american_odds']:+.0f}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Odds Conversion Reference</h2>
        <table>
            <tr>
                <th>Implied Probability</th>
                <th>Decimal Odds</th>
                <th>American Odds</th>
                <th>Interpretation</th>
            </tr>
            <tr><td>80%</td><td>1.25</td><td>-400</td><td>Heavy favorite</td></tr>
            <tr><td>66%</td><td>1.50</td><td>-200</td><td>Clear favorite</td></tr>
            <tr><td>50%</td><td>2.00</td><td>+100</td><td>Even money (coin flip)</td></tr>
            <tr><td>33%</td><td>3.00</td><td>+200</td><td>Underdog</td></tr>
            <tr><td>20%</td><td>5.00</td><td>+400</td><td>Long shot</td></tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Model Insights:</strong>
        <ul>
            <li>Implied probabilities derived from season record with Bayesian smoothing</li>
            <li>Top team ({probs[0]['team']}) has {probs[0]['implied_prob']:.1f}% implied win probability</li>
            <li>Value opportunities exist when market odds diverge from implied probabilities</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Win probability = (Wins × Games + Prior × 10) / (Games + 10).
        Prior of 33% used for regression to mean. American odds: favorite = -100×p/(1-p), underdog = 100×(1-p)/p.
        Decimal odds = 1/probability. Data from TheSportsDB current season standings.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Implied Probability Model",
        subtitle="Win Probability & Odds Conversion",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API + Statistical Model",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Implied_Probability.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 2: HOME/AWAY EDGE ANALYSIS
# ============================================================================

def generate_home_away_report(events, league_name):
    """Generate Home/Away Edge Analysis PDF"""
    print(f"\n🎰 Generating {league_name} Home/Away Edge Analysis...")

    if not events:
        return

    edge = analyze_home_away_edge(events)
    expected_home = 40  # Typical market expectation

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Home Win Rate</div>
            <div class="value {'positive' if edge['home_win_pct'] > expected_home else 'neutral'}">{edge['home_win_pct']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Away Win Rate</div>
            <div class="value">{edge['away_win_pct']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Draw Rate</div>
            <div class="value">{edge['draw_pct']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Games Analyzed</div>
            <div class="value">{edge['total_games']}</div>
        </div>
    </div>

    <div class="section">
        <h2>Outcome Distribution</h2>
        <table>
            <tr>
                <th>Outcome</th>
                <th>Count</th>
                <th>Actual %</th>
                <th>Market Expectation</th>
                <th>Edge</th>
            </tr>
            <tr>
                <td><strong>Home Win</strong></td>
                <td>{edge['home_wins']}</td>
                <td>{edge['home_win_pct']:.1f}%</td>
                <td>~40-45%</td>
                <td class="{'positive' if edge['home_win_pct'] > 45 else 'negative' if edge['home_win_pct'] < 40 else ''}">{edge['home_win_pct'] - 42.5:+.1f}%</td>
            </tr>
            <tr>
                <td><strong>Draw</strong></td>
                <td>{edge['draws']}</td>
                <td>{edge['draw_pct']:.1f}%</td>
                <td>~25-30%</td>
                <td class="{'positive' if edge['draw_pct'] > 30 else 'negative' if edge['draw_pct'] < 25 else ''}">{edge['draw_pct'] - 27.5:+.1f}%</td>
            </tr>
            <tr>
                <td><strong>Away Win</strong></td>
                <td>{edge['away_wins']}</td>
                <td>{edge['away_win_pct']:.1f}%</td>
                <td>~28-32%</td>
                <td class="{'positive' if edge['away_win_pct'] > 32 else 'negative' if edge['away_win_pct'] < 28 else ''}">{edge['away_win_pct'] - 30:+.1f}%</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <h2>Betting Implications</h2>
        <table>
            <tr>
                <th>Strategy</th>
                <th>Historical Edge</th>
                <th>Recommendation</th>
            </tr>
            <tr>
                <td>Blind Home Betting</td>
                <td>{edge['home_win_pct'] - 42.5:+.1f}% vs expectation</td>
                <td>{'Positive expected value' if edge['home_win_pct'] > 45 else 'Neutral' if edge['home_win_pct'] > 40 else 'Negative expected value'}</td>
            </tr>
            <tr>
                <td>Draw Specialization</td>
                <td>{edge['draw_pct'] - 27.5:+.1f}% vs expectation</td>
                <td>{'Consider draw markets' if edge['draw_pct'] > 28 else 'Avoid draw bets'}</td>
            </tr>
            <tr>
                <td>Away Underdog</td>
                <td>{edge['away_win_pct'] - 30:+.1f}% vs expectation</td>
                <td>{'Value in away teams' if edge['away_win_pct'] > 32 else 'Standard pricing'}</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Edge Analysis:</strong>
        <ul>
            <li>Home advantage: {edge['home_win_pct']:.1f}% actual vs ~42% market expectation</li>
            <li>Net edge: {edge['home_win_pct'] - 42:+.1f}% when backing home teams</li>
            <li>Sample size: {edge['total_games']} games (statistical significance {'achieved' if edge['total_games'] > 50 else 'pending'})</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Historical outcome analysis from recent completed matches.
        Market expectations based on typical Premier League/NFL distributions.
        Edge = Actual % - Expected %. Positive edge indicates potential value.
        Statistical significance typically requires 50+ games.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Home/Away Edge Analysis",
        subtitle="Historical Outcome Distribution & Betting Edge",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Home_Away_Edge.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 3: BANKROLL SIMULATION
# ============================================================================

def generate_bankroll_report(events, league_name):
    """Generate Bankroll Simulation PDF"""
    print(f"\n🎰 Generating {league_name} Bankroll Simulation...")

    if not events:
        return

    sim = simulate_kelly_bankroll(events)

    roi_class = 'positive' if sim['total_return'] > 0 else 'negative'

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Starting Bankroll</div>
            <div class="value">$1,000</div>
        </div>
        <div class="kpi-card">
            <div class="label">Ending Bankroll</div>
            <div class="value {roi_class}">${sim['final_bankroll']:.2f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Total Return</div>
            <div class="value {roi_class}">{sim['total_return']:+.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Win Rate</div>
            <div class="value">{sim['win_rate']:.1f}%</div>
        </div>
    </div>

    <div class="section">
        <h2>Simulation Parameters</h2>
        <table>
            <tr>
                <th>Parameter</th>
                <th>Value</th>
                <th>Description</th>
            </tr>
            <tr>
                <td>Strategy</td>
                <td>Home Favorite</td>
                <td>Bet on home team each game</td>
            </tr>
            <tr>
                <td>Bet Sizing</td>
                <td>Kelly Criterion (25%)</td>
                <td>Fractional Kelly for risk management</td>
            </tr>
            <tr>
                <td>Implied Edge</td>
                <td>3%</td>
                <td>Assumed true probability vs market</td>
            </tr>
            <tr>
                <td>Max Bet Size</td>
                <td>10% of bankroll</td>
                <td>Risk cap per bet</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <h2>Sample Bet History</h2>
        <table>
            <tr>
                <th>Game</th>
                <th>Bet Size</th>
                <th>Result</th>
                <th>P/L</th>
                <th>Bankroll</th>
            </tr>
    """

    for bet in sim['bets'][:10]:
        pl_class = 'positive' if bet['profit'] > 0 else 'negative'
        content += f"""
            <tr>
                <td>{bet['game'][:30]}...</td>
                <td>${bet['bet']:.2f}</td>
                <td>{bet['result']}</td>
                <td class="{pl_class}">${bet['profit']:+.2f}</td>
                <td>${bet['bankroll']:.2f}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Risk Metrics</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Max Drawdown</td>
                <td class="{'negative' if sim['max_drawdown'] < -20 else ''}">{sim['max_drawdown']:.1f}%</td>
                <td>{'High risk' if sim['max_drawdown'] < -20 else 'Acceptable' if sim['max_drawdown'] < -10 else 'Low risk'}</td>
            </tr>
            <tr>
                <td>Win Rate</td>
                <td>{sim['win_rate']:.1f}%</td>
                <td>{'Above break-even' if sim['win_rate'] > 50 else 'Below break-even'}</td>
            </tr>
            <tr>
                <td>ROI</td>
                <td class="{roi_class}">{sim['total_return']:+.1f}%</td>
                <td>{'Profitable' if sim['total_return'] > 0 else 'Unprofitable'}</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box {'warning' if sim['total_return'] < 0 else ''}">
        <strong>Simulation Results:</strong>
        <ul>
            <li>Final bankroll: ${sim['final_bankroll']:.2f} ({sim['total_return']:+.1f}% return)</li>
            <li>Win rate: {sim['win_rate']:.1f}% on {len(sim['bets'])} bets</li>
            <li>Kelly criterion helps optimize bet sizing for long-term growth</li>
            <li><em>Note: Past performance does not guarantee future results</em></li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Kelly Criterion formula: f* = (bp - q) / b where b = odds - 1, p = win probability, q = 1 - p.
        Fractional Kelly (25%) used to reduce variance. Simulation uses actual game outcomes with assumed edge.
        This is a demonstration model; real betting requires edge verification and proper bankroll management.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Bankroll Simulation",
        subtitle="Kelly Criterion Strategy Backtest",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API + Statistical Model",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Bankroll_Simulation.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 4: PUBLIC VS SHARP ANALYSIS
# ============================================================================

def generate_public_sharp_report(standings, league_name):
    """Generate Public vs Sharp Team Analysis PDF"""
    print(f"\n🎰 Generating {league_name} Public vs Sharp Analysis...")

    if not standings:
        return

    teams = identify_public_teams(standings)
    if not teams:
        return

    public_teams = [t for t in teams if t['is_public_team']]
    public_avg_win = np.mean([t['win_pct'] for t in public_teams]) if public_teams else 0
    all_avg_win = np.mean([t['win_pct'] for t in teams])

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Public Teams Identified</div>
            <div class="value">{len(public_teams)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Public Team Avg Win %</div>
            <div class="value">{public_avg_win:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">League Avg Win %</div>
            <div class="value">{all_avg_win:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Public Premium</div>
            <div class="value {'positive' if public_avg_win > all_avg_win else 'negative'}">{public_avg_win - all_avg_win:+.1f}%</div>
        </div>
    </div>

    <div class="section">
        <h2>Public Team Analysis</h2>
        <p>Public teams attract more recreational betting volume, often creating line value on opponents.</p>
        <table>
            <tr>
                <th>Team</th>
                <th>Rank</th>
                <th>Win %</th>
                <th>Public Rating</th>
                <th>Line Impact</th>
            </tr>
    """

    for t in teams[:12]:
        line_impact = 'Inflated lines' if t['is_public_team'] else 'Fair value'
        content += f"""
            <tr>
                <td><strong>{t['team']}</strong></td>
                <td>{t['rank']}</td>
                <td>{t['win_pct']:.1f}%</td>
                <td>{t['public_rating']}</td>
                <td>{line_impact}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Betting Implications</h2>
        <table>
            <tr>
                <th>Scenario</th>
                <th>Strategy</th>
                <th>Rationale</th>
            </tr>
            <tr>
                <td>Public team favored big</td>
                <td>Fade public (bet opponent)</td>
                <td>Line inflated by recreational money</td>
            </tr>
            <tr>
                <td>Public team as underdog</td>
                <td>Bet public team</td>
                <td>Getting points on good team</td>
            </tr>
            <tr>
                <td>Low-profile matchup</td>
                <td>Standard analysis</td>
                <td>Less public bias in line</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Public Betting Insights:</strong>
        <ul>
            <li>Public teams typically attract 60-70% of bet count but less than 50% of money</li>
            <li>Sharp bettors often fade heavily-backed public favorites</li>
            <li>Value opportunities exist when public perception diverges from true probability</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Public teams identified by name recognition, historical popularity, and current rank (top 5).
        Public rating: High = major market team or top performer, Medium = top 10, Low = others.
        This analysis helps identify potential line value from public betting bias.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Public vs Sharp Analysis",
        subtitle="Identifying Public Betting Bias",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API + Betting Theory",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Public_Sharp.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("CMS LinkedIn Proof Package - Betting Analytics Reports")
    print(f"Author: {AUTHOR}")
    print(f"Date: {DATE}")
    print("=" * 60)

    # Fetch data
    primary_league = 'Premier League'
    league_id = LEAGUES[primary_league]

    print(f"\n📥 Fetching {primary_league} data...")
    standings = fetch_league_standings(league_id)
    events = fetch_past_events(league_id)

    if standings:
        print(f"  ✓ Standings: {len(standings)} teams")
    if events:
        print(f"  ✓ Events: {len(events)} games")

    print("\n📄 Generating Reports...")

    if standings:
        generate_implied_prob_report(standings, primary_league)
        generate_public_sharp_report(standings, primary_league)

    if events:
        generate_home_away_report(events, primary_league)
        generate_bankroll_report(events, primary_league)

    print("\n" + "=" * 60)
    print("✅ Betting Reports Complete!")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
