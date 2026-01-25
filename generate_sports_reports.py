#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Sports Reports
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026

Data Source: TheSportsDB API (free tier, API key: 3)
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

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Sports"
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"
DATE = datetime.now().strftime("%B %d, %Y")

# TheSportsDB API
API_KEY = "3"  # Free tier key
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

# League IDs
LEAGUES = {
    'NFL': 4391,
    'NBA': 4387,
    'NHL': 4380,
    'MLB': 4424,
    'Premier League': 4328
}

# ============================================================================
# CSS
# ============================================================================

BASE_CSS = """
@page { size: letter; margin: 0.75in; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.5; }
.header { border-bottom: 3px solid #8b5cf6; padding-bottom: 10px; margin-bottom: 20px; }
.header h1 { color: #1a1a2e; margin: 0; font-size: 28px; }
.header .subtitle { color: #666; font-size: 14px; margin-top: 5px; }
.header .date { color: #999; font-size: 12px; float: right; }
.kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }
.kpi-card { background: #faf5ff; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #8b5cf6; }
.kpi-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
.kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; margin: 8px 0; }
.kpi-card .value.good { color: #10b981; }
.kpi-card .value.poor { color: #ef4444; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
th { background: #8b5cf6; color: white; padding: 12px 10px; text-align: left; }
td { padding: 10px; border-bottom: 1px solid #eee; }
tr:nth-child(even) { background: #faf5ff; }
.section { margin: 30px 0; }
.section h2 { color: #8b5cf6; font-size: 18px; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.highlight-box { background: #f3e8ff; border-left: 4px solid #a855f7; padding: 15px; margin: 15px 0; }
.methodology { background: #f8f9fa; padding: 15px; border-radius: 8px; font-size: 12px; color: #666; margin-top: 30px; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; font-size: 11px; color: #999; }
.footer .author { color: #8b5cf6; font-weight: bold; }
.win { color: #10b981; font-weight: bold; }
.loss { color: #ef4444; }
.draw { color: #f59e0b; }
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
    """Fetch league standings/table"""
    url = f"{BASE_URL}/lookuptable.php?l={league_id}&s={season}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('table'):
            return data['table']
    except Exception as e:
        print(f"    Error fetching standings: {e}")
    return None

def fetch_league_teams(league_id):
    """Fetch all teams in a league"""
    url = f"{BASE_URL}/lookup_all_teams.php?id={league_id}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('teams'):
            return data['teams']
    except Exception as e:
        print(f"    Error fetching teams: {e}")
    return None

def fetch_past_events(league_id, round_num=None):
    """Fetch past events/games"""
    url = f"{BASE_URL}/eventspastleague.php?id={league_id}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('events'):
            return data['events']
    except Exception as e:
        print(f"    Error fetching events: {e}")
    return None

def fetch_next_events(league_id):
    """Fetch upcoming events"""
    url = f"{BASE_URL}/eventsnextleague.php?id={league_id}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('events'):
            return data['events']
    except Exception as e:
        print(f"    Error fetching next events: {e}")
    return None

# ============================================================================
# DATA PROCESSING
# ============================================================================

def process_standings(standings):
    """Process standings data into usable format"""
    processed = []
    for team in standings:
        try:
            processed.append({
                'rank': int(team.get('intRank', 0)),
                'team': team.get('strTeam', 'Unknown'),
                'played': int(team.get('intPlayed', 0)),
                'wins': int(team.get('intWin', 0)),
                'draws': int(team.get('intDraw', 0)),
                'losses': int(team.get('intLoss', 0)),
                'goals_for': int(team.get('intGoalsFor', 0)),
                'goals_against': int(team.get('intGoalsAgainst', 0)),
                'goal_diff': int(team.get('intGoalDifference', 0)),
                'points': int(team.get('intPoints', 0)),
                'form': team.get('strForm', '')
            })
        except (ValueError, TypeError):
            continue
    return processed

def process_events(events):
    """Process events data"""
    processed = []
    for event in events:
        try:
            home_score = event.get('intHomeScore')
            away_score = event.get('intAwayScore')
            if home_score and away_score:
                processed.append({
                    'date': event.get('dateEvent', ''),
                    'home_team': event.get('strHomeTeam', ''),
                    'away_team': event.get('strAwayTeam', ''),
                    'home_score': int(home_score),
                    'away_score': int(away_score),
                    'venue': event.get('strVenue', ''),
                    'round': event.get('intRound', '')
                })
        except (ValueError, TypeError):
            continue
    return processed

# ============================================================================
# KPI CALCULATIONS
# ============================================================================

def compute_league_kpis(standings):
    """Compute league-wide KPIs"""
    if not standings:
        return {}

    df = pd.DataFrame(standings)

    kpis = {}
    kpis['total_teams'] = len(df)
    kpis['total_games'] = df['played'].sum() // 2  # Each game counted twice
    kpis['total_goals'] = df['goals_for'].sum()
    kpis['avg_goals_per_game'] = kpis['total_goals'] / kpis['total_games'] if kpis['total_games'] > 0 else 0

    # Win percentage distribution
    df['win_pct'] = df['wins'] / df['played'] * 100
    kpis['avg_win_pct'] = df['win_pct'].mean()
    kpis['win_pct_std'] = df['win_pct'].std()

    # Points spread (competitive balance)
    kpis['points_spread'] = df['points'].max() - df['points'].min()
    kpis['points_std'] = df['points'].std()

    # Parity index (lower = more competitive)
    kpis['parity_index'] = kpis['points_std'] / df['points'].mean() * 100 if df['points'].mean() > 0 else 0

    # Leader
    kpis['leader'] = df.loc[df['points'].idxmax(), 'team']
    kpis['leader_points'] = df['points'].max()

    return kpis

def compute_team_kpis(team_standings):
    """Compute individual team KPIs"""
    if not team_standings:
        return {}

    kpis = {}
    kpis['win_pct'] = team_standings['wins'] / team_standings['played'] * 100 if team_standings['played'] > 0 else 0
    kpis['ppg'] = team_standings['points'] / team_standings['played'] if team_standings['played'] > 0 else 0
    kpis['goals_per_game'] = team_standings['goals_for'] / team_standings['played'] if team_standings['played'] > 0 else 0
    kpis['goals_against_per_game'] = team_standings['goals_against'] / team_standings['played'] if team_standings['played'] > 0 else 0

    return kpis

# ============================================================================
# REPORT 1: LEAGUE SEASON SUMMARY
# ============================================================================

def generate_season_summary_report(standings, league_name='NFL'):
    """Generate League Season Summary PDF"""
    print(f"\n🏈 Generating {league_name} Season Summary...")

    if not standings:
        print(f"  ✗ No standings data for {league_name}")
        return

    processed = process_standings(standings)
    if not processed:
        print(f"  ✗ Could not process standings")
        return

    kpis = compute_league_kpis(processed)
    df = pd.DataFrame(processed)

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">League Leader</div>
            <div class="value good" style="font-size: 18px;">{kpis.get('leader', 'N/A')}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Leader Points</div>
            <div class="value">{kpis.get('leader_points', 0)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Games Played</div>
            <div class="value">{kpis.get('total_games', 0)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Avg Goals/Game</div>
            <div class="value">{kpis.get('avg_goals_per_game', 0):.2f}</div>
        </div>
    </div>

    <div class="section">
        <h2>Current Standings</h2>
        <table>
            <tr>
                <th>#</th>
                <th>Team</th>
                <th>P</th>
                <th>W</th>
                <th>D</th>
                <th>L</th>
                <th>GF</th>
                <th>GA</th>
                <th>GD</th>
                <th>Pts</th>
            </tr>
    """

    for team in sorted(processed, key=lambda x: x['rank'])[:15]:  # Top 15
        gd_class = 'win' if team['goal_diff'] > 0 else 'loss' if team['goal_diff'] < 0 else ''
        content += f"""
            <tr>
                <td>{team['rank']}</td>
                <td><strong>{team['team']}</strong></td>
                <td>{team['played']}</td>
                <td class="win">{team['wins']}</td>
                <td class="draw">{team['draws']}</td>
                <td class="loss">{team['losses']}</td>
                <td>{team['goals_for']}</td>
                <td>{team['goals_against']}</td>
                <td class="{gd_class}">{team['goal_diff']:+d}</td>
                <td><strong>{team['points']}</strong></td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>League Statistics</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Total Teams</td>
                <td>{kpis.get('total_teams', 0)}</td>
                <td>League size</td>
            </tr>
            <tr>
                <td>Avg Win %</td>
                <td>{kpis.get('avg_win_pct', 0):.1f}%</td>
                <td>Expected ~50% for balanced competition</td>
            </tr>
            <tr>
                <td>Points Spread</td>
                <td>{kpis.get('points_spread', 0)}</td>
                <td>Gap between 1st and last</td>
            </tr>
            <tr>
                <td>Parity Index</td>
                <td>{kpis.get('parity_index', 0):.1f}%</td>
                <td>{'High parity (competitive)' if kpis.get('parity_index', 100) < 30 else 'Low parity (dominant teams)'}</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Season Insights:</strong>
        <ul>
            <li>Current leader: {kpis.get('leader', 'N/A')} with {kpis.get('leader_points', 0)} points</li>
            <li>League competitiveness: {'High' if kpis.get('parity_index', 100) < 30 else 'Moderate' if kpis.get('parity_index', 100) < 50 else 'Low'} (parity index: {kpis.get('parity_index', 0):.1f}%)</li>
            <li>Scoring rate: {kpis.get('avg_goals_per_game', 0):.2f} goals per match</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Standings data from TheSportsDB API.
        Points system: 3 for win, 1 for draw, 0 for loss.
        Parity Index = (Points StdDev / Points Mean) × 100. Lower values indicate more competitive league.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Season Summary",
        subtitle="2024-2025 Season | League Standings & Statistics",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Season_Summary.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 2: TEAM PERFORMANCE TRENDS
# ============================================================================

def generate_performance_trends_report(standings, events, league_name='NFL'):
    """Generate Team Performance Trends PDF"""
    print(f"\n🏈 Generating {league_name} Performance Trends...")

    if not standings:
        return

    processed = process_standings(standings)
    if not processed:
        return

    # Analyze form and trends
    df = pd.DataFrame(processed)
    df['win_pct'] = df['wins'] / df['played'] * 100
    df['goals_per_game'] = df['goals_for'] / df['played']
    df['goals_against_per_game'] = df['goals_against'] / df['played']

    # Identify trends from form
    trending_up = []
    trending_down = []
    for team in processed:
        form = team.get('form', '')
        if len(form) >= 3:
            recent = form[-3:]
            wins = recent.count('W')
            losses = recent.count('L')
            if wins >= 2:
                trending_up.append(team['team'])
            elif losses >= 2:
                trending_down.append(team['team'])

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Teams Trending Up</div>
            <div class="value good">{len(trending_up)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Teams Trending Down</div>
            <div class="value poor">{len(trending_down)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Best Offense (GPG)</div>
            <div class="value">{df['goals_per_game'].max():.2f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Best Defense (GA/G)</div>
            <div class="value">{df['goals_against_per_game'].min():.2f}</div>
        </div>
    </div>

    <div class="section">
        <h2>Top Performers</h2>
        <table>
            <tr>
                <th>Team</th>
                <th>Win %</th>
                <th>Goals/Game</th>
                <th>GA/Game</th>
                <th>Recent Form</th>
            </tr>
    """

    top_teams = sorted(processed, key=lambda x: x['points'], reverse=True)[:8]
    for team in top_teams:
        win_pct = team['wins'] / team['played'] * 100 if team['played'] > 0 else 0
        gpg = team['goals_for'] / team['played'] if team['played'] > 0 else 0
        gapg = team['goals_against'] / team['played'] if team['played'] > 0 else 0
        form = team.get('form', '')[-5:]  # Last 5 games
        form_html = ''.join([f'<span class="{"win" if c=="W" else "loss" if c=="L" else "draw"}">{c}</span>' for c in form])

        content += f"""
            <tr>
                <td><strong>{team['team']}</strong></td>
                <td>{win_pct:.1f}%</td>
                <td>{gpg:.2f}</td>
                <td>{gapg:.2f}</td>
                <td>{form_html}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Trending Teams</h2>
        <table>
            <tr>
                <th>Trend</th>
                <th>Teams</th>
            </tr>
            <tr>
                <td class="win">Trending Up (2+ wins in last 3)</td>
                <td>{', '.join(trending_up[:5]) if trending_up else 'None identified'}</td>
            </tr>
            <tr>
                <td class="loss">Trending Down (2+ losses in last 3)</td>
                <td>{', '.join(trending_down[:5]) if trending_down else 'None identified'}</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Performance Insights:</strong>
        <ul>
            <li>Best offensive team: {df.loc[df['goals_per_game'].idxmax(), 'team']} ({df['goals_per_game'].max():.2f} GPG)</li>
            <li>Best defensive team: {df.loc[df['goals_against_per_game'].idxmin(), 'team']} ({df['goals_against_per_game'].min():.2f} GA/G)</li>
            <li>Most balanced: Teams with low goal differential variance suggest consistent performance</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Performance metrics computed from season-to-date statistics.
        Form string represents recent results (W=Win, D=Draw, L=Loss).
        Trend analysis based on last 3 matches: 2+ wins = trending up, 2+ losses = trending down.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Performance Trends",
        subtitle="Team Analytics & Form Analysis",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Performance_Trends.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 3: LEAGUE PARITY INDEX
# ============================================================================

def generate_parity_report(all_standings):
    """Generate League Parity Index comparison"""
    print("\n🏈 Generating League Parity Index...")

    results = []
    for league_name, standings in all_standings.items():
        if not standings:
            continue
        processed = process_standings(standings)
        if not processed:
            continue

        kpis = compute_league_kpis(processed)
        df = pd.DataFrame(processed)

        results.append({
            'league': league_name,
            'teams': len(processed),
            'points_spread': kpis.get('points_spread', 0),
            'parity_index': kpis.get('parity_index', 0),
            'avg_win_pct': kpis.get('avg_win_pct', 50),
            'leader': kpis.get('leader', 'N/A'),
            'avg_goals': kpis.get('avg_goals_per_game', 0)
        })

    if not results:
        print("  ✗ No data to analyze")
        return

    # Sort by parity (lower = more competitive)
    results.sort(key=lambda x: x['parity_index'])

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Most Competitive League</div>
            <div class="value good" style="font-size: 18px;">{results[0]['league']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Lowest Parity Index</div>
            <div class="value">{results[0]['parity_index']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Leagues Analyzed</div>
            <div class="value">{len(results)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Avg Parity Index</div>
            <div class="value">{np.mean([r['parity_index'] for r in results]):.1f}%</div>
        </div>
    </div>

    <div class="section">
        <h2>League Competitiveness Ranking</h2>
        <table>
            <tr>
                <th>Rank</th>
                <th>League</th>
                <th>Teams</th>
                <th>Parity Index</th>
                <th>Points Spread</th>
                <th>Competitiveness</th>
            </tr>
    """

    for i, r in enumerate(results):
        comp = 'High' if r['parity_index'] < 30 else 'Moderate' if r['parity_index'] < 50 else 'Low'
        comp_class = 'good' if comp == 'High' else '' if comp == 'Moderate' else 'poor'
        content += f"""
            <tr>
                <td>{i+1}</td>
                <td><strong>{r['league']}</strong></td>
                <td>{r['teams']}</td>
                <td>{r['parity_index']:.1f}%</td>
                <td>{r['points_spread']}</td>
                <td class="{comp_class}">{comp}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>League Leaders</h2>
        <table>
            <tr>
                <th>League</th>
                <th>Current Leader</th>
                <th>Avg Goals/Game</th>
            </tr>
    """

    for r in results:
        content += f"""
            <tr>
                <td><strong>{r['league']}</strong></td>
                <td>{r['leader']}</td>
                <td>{r['avg_goals']:.2f}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Competitive Balance Insights:</strong>
        <ul>
            <li>Parity Index measures how evenly distributed points are across teams</li>
            <li>Lower parity index = more competitive league (any team can beat any team)</li>
            <li>High parity leagues tend to have more viewer engagement and unpredictable outcomes</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Parity Index = (Points Standard Deviation / Points Mean) × 100.
        Lower values indicate more competitive balance. Points spread = gap between 1st and last place.
        Data from TheSportsDB for current season standings.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="League Parity Index",
        subtitle="Cross-League Competitive Balance Analysis",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_League_Parity_Index.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 4: HEAD-TO-HEAD ANALYSIS (Recent Games)
# ============================================================================

def generate_recent_games_report(events, league_name='Premier League'):
    """Generate Recent Games Analysis PDF"""
    print(f"\n🏈 Generating {league_name} Recent Games Analysis...")

    if not events:
        print(f"  ✗ No events data")
        return

    processed = process_events(events)
    if not processed:
        print(f"  ✗ Could not process events")
        return

    df = pd.DataFrame(processed)

    # Calculate game metrics
    df['total_goals'] = df['home_score'] + df['away_score']
    df['home_win'] = df['home_score'] > df['away_score']
    df['away_win'] = df['away_score'] > df['home_score']
    df['draw'] = df['home_score'] == df['away_score']

    # KPIs
    total_games = len(df)
    home_wins = df['home_win'].sum()
    away_wins = df['away_win'].sum()
    draws = df['draw'].sum()
    avg_goals = df['total_goals'].mean()
    high_scoring = (df['total_goals'] >= 4).sum()

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Recent Games</div>
            <div class="value">{total_games}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Home Win %</div>
            <div class="value">{home_wins/total_games*100:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Avg Goals/Game</div>
            <div class="value">{avg_goals:.2f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">High-Scoring Games (4+)</div>
            <div class="value">{high_scoring}</div>
        </div>
    </div>

    <div class="section">
        <h2>Recent Results</h2>
        <table>
            <tr>
                <th>Date</th>
                <th>Home</th>
                <th>Score</th>
                <th>Away</th>
                <th>Total Goals</th>
            </tr>
    """

    for _, game in df.head(15).iterrows():
        score_class = 'win' if game['home_win'] else 'loss' if game['away_win'] else 'draw'
        content += f"""
            <tr>
                <td>{game['date']}</td>
                <td><strong>{game['home_team']}</strong></td>
                <td class="{score_class}">{game['home_score']} - {game['away_score']}</td>
                <td>{game['away_team']}</td>
                <td>{game['total_goals']}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Outcome Distribution</h2>
        <table>
            <tr>
                <th>Outcome</th>
                <th>Count</th>
                <th>Percentage</th>
            </tr>
            <tr>
                <td class="win">Home Win</td>
                <td>{home_wins}</td>
                <td>{home_wins/total_games*100:.1f}%</td>
            </tr>
            <tr>
                <td class="draw">Draw</td>
                <td>{draws}</td>
                <td>{draws/total_games*100:.1f}%</td>
            </tr>
            <tr>
                <td class="loss">Away Win</td>
                <td>{away_wins}</td>
                <td>{away_wins/total_games*100:.1f}%</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Match Insights:</strong>
        <ul>
            <li>Home advantage: {home_wins/total_games*100:.1f}% home win rate vs {away_wins/total_games*100:.1f}% away</li>
            <li>Scoring trends: {avg_goals:.2f} average goals per match</li>
            <li>High-scoring matches (4+ goals): {high_scoring/total_games*100:.1f}% of games</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Analysis of recent completed matches from TheSportsDB.
        Home advantage calculated as home win percentage. High-scoring threshold: 4+ total goals.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"{league_name} Recent Games Analysis",
        subtitle="Match Results & Scoring Trends",
        date=DATE,
        author=AUTHOR,
        data_source="TheSportsDB API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, f"CMS_{league_name.replace(' ', '_')}_Recent_Games.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("CMS LinkedIn Proof Package - Sports Reports")
    print(f"Author: {AUTHOR}")
    print(f"Date: {DATE}")
    print("=" * 60)

    # Fetch data for multiple leagues
    all_standings = {}
    all_events = {}

    for league_name, league_id in LEAGUES.items():
        print(f"\n📥 Fetching {league_name} data...")

        standings = fetch_league_standings(league_id)
        if standings:
            all_standings[league_name] = standings
            print(f"  ✓ Standings: {len(standings)} teams")

        events = fetch_past_events(league_id)
        if events:
            all_events[league_name] = events
            print(f"  ✓ Events: {len(events)} games")

    print("\n📄 Generating Reports...")

    # Generate reports
    if 'Premier League' in all_standings:
        generate_season_summary_report(all_standings['Premier League'], 'Premier League')
        generate_performance_trends_report(all_standings['Premier League'], all_events.get('Premier League'), 'Premier League')

    if 'NBA' in all_standings:
        generate_season_summary_report(all_standings['NBA'], 'NBA')

    if all_standings:
        generate_parity_report(all_standings)

    if 'Premier League' in all_events:
        generate_recent_games_report(all_events['Premier League'], 'Premier League')

    print("\n" + "=" * 60)
    print("✅ Sports Reports Complete!")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
