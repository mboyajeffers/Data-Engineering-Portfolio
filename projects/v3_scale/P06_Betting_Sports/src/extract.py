"""
P06 Betting & Sports Analytics - Data Extraction Module
Author: Mboya Jeffers

Enterprise-scale extractor for sports data from ESPN and other public APIs.
Implements rate limiting, checkpointing, and comprehensive logging.
"""

import json
import logging
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SportsDataExtractor:
    """
    Enterprise-scale extractor for sports betting and analytics data.

    Data Sources:
    - ESPN API (public endpoints)
    - Sports-Reference style data

    Features:
    - Rate limiting with exponential backoff
    - Checkpoint/resume capability
    - Comprehensive error handling
    - Extraction logging for audit trail
    """

    # ESPN API endpoints (public, no auth required)
    ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

    # Supported leagues
    LEAGUES = {
        'nfl': {'sport': 'football', 'league': 'nfl', 'name': 'NFL'},
        'nba': {'sport': 'basketball', 'league': 'nba', 'name': 'NBA'},
        'mlb': {'sport': 'baseball', 'league': 'mlb', 'name': 'MLB'},
        'nhl': {'sport': 'hockey', 'league': 'nhl', 'name': 'NHL'},
        'ncaaf': {'sport': 'football', 'league': 'college-football', 'name': 'NCAA Football'},
        'ncaab': {'sport': 'basketball', 'league': 'mens-college-basketball', 'name': 'NCAA Basketball'}
    }

    # Rate limiting
    REQUESTS_PER_MINUTE = 30
    REQUEST_DELAY = 2.0  # seconds between requests

    def __init__(self, output_dir: str = "data"):
        """Initialize extractor with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Sports-Analytics-Pipeline/1.0 (mboyajeffers9@gmail.com)'
        })

        self.extraction_log = {
            'start_time': None,
            'end_time': None,
            'leagues_processed': [],
            'total_games': 0,
            'total_teams': 0,
            'total_players': 0,
            'errors': [],
            'api_calls': 0
        }

        self.checkpoint_file = self.output_dir / "checkpoint.json"
        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, url: str, params: Optional[Dict] = None,
                      max_retries: int = 3) -> Optional[Dict]:
        """
        Make HTTP request with exponential backoff retry.

        Args:
            url: API endpoint URL
            params: Query parameters
            max_retries: Maximum retry attempts

        Returns:
            JSON response dict or None on failure
        """
        self._rate_limit()

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                self.extraction_log['api_calls'] += 1

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    wait_time = (2 ** attempt) * 5
                    logger.warning(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code == 404:
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code}: {url}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def _save_checkpoint(self, league: str, season: int, progress: Dict):
        """Save extraction checkpoint for resume capability."""
        checkpoint = {
            'league': league,
            'season': season,
            'progress': progress,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _load_checkpoint(self) -> Optional[Dict]:
        """Load previous checkpoint if exists."""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return None

    def get_teams(self, league_code: str) -> List[Dict]:
        """
        Fetch all teams for a league.

        Args:
            league_code: League identifier (nfl, nba, mlb, nhl)

        Returns:
            List of team dictionaries
        """
        league_info = self.LEAGUES.get(league_code)
        if not league_info:
            logger.error(f"Unknown league: {league_code}")
            return []

        url = f"{self.ESPN_BASE}/{league_info['sport']}/{league_info['league']}/teams"
        data = self._make_request(url)

        if not data or 'sports' not in data:
            return []

        teams = []
        try:
            for sport in data.get('sports', []):
                for league in sport.get('leagues', []):
                    for team in league.get('teams', []):
                        team_data = team.get('team', {})
                        teams.append({
                            'team_id': team_data.get('id'),
                            'team_name': team_data.get('displayName'),
                            'team_abbrev': team_data.get('abbreviation'),
                            'city': team_data.get('location'),
                            'league': league_code,
                            'primary_color': team_data.get('color'),
                            'logo_url': team_data.get('logos', [{}])[0].get('href') if team_data.get('logos') else None
                        })
        except Exception as e:
            logger.error(f"Error parsing teams: {e}")
            self.extraction_log['errors'].append(f"Team parse error: {str(e)}")

        logger.info(f"Retrieved {len(teams)} teams from {league_code.upper()}")
        self.extraction_log['total_teams'] += len(teams)
        return teams

    def get_scoreboard(self, league_code: str, date: str) -> List[Dict]:
        """
        Fetch games for a specific date.

        Args:
            league_code: League identifier
            date: Date in YYYYMMDD format

        Returns:
            List of game dictionaries
        """
        league_info = self.LEAGUES.get(league_code)
        if not league_info:
            return []

        url = f"{self.ESPN_BASE}/{league_info['sport']}/{league_info['league']}/scoreboard"
        params = {'dates': date}
        data = self._make_request(url, params)

        if not data or 'events' not in data:
            return []

        games = []
        for event in data.get('events', []):
            try:
                competition = event.get('competitions', [{}])[0]
                competitors = competition.get('competitors', [])

                home_team = next((c for c in competitors if c.get('homeAway') == 'home'), {})
                away_team = next((c for c in competitors if c.get('homeAway') == 'away'), {})

                venue_info = competition.get('venue', {})

                game = {
                    'game_id': event.get('id'),
                    'date': event.get('date'),
                    'league': league_code,
                    'status': event.get('status', {}).get('type', {}).get('name'),
                    'home_team_id': home_team.get('team', {}).get('id'),
                    'home_team_name': home_team.get('team', {}).get('displayName'),
                    'home_score': int(home_team.get('score', 0) or 0),
                    'away_team_id': away_team.get('team', {}).get('id'),
                    'away_team_name': away_team.get('team', {}).get('displayName'),
                    'away_score': int(away_team.get('score', 0) or 0),
                    'venue_id': venue_info.get('id'),
                    'venue_name': venue_info.get('fullName'),
                    'venue_city': venue_info.get('address', {}).get('city'),
                    'attendance': competition.get('attendance'),
                    'is_neutral_site': competition.get('neutralSite', False)
                }

                # Get odds if available
                odds_data = competition.get('odds', [{}])[0] if competition.get('odds') else {}
                if odds_data:
                    game['spread'] = odds_data.get('spread')
                    game['over_under'] = odds_data.get('overUnder')
                    game['home_moneyline'] = odds_data.get('homeTeamOdds', {}).get('moneyLine')
                    game['away_moneyline'] = odds_data.get('awayTeamOdds', {}).get('moneyLine')

                games.append(game)

            except Exception as e:
                logger.warning(f"Error parsing game: {e}")
                continue

        return games

    def get_game_boxscore(self, league_code: str, game_id: str) -> Dict:
        """
        Fetch detailed boxscore for a game.

        Args:
            league_code: League identifier
            game_id: ESPN game ID

        Returns:
            Dictionary with player stats
        """
        league_info = self.LEAGUES.get(league_code)
        if not league_info:
            return {}

        url = f"{self.ESPN_BASE}/{league_info['sport']}/{league_info['league']}/summary"
        params = {'event': game_id}
        data = self._make_request(url, params)

        if not data:
            return {}

        boxscore = {
            'game_id': game_id,
            'players': []
        }

        try:
            for box_player in data.get('boxscore', {}).get('players', []):
                team_id = box_player.get('team', {}).get('id')

                for stat_group in box_player.get('statistics', []):
                    stat_names = stat_group.get('names', [])

                    for athlete in stat_group.get('athletes', []):
                        player_info = athlete.get('athlete', {})
                        stats = athlete.get('stats', [])

                        player_stats = {
                            'player_id': player_info.get('id'),
                            'player_name': player_info.get('displayName'),
                            'team_id': team_id,
                            'position': player_info.get('position', {}).get('abbreviation'),
                            'is_starter': athlete.get('starter', False)
                        }

                        # Map stats to names
                        for i, stat_name in enumerate(stat_names):
                            if i < len(stats):
                                player_stats[stat_name.lower().replace(' ', '_')] = stats[i]

                        boxscore['players'].append(player_stats)

        except Exception as e:
            logger.warning(f"Error parsing boxscore: {e}")
            self.extraction_log['errors'].append(f"Boxscore parse error: {game_id}")

        return boxscore

    def extract_league_season(self, league_code: str, season_year: int,
                              start_month: int = 1, end_month: int = 12) -> Dict:
        """
        Extract all games for a league season.

        Args:
            league_code: League identifier
            season_year: Year of the season
            start_month: Starting month (1-12)
            end_month: Ending month (1-12)

        Returns:
            Dictionary with teams, games, and player stats
        """
        logger.info(f"Extracting {league_code.upper()} {season_year} season...")

        result = {
            'league': league_code,
            'season': season_year,
            'teams': [],
            'games': [],
            'player_stats': [],
            'venues': []
        }

        # Get teams
        result['teams'] = self.get_teams(league_code)

        # Get games for each day of the season
        current_date = datetime(season_year, start_month, 1)
        end_date = datetime(season_year, end_month, 28)

        games_found = 0
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            games = self.get_scoreboard(league_code, date_str)

            for game in games:
                if game['status'] == 'STATUS_FINAL':
                    result['games'].append(game)
                    games_found += 1

                    # Track venues
                    if game.get('venue_id'):
                        venue = {
                            'venue_id': game['venue_id'],
                            'venue_name': game['venue_name'],
                            'city': game['venue_city']
                        }
                        if venue not in result['venues']:
                            result['venues'].append(venue)

            # Progress log every 7 days
            if current_date.day == 1 or current_date.day == 15:
                logger.info(f"  {date_str}: {games_found} games so far")
                self._save_checkpoint(league_code, season_year, {
                    'date': date_str,
                    'games': games_found
                })

            current_date += timedelta(days=1)

        logger.info(f"  Extracted {len(result['games'])} completed games")
        self.extraction_log['total_games'] += len(result['games'])
        self.extraction_log['leagues_processed'].append({
            'league': league_code,
            'season': season_year,
            'games': len(result['games']),
            'teams': len(result['teams'])
        })

        return result

    def run_test_extraction(self, limit: int = 100) -> Dict:
        """
        Run limited test extraction for validation.

        Args:
            limit: Maximum games to extract per league

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()
        logger.info("=" * 60)
        logger.info(f"STARTING TEST EXTRACTION (limit={limit})")
        logger.info("=" * 60)

        all_data = {
            'leagues': [],
            'all_teams': [],
            'all_games': [],
            'all_venues': []
        }

        # Test with major leagues
        test_leagues = ['nba', 'nfl']
        current_year = datetime.now().year

        for league_code in test_leagues:
            league_data = {
                'league': league_code,
                'teams': self.get_teams(league_code),
                'games': [],
                'venues': []
            }

            # Get recent games (last 7 days)
            for days_back in range(7):
                date = datetime.now() - timedelta(days=days_back)
                date_str = date.strftime('%Y%m%d')

                games = self.get_scoreboard(league_code, date_str)
                for game in games:
                    if game['status'] == 'STATUS_FINAL':
                        league_data['games'].append(game)
                        if len(league_data['games']) >= limit:
                            break

                if len(league_data['games']) >= limit:
                    break

            all_data['leagues'].append(league_data)
            all_data['all_teams'].extend(league_data['teams'])
            all_data['all_games'].extend(league_data['games'])

            self.extraction_log['leagues_processed'].append({
                'league': league_code,
                'games': len(league_data['games']),
                'teams': len(league_data['teams'])
            })

        self.extraction_log['end_time'] = datetime.now().isoformat()
        self.extraction_log['total_games'] = len(all_data['all_games'])
        self.extraction_log['total_teams'] = len(all_data['all_teams'])

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data

    def run_full_extraction(self, leagues: Optional[List[str]] = None,
                           seasons: Optional[List[int]] = None) -> Dict:
        """
        Run full-scale extraction for production.

        Args:
            leagues: List of league codes (default: all major leagues)
            seasons: List of season years (default: last 5 years)

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()

        if leagues is None:
            leagues = ['nba', 'nfl', 'mlb', 'nhl']

        if seasons is None:
            current_year = datetime.now().year
            seasons = list(range(current_year - 4, current_year + 1))

        logger.info("=" * 60)
        logger.info("STARTING FULL EXTRACTION")
        logger.info(f"Leagues: {leagues}")
        logger.info(f"Seasons: {seasons}")
        logger.info("=" * 60)

        all_data = {
            'leagues': [],
            'all_teams': [],
            'all_games': [],
            'all_venues': [],
            'all_player_stats': []
        }

        # Season date ranges by league
        SEASON_MONTHS = {
            'nba': (10, 6),    # Oct - Jun
            'nfl': (9, 2),     # Sep - Feb
            'mlb': (3, 10),    # Mar - Oct
            'nhl': (10, 6)     # Oct - Jun
        }

        for league_code in leagues:
            start_month, end_month = SEASON_MONTHS.get(league_code, (1, 12))

            for season_year in seasons:
                try:
                    season_data = self.extract_league_season(
                        league_code, season_year,
                        start_month=start_month,
                        end_month=end_month
                    )

                    all_data['leagues'].append(season_data)
                    all_data['all_teams'].extend(season_data['teams'])
                    all_data['all_games'].extend(season_data['games'])
                    all_data['all_venues'].extend(season_data['venues'])

                    # Save intermediate results
                    intermediate_path = self.output_dir / f"{league_code}_{season_year}.json"
                    with open(intermediate_path, 'w') as f:
                        json.dump(season_data, f)
                    logger.info(f"  Saved: {intermediate_path}")

                except Exception as e:
                    logger.error(f"Failed {league_code} {season_year}: {e}")
                    self.extraction_log['errors'].append(f"{league_code} {season_year}: {str(e)}")

        self.extraction_log['end_time'] = datetime.now().isoformat()
        self.extraction_log['total_games'] = len(all_data['all_games'])
        self.extraction_log['total_teams'] = len(set(t['team_id'] for t in all_data['all_teams']))

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data


if __name__ == "__main__":
    extractor = SportsDataExtractor()

    # Test extraction
    data = extractor.run_test_extraction(limit=50)

    print("\nExtraction Summary:")
    print(f"  Teams: {len(data['all_teams'])}")
    print(f"  Games: {len(data['all_games'])}")
    print(f"  API Calls: {extractor.extraction_log['api_calls']}")
