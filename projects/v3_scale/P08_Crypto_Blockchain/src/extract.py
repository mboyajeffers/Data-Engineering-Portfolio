"""
P08 Crypto & Blockchain Analytics - Data Extraction Module
Author: Mboya Jeffers

Enterprise-scale extractor for cryptocurrency market data from CoinGecko.
Implements rate limiting, pagination, and comprehensive logging.
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


class CryptoDataExtractor:
    """
    Enterprise-scale extractor for cryptocurrency market data.

    Data Sources:
    - CoinGecko API (free tier, no API key required)

    Features:
    - Rate limiting (10-30 calls/minute for free tier)
    - Pagination for coin lists
    - Historical OHLCV extraction
    - Global market metrics
    """

    # CoinGecko API base URL
    BASE_URL = "https://api.coingecko.com/api/v3"

    # Rate limiting (CoinGecko free tier: ~10-30 calls/min)
    REQUESTS_PER_MINUTE = 10
    REQUEST_DELAY = 6.0  # seconds between requests

    def __init__(self, output_dir: str = "data"):
        """Initialize extractor with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Crypto-Analytics-Pipeline/1.0 (mboyajeffers9@gmail.com)',
            'Accept': 'application/json'
        })

        self.extraction_log = {
            'start_time': None,
            'end_time': None,
            'coins_extracted': 0,
            'ohlcv_records': 0,
            'api_calls': 0,
            'errors': []
        }

        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None,
                      max_retries: int = 3) -> Optional[Dict]:
        """
        Make HTTP request with exponential backoff retry.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            max_retries: Maximum retry attempts

        Returns:
            JSON response dict or None on failure
        """
        self._rate_limit()
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                self.extraction_log['api_calls'] += 1

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    wait_time = (2 ** attempt) * 30
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

    def get_coin_list(self, include_platform: bool = True) -> List[Dict]:
        """
        Fetch list of all supported coins.

        Args:
            include_platform: Include platform/contract info

        Returns:
            List of coin dictionaries
        """
        logger.info("Fetching coin list...")

        params = {'include_platform': str(include_platform).lower()}
        data = self._make_request('coins/list', params)

        if data:
            logger.info(f"Retrieved {len(data)} coins")
            return data
        return []

    def get_coin_markets(self, vs_currency: str = 'usd',
                         per_page: int = 250,
                         page: int = 1,
                         order: str = 'market_cap_desc') -> List[Dict]:
        """
        Fetch market data for coins.

        Args:
            vs_currency: Target currency (usd, btc, eth)
            per_page: Results per page (max 250)
            page: Page number
            order: Sort order

        Returns:
            List of coin market data
        """
        logger.info(f"Fetching market data (page {page})...")

        params = {
            'vs_currency': vs_currency,
            'order': order,
            'per_page': per_page,
            'page': page,
            'sparkline': 'false',
            'price_change_percentage': '24h,7d,30d'
        }

        data = self._make_request('coins/markets', params)
        return data if data else []

    def get_coin_details(self, coin_id: str) -> Optional[Dict]:
        """
        Fetch detailed info for a specific coin.

        Args:
            coin_id: CoinGecko coin ID

        Returns:
            Coin details dictionary
        """
        params = {
            'localization': 'false',
            'tickers': 'false',
            'market_data': 'true',
            'community_data': 'false',
            'developer_data': 'false'
        }

        data = self._make_request(f'coins/{coin_id}', params)
        return data

    def get_coin_ohlc(self, coin_id: str, vs_currency: str = 'usd',
                       days: int = 365) -> List[List]:
        """
        Fetch OHLC data for a coin.

        Args:
            coin_id: CoinGecko coin ID
            vs_currency: Target currency
            days: Number of days (1, 7, 14, 30, 90, 180, 365, max)

        Returns:
            List of [timestamp, open, high, low, close]
        """
        params = {
            'vs_currency': vs_currency,
            'days': days
        }

        data = self._make_request(f'coins/{coin_id}/ohlc', params)
        return data if data else []

    def get_coin_market_chart(self, coin_id: str, vs_currency: str = 'usd',
                               days: int = 365) -> Optional[Dict]:
        """
        Fetch historical market chart data.

        Args:
            coin_id: CoinGecko coin ID
            vs_currency: Target currency
            days: Number of days

        Returns:
            Dict with prices, market_caps, total_volumes arrays
        """
        params = {
            'vs_currency': vs_currency,
            'days': days
        }

        data = self._make_request(f'coins/{coin_id}/market_chart', params)
        return data

    def get_global_data(self) -> Optional[Dict]:
        """
        Fetch global cryptocurrency market data.

        Returns:
            Global market metrics
        """
        logger.info("Fetching global market data...")

        data = self._make_request('global')
        return data.get('data') if data else None

    def get_exchanges(self, per_page: int = 100, page: int = 1) -> List[Dict]:
        """
        Fetch exchange data.

        Args:
            per_page: Results per page
            page: Page number

        Returns:
            List of exchange dictionaries
        """
        logger.info(f"Fetching exchanges (page {page})...")

        params = {
            'per_page': per_page,
            'page': page
        }

        data = self._make_request('exchanges', params)
        return data if data else []

    def run_test_extraction(self, limit: int = 50) -> Dict:
        """
        Run limited test extraction for validation.

        Args:
            limit: Maximum coins to extract

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info(f"STARTING TEST EXTRACTION (limit={limit})")
        logger.info("=" * 60)

        all_data = {
            'coins': [],
            'market_data': [],
            'ohlcv': [],
            'global_metrics': None,
            'exchanges': []
        }

        # Get top coins by market cap
        market_data = self.get_coin_markets(per_page=min(limit, 250))
        all_data['market_data'] = market_data
        self.extraction_log['coins_extracted'] = len(market_data)

        logger.info(f"  Retrieved {len(market_data)} coins with market data")

        # Get OHLCV for top coins (limited)
        top_coins = market_data[:min(10, len(market_data))]
        for coin in top_coins:
            coin_id = coin.get('id')
            if not coin_id:
                continue

            ohlcv = self.get_coin_ohlc(coin_id, days=30)
            if ohlcv:
                for candle in ohlcv:
                    all_data['ohlcv'].append({
                        'coin_id': coin_id,
                        'timestamp': candle[0],
                        'open': candle[1],
                        'high': candle[2],
                        'low': candle[3],
                        'close': candle[4]
                    })

        self.extraction_log['ohlcv_records'] = len(all_data['ohlcv'])
        logger.info(f"  Retrieved {len(all_data['ohlcv'])} OHLCV records")

        # Get global metrics
        all_data['global_metrics'] = self.get_global_data()

        # Get exchanges
        all_data['exchanges'] = self.get_exchanges(per_page=50, page=1)
        logger.info(f"  Retrieved {len(all_data['exchanges'])} exchanges")

        self.extraction_log['end_time'] = datetime.now().isoformat()

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data

    def run_full_extraction(self, pages: int = 20) -> Dict:
        """
        Run full-scale extraction for production.

        Args:
            pages: Number of pages of market data (250 coins/page)

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info("STARTING FULL EXTRACTION")
        logger.info(f"Extracting {pages * 250} coins maximum")
        logger.info("=" * 60)

        all_data = {
            'coins': [],
            'market_data': [],
            'ohlcv': [],
            'global_metrics': None,
            'exchanges': []
        }

        # Get market data for multiple pages
        for page in range(1, pages + 1):
            market_data = self.get_coin_markets(per_page=250, page=page)
            if not market_data:
                break

            all_data['market_data'].extend(market_data)
            logger.info(f"  Page {page}: {len(market_data)} coins")

            # Save intermediate
            if page % 5 == 0:
                intermediate_path = self.output_dir / f"market_data_p{page}.json"
                with open(intermediate_path, 'w') as f:
                    json.dump(all_data['market_data'], f)

        self.extraction_log['coins_extracted'] = len(all_data['market_data'])

        # Get OHLCV for top 100 coins
        top_coins = all_data['market_data'][:100]
        for i, coin in enumerate(top_coins):
            coin_id = coin.get('id')
            if not coin_id:
                continue

            logger.info(f"  Fetching OHLCV for {coin_id} ({i+1}/100)...")
            ohlcv = self.get_coin_ohlc(coin_id, days=365)

            if ohlcv:
                for candle in ohlcv:
                    all_data['ohlcv'].append({
                        'coin_id': coin_id,
                        'symbol': coin.get('symbol'),
                        'timestamp': candle[0],
                        'open': candle[1],
                        'high': candle[2],
                        'low': candle[3],
                        'close': candle[4]
                    })

        self.extraction_log['ohlcv_records'] = len(all_data['ohlcv'])

        # Get global metrics
        all_data['global_metrics'] = self.get_global_data()

        # Get all exchanges (multiple pages)
        for page in range(1, 6):
            exchanges = self.get_exchanges(per_page=100, page=page)
            if not exchanges:
                break
            all_data['exchanges'].extend(exchanges)

        self.extraction_log['end_time'] = datetime.now().isoformat()

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data


if __name__ == "__main__":
    extractor = CryptoDataExtractor()

    # Test extraction
    data = extractor.run_test_extraction(limit=25)

    print("\nExtraction Summary:")
    print(f"  Coins: {len(data['market_data'])}")
    print(f"  OHLCV records: {len(data['ohlcv'])}")
    print(f"  Exchanges: {len(data['exchanges'])}")
    print(f"  API Calls: {extractor.extraction_log['api_calls']}")
