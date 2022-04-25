"""
API to interface with Alpaca Finance public API.
Alpaca is pretty open with their data. It all comes over
a JSON file and contains typically everything needed. Unlike
Alpha Homora, they don't do as many frontend calculations from it.
"""
from adapters.apis.base import BaseAdapter
from adapters.apis.util import get_url_json
from pprint import pprint
from typing import Any, Dict, List


class AlpacaFinanceAdapter(BaseAdapter):
    # Endpoints to fetch live data from Alpha Homora v2
    _json_url = (
        "https://alpaca-static-api.alpacafinance.org/bsc/v1/landing/summary.json"
    )

    @classmethod
    def get(cls) -> List[Dict[str, Any]]:
        """Wrapper to run class generically"""
        return [get_url_json(cls._json_url)]


if __name__ == "__main__":
    print("Fetching pool data ...")
    resp = AlpacaFinanceAdapter.get()
    farming_pools = resp[0]["data"]["farmingPools"]
    print(f"Fetched {len(farming_pools)} pools.")
    print("Sample:")
    pprint(farming_pools[0])
