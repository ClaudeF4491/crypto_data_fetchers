"""
API to interface with Alpha Homora public API
"""
from adapters.apis.base import BaseAdapter
from adapters.apis.util import get_url_json
from pprint import pprint
from typing import Any, Dict, List, Optional


class AlphaHomoraAdapter(BaseAdapter):
    # Endpoints to fetch live data from Alpha Homora v2
    _apy_url = "https://homora-api.alphafinance.io/v2/43114/apys"
    _positions_url = "https://homora-api.alphafinance.io/v2/43114/positions"
    _pools_url = "https://homora-api.alphafinance.io/v2/43114/pools"

    @classmethod
    def get(cls, *args, **kwargs):
        """Wrapper to run class generically"""
        return cls.get_all_token_histories(*args, **kwargs)

    @classmethod
    def get_apy(cls, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Retrieve current APY for all pools.

        Args:
            timeout: time in seconds to wait before timeout occurs

        Returns:
            apy_data: APY (total, trading, farming) data broken out by pool
        """
        apy_data = get_url_json(cls._apy_url, timeout=timeout)
        # Convert to list output
        tlist = list()
        for k, v in apy_data.items():
            el = v
            el["pool"] = k
            tlist.append(el)
        apy_data = tlist
        return apy_data

    @classmethod
    def get_positions(cls, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Retrieve current status of all positions

        Args:
            timeout: time in seconds to wait before timeout occurs

        Returns:
            position_data: List of positions and their current status
        """
        position_data = get_url_json(cls._positions_url, timeout=timeout)
        return position_data

    @classmethod
    def get_pools(cls, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Retrieve current status of all pools from Alpha Homora

        Args:
            timeout: time in seconds to wait before timeout occurs

        Returns:
            pool_data: List of pools and their current status
        """
        pool_data = get_url_json(cls._pools_url, timeout=timeout)
        return pool_data


if __name__ == "__main__":
    print("Fetching APY data ...")
    apy_data = AlphaHomoraAdapter.get_apy()
    print(f"Fetched {len(apy_data)} APY records.")
    print("Sample:")
    pprint(list(apy_data.items())[0])

    print("Fetching positions data ...")
    position_data = AlphaHomoraAdapter.get_positions()
    print(f"Fetched {len(position_data)} position records.")
    print("Sample:")
    pprint(position_data[0])

    print("Fetching pool data ...")
    pool_data = AlphaHomoraAdapter.get_pools()
    print(f"Fetched {len(pool_data)} pool records.")
    print("Sample:")
    pprint(pool_data[0])
