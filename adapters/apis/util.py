"""
Support functions for interfacing with APIs
"""
import backoff
import numpy as np
import requests
from typing import Union

# constants manually extracted from CREAM Finance JS webpack
BLOCKS_PER_YEAR = {
    "eth": 2102400,
    "bsc": 10512e3,
    "ironbank": 2102400,
    "fantom": 31536e3,
    "polygon": 15768e3,
    "arbitrum": 2102400,
    "avalanche": 31536e3,
}
BACKOFF_MAX_TRIES = 8  # With exponential, 7 retries = 30 sec, 8 retries ~1.5 min


@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=BACKOFF_MAX_TRIES
)
def get_url_json(url, headers=None, params=None, timeout=None):
    """
    URL JSON getter, wrapped in an exponential backoff strategy

    Args:
        url: URL to run requests.get on
        headers: headers to pass to get
        params: params to pass to get
        timeout: Time to wait in seconds until timeout

    Returns:
        resp.json: JSON payload

    Raises:
        RequestException: Any request exception, after backoff complete
    """
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def calculate_apy_from_rate(rate: int, blocks_per_year: Union[int, str], mantissa: int):
    """
    Given borrow or supply rate and blocks per year, calculate equivalent APY.
    Ref: https://docs.strike.org/getting-started/protocol-math/calculating-the-apy-using-rate-per-block

    Args:
        rate: borrow rate or supply rate per block
        blocks_per_year: Number of blocks per year as int OR name of protocol to auto-lookup
    """  # NOQA
    if isinstance(blocks_per_year, str):
        blocks_per_year = BLOCKS_PER_YEAR[
            blocks_per_year
        ]  # Fetch from lookup using protocol
    try:
        apy = (
            100
            + ((((rate / mantissa * blocks_per_year / 365 + 1) ** 365 - 1)) - 1) * 100
        )
    except OverflowError:
        apy = np.nan
    return apy
