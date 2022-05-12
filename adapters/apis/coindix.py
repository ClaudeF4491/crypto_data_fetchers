"""
API to interface with CoinDix public API
"""
from adapters.apis.base import BaseAdapter
from adapters.apis.util import get_url_json
import json
import logging
from pathlib import Path
import requests
import time
from tqdm import tqdm
import typer
from typing import Any, Dict, List, Optional, Sequence

app = typer.Typer()


class CoinDixAdapter(BaseAdapter):
    # Endpoints to fetch live data
    _init_url = "https://api.coindix.com/init"
    _search_url = "https://api.coindix.com/search"
    _auth_url = "https://api.coindix.com/users/login"
    _vault_url = "https://api.coindix.com/vaults"
    _search_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "If-None-Match": 'W/"18ef-F3ewVVK3weV89utiLtPtZPpgPLc"',
        "Origin": "https://coindix.com",
        "Referer": "https://coindix.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36",  # NOQA
    }
    _auth_headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://coindix.com",
        "Referer": "https://coindix.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36",  # NOQA
    }
    _state_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": None,
        "Connection": "keep-alive",
        "Origin": "https://coindix.com",
        "Referer": "https://coindix.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36",  # NOQA
    }

    @classmethod
    def authenticate(cls, email: str, password: str) -> Dict[str, str]:
        data = {
            "email": email,
            "password": password,
        }
        resp = requests.post(cls._auth_url, headers=cls._auth_headers, data=data)
        return resp.json()

    @classmethod
    def get(cls, *args, **kwargs):
        """Wrapper to run class generically"""
        return cls.get_all_states(*args, **kwargs)

    @classmethod
    def get_chains(cls, timeout: Optional[float] = None) -> Dict[str, List[str]]:
        data = get_url_json(cls._init_url, headers=cls._search_headers, timeout=timeout)
        chains = data["chains"]
        for k, v in chains.items():
            chains[k] = [vv["hash"] for vv in v]
        return chains

    @classmethod
    def get_vault_history(
        cls, vault: int, token: str, timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        headers = cls._state_headers.copy()
        headers["Authorization"] = f"Bearer {token}"
        params = {"period": 365}
        url = f"{cls._vault_url}/{vault}"
        data = get_url_json(url, params=params, headers=headers, timeout=timeout)
        return data

    @classmethod
    def get_all_vault_histories(
        cls,
        email: str,
        password: str,
        timeout: Optional[float] = None,
        sleep_dur: float = 0.1,
    ) -> List[Dict[str, Any]]:
        resp_auth = cls.authenticate(email, password)
        token = resp_auth["token"]

        # Fetch all states to get vault IDs
        logging.info("Fetching current vault states to get all IDs ...")
        states = cls.get_all_states(timeout=timeout, sleep_dur=sleep_dur)
        logging.info(f"Retrieved a total of {len(states)} vault states.")
        vault_ids = [s["id"] for s in states]

        # Given vault IDs, fetch all histories (can take a while, like ~1hr or more)
        vault_histories = list()
        logging.info(f"Fetching history for {len(vault_ids)} vaults ...")
        for vault_id in tqdm(vault_ids):
            data = cls.get_vault_history(vault_id, token, timeout=timeout)
            vault_histories.append(data)
            time.sleep(sleep_dur)

        return vault_histories

    @classmethod
    def get_all_states(
        cls, timeout: Optional[float] = None, sleep_dur: float = 0.1
    ) -> List[Dict[str, Any]]:
        # Get all chains so that we don't hit the max return in paginated results
        chains = cls.get_chains(timeout=timeout)
        logging.info(f"Found {len(chains)} to query.")

        states = list()
        for chain in tqdm(chains.keys()):
            logging.info(f"Fetching current vault states for chain: {chain} ...")
            protocols = chains[chain]
            for protocol in protocols:
                cur_states = CoinDixAdapter.get_current_states(
                    chains=[chain], protocols=[protocol]
                )
                if len(cur_states) > 0:
                    states += cur_states
                logging.info(
                    f"Retrieved {len(cur_states)} vault states for chain={chain}, "
                    f"protocol={protocol}"
                )
        return states

    @classmethod
    def get_current_states(
        cls,
        chains: Optional[Sequence[str]] = None,
        protocols: Optional[Sequence[str]] = None,
        timeout: Optional[float] = None,
        sleep_dur: float = 0.1,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve current state for all pools derived from Coindix search URL

        Args:
            timeout: time in seconds to wait before timeout occurs

        Returns:
            apy_data: APY (total, trading, farming) data broken out by pool
        """
        params = {"sort": "-tvl", "page": 1}
        if chains:
            chains_str = "-".join(chains)
            params["chain"] = chains_str
        if protocols:
            protocols_str = "-".join(protocols)
            params["protocol"] = protocols_str
        has_next_page = True
        states = list()
        cnt = 1
        while has_next_page:
            resp = get_url_json(
                cls._search_url,
                headers=cls._search_headers,
                params=params,
                timeout=timeout,
            )
            data = resp.get("data")
            if data is not None and len(data) > 0:
                states += data
                if cnt == 1:
                    total_pages = resp.get("totalPages")
                    total_vaults = resp.get("total")
                    logging.info(f"Fetching {total_vaults} across {total_pages} pages.")
            cnt += 1
            time.sleep(sleep_dur)
            params["page"] = cnt
            has_next_page = resp["hasNextPage"]
        return states


@app.command()
def download_all_vault_histories(
    email: str,
    password: str,
    output_file: Path = f"coindix_vault_histories_{int(time.time())}.json",
):
    logging.info("Fetching all vault histories ...")
    data = CoinDixAdapter.get_all_vault_histories(email, password)
    logging.info(f"Retrieved {len(data)} vault histories.")
    logging.info(f"Saving results to: {output_file}.")
    with open(output_file, "w") as f:
        json.dump(data, f)
    logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
