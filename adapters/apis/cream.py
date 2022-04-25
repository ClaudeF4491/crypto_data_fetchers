"""
API to interface with CREAM Finance public API
"""
from adapters.apis.base import BaseAdapter
from adapters.apis.util import calculate_apy_from_rate, get_url_json
from datetime import datetime
from joblib import Parallel, delayed
from pprint import pprint
from typing import Any, Dict, List, Optional, Sequence


class CreamAdapter(BaseAdapter):
    # Endpoints to fetch all active cr-tokens and their current data
    _current_token_state_url = "https://api.cream.finance/api/v1/crtoken"
    _history_url = "https://api.cream.finance/api/v1/history"
    # List of default comptrollers
    _comptrollers = (
        "eth",
        "bsc",
        "ironbank",
        "fantom",
        "polygon",
        "arbitrum",
        "avalanche",
    )
    # Default mantissa for borrow and supply rate per block. Seems to be consistent
    # for all tokens
    _mantissa = 10**18

    @classmethod
    def get(cls, *args, **kwargs) -> List[List[Dict[str, Any]]]:
        """Wrapper to run class generically."""
        return cls.get_all_current_token_states(*args, **kwargs)

    @classmethod
    def get_current_token_states_by_comptroller(
        cls, comptroller: str, timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Retrieve current state for all tokens. The contents differ
        from the historical API. Enrich with timestamp as well.

        Args:
            comptroller: Which comptroller to fetch from. See _comptrollers for
                options
            timeout: time in seconds to wait before timeout occurs
        """
        token_states = get_url_json(
            cls._current_token_state_url,
            params={"comptroller": comptroller},
            timeout=timeout,
        )

        # Enrich with a timestamp, UTC ISO-8601 and others
        timestamp = datetime.utcnow().isoformat()
        for t in token_states:
            t["timestamp"] = timestamp
            t["comptroller"] = comptroller
            total_supply = float(t["total_borrows"]["value"]) + float(
                t["cash"]["value"]
            )
            t["utilization_rate"] = None
            if total_supply > 0:
                t["utilization_rate"] = (
                    float(t["total_borrows"]["value"]) / total_supply * 100
                )

        return token_states

    @classmethod
    def get_all_current_token_states(
        cls,
        comptrollers: Optional[Sequence[str]] = None,
        n_jobs: int = -1,
        timeout: Optional[int] = None,
        verbose: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Parallel wrapper for get_current_token_states_by_comptroller.
        Fetches all token states for given comptroller in parallel.

        Args:
            comptrollers: List of comptrollers to iterate over. If None,
                uses all defaults
            n_jobs: Number of jobs for joblib
            timeout: Timeout in seconds for request.get
            verbose: Verbosity level for joblib

        Returns:
            all_token_states: List of token states
        """
        # Initialize
        parallel_pool = Parallel(n_jobs=n_jobs, backend="threading", verbose=verbose)
        if isinstance(comptrollers, str):
            comptrollers = [comptrollers]  # force to list
        if not comptrollers:
            comptrollers = cls._comptrollers  # use defaults

        # Set up functions to parallelize
        delayed_funcs = list()
        for c in comptrollers:
            d_fn = delayed(cls.get_current_token_states_by_comptroller)(c, timeout)
            delayed_funcs.append(d_fn)

        # Parallel fetch all
        all_token_states = parallel_pool(delayed_funcs)

        # Flatten
        all_token_states = [item for sublist in all_token_states for item in sublist]

        return all_token_states

    @classmethod
    def get_token_history(
        cls, token_address: str, comptroller: str, timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Gets history based on token address and comptroller.

        Args:
            token_address: Address of token to get all history for
            comptroller: Which comptroller to fetch from
            timeout: time in seconds to wait before timeout occurs

        Returns
            token_history: History fetched for token
        """
        token_history = get_url_json(
            cls._history_url,
            params={"comptroller": comptroller, "ctoken": token_address},
            timeout=timeout,
        )

        # Enrich each record with some derived fields
        for t in token_history:
            t["comptroller"] = comptroller
            total_supply = float(t["totalBorrows"]) + float(t["cash"])
            t["utilization_rate"] = None
            if total_supply > 0:
                t["utilization_rate"] = float(t["totalBorrows"]) / total_supply * 100

        return token_history

    @classmethod
    def get_all_token_histories(
        cls,
        comptrollers: Optional[Sequence[str]] = None,
        n_jobs: int = -1,
        timeout: Optional[int] = None,
        verbose: int = 0,
    ) -> List[List[Dict[str, Any]]]:
        """
        Parallel wrapper for get_token_history. Finds all tokens for each
        comptroller and fetches history for each one.

        Args:
            comptrollers: List of comptrollers to iterate over.
                If None, uses all defaults
            n_jobs: Number of jobs for joblib
            timeout: Timeout in seconds for request.get
            verbose: Verbosity level for joblib

        Returns:
            token_histories: List of token histories
        """
        # Initialize
        parallel_pool = Parallel(n_jobs=n_jobs, backend="threading", verbose=verbose)
        if isinstance(comptrollers, str):
            comptrollers = [comptrollers]  # force to list
        if not comptrollers:
            comptrollers = cls._comptrollers  # use defaults

        # First fetch all token info so that we can get the addresses
        all_token_states = cls.get_all_current_token_states(
            comptrollers, n_jobs=n_jobs, timeout=timeout, verbose=verbose
        )

        # Set up functions to parallelize to fetch history for all tokens
        delayed_funcs = list()
        for token in all_token_states:
            d_fn = delayed(cls.get_token_history)(
                token["token_address"], token["comptroller"], timeout
            )
            delayed_funcs.append(d_fn)

        # Parallel fetch all
        token_histories = parallel_pool(delayed_funcs)

        # Enrich with supporting fields, aligning with token_states
        # to use fields from there for enrichment
        empty_inds = list()
        for i, (ts, th) in enumerate(zip(all_token_states, token_histories)):
            if len(th) == 0:
                empty_inds.append(i)
                continue
            for el in th:
                comp = ts["comptroller"]
                el["comptroller"] = comp
                el["underlying_symbol"] = ts["underlying_symbol"]
                borrow_rate_block = int(el["borrowRatePerBlock"])
                supply_rate_block = int(el["supplyRatePerBlock"])
                el["borrow_apy"] = calculate_apy_from_rate(
                    borrow_rate_block, comp, cls._mantissa
                )
                el["supply_apy"] = calculate_apy_from_rate(
                    supply_rate_block, comp, cls._mantissa
                )

        # Remove empties
        token_histories = [
            el for i, el in enumerate(token_histories) if i not in empty_inds
        ]

        return token_histories


if __name__ == "__main__":

    # Config
    comptroller = "avalanche"

    # Fetch token states for single comptroller
    print("Fetching current token states from comptroller={comptroller} ...")
    avax_token_states = CreamAdapter.get_current_token_states_by_comptroller(
        comptroller
    )

    # for use later
    test_token = avax_token_states[0]
    print("Example output getting token states by comptroller:")
    print(f"Fetched: {len(avax_token_states)} records.")
    print("Example record:")
    pprint(test_token)

    # Fetch all token current states
    print("Fetching all current token states across all comptrollers ...")
    all_token_states = CreamAdapter.get_all_current_token_states()
    print(f"Fetched {len(all_token_states)} records.")
    print("Example of the most recent sample from the last token fetched:")
    pprint(all_token_states[-1])

    # Fetch history of one token
    sym = test_token["underlying_symbol"]
    addr = test_token["token_address"]
    comp = test_token["comptroller"]
    print(f"Fetching token history for token {sym} at address {addr} ...")
    token_history = CreamAdapter.get_token_history(addr, comp)
    print(f"Fetched {len(token_history)} history samples for token.")
    print("Example history sample:")
    pprint(token_history[-1])

    # Fetch all history of all tokens
    print(
        "Fetching all tokens and all history for them "
        "(this may take a minute or two) ..."
    )
    all_histories = CreamAdapter.get_all_token_histories(verbose=10)
    print(
        f"Fetched {len(all_histories)} records. "
        f"This is every history record of every token."
    )
    print("Example sample:")
    pprint(all_histories[-1][-1])
