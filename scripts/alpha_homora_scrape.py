"""
Alpha Homora Site Scraper

Uses Selenium to fetch pool and APY data from Alpha Homora V2.
See README for setup.

WARNING: DO NOT USE A REAL WEB3 ACCOUNT. Make a dummy one.
This app only fetches data. It does NOT need a wallet with actual
funds in it.

Note: This script has too many parameters to be all CLI. So it uses
a config file for non-password parameters.

"""
from adapters.database_adapter import DatabaseAdapter, AlphaHomoraPool
from interfaces.browser import (
    init_driver_firefox,
    load_wallet_metamask,
    load_url_and_connect_metamask,
)
from interfaces.handlers import DiscordHandler
import jsonlines
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Union
from time import sleep
import typer
import yaml

app = typer.Typer()

# Globals
URL_SCRAPE = "https://homora-v2.alphaventuredao.io/farm-pools"


# Hardcoded mappings. WARNING: This can change if site changes
# Provides text lookup for different data fields
FIELD_MAP = {
    "leverage_maxi": "Achieved from",
    "tvl_pool": "Pool TVL",
    "apr_max": "Maximum APR",
    "leverage_highest_apr": "Achieved from",
    "apy_trading_fee": "Trading Fee APY",
    "apr_farming": "Yield Farming APR",
    "apr_reward": "Reward APR",
    "apy_borrow": "Borrowing Interest",
    "trading_volume_24h": "Trading Volume (24h)",
    "positions": "Positions",
    "tvl_homora": "TVL via Homora v2",
}

# Labels that indicate the start of a row
# WARNING: Can change if site changes
START_KEYS = ("Yield Farming", "Liquidity Providing")


# click through to diffeerent network
def click_chain(driver, chain_name, chains=("Ethereum", "Fantom", "Avalanche")):
    # Store current window before doing stuff
    cur_handle = driver.current_window_handle

    # Click network change button at top.
    for c in chains:
        try:
            driver.find_element_by_xpath(f'//span[text()="{c}"]').click()
            break
        except Exception as e:  # NOQA
            sleep(0.5)
            continue

    # Select the chain
    driver.find_element_by_xpath(f'//p[text()="{chain_name}"]').click()
    sleep(1)

    # Try going to metamask if it pops up
    sleep(2)  # wait a moment for metamask window to load
    driver.switch_to.window(driver.window_handles[-1])

    # Click metamask if it's there
    try:
        driver.find_element_by_xpath('//button[text()="Approve"]').click()
    except Exception as e:  # NOQA
        pass
    try:
        driver.find_element_by_xpath('//button[text()="Switch network"]').click()
    except Exception as e:  # NOQA
        pass

    # Switch back to main window
    driver.switch_to.window(cur_handle)


def expand_details(driver, wait_sec=0.5):
    """Expand all the detail buttons"""
    detail_buttons = driver.find_elements_by_xpath('//span[text()="See details"]')
    for i, d in enumerate(detail_buttons):
        try:
            # Click "slowly" since it can act funny otherwise
            sleep(wait_sec)
            d.click()
        except Exception as e:  # NOQA
            pass


def get_row_text(driver):
    """Retrieves raw row text"""
    els = driver.find_elements_by_xpath('//*[contains(@class, "MuiTypography-noWrap")]')
    els_text = list()
    for el in els:
        els_text.append(el.text)
    return els_text


def check_page_loaded(els_text):
    """Page isn't loaded if % and $ don't appear in the row text"""
    return any(["%" in s for s in els_text]) and any(["$" in s for s in els_text])


def parse_text_field(t: str):
    """
    Parses a single field in the row.
    WARNING: If site changes, this can break

    Args:
        t: text field to convert to structured values

    Returns:
        t: text field, parsed
    """
    t = t.replace("%", "").replace("$", "").replace(",", "")
    t = t.replace("From ", "")
    t = t.replace(" up to", "")
    t = t.strip()
    if len(t) < 7 and t[-1] == "x":
        t = t[:-1]
    try:
        t = float(t)
    except:  # NOQA
        pass
    return t


def parse_row(row_fields: List[str]) -> Dict[str, Union[str, float]]:
    """
    Converts raw scraped row text into dictionary with extracted data
    WARNING: If site changes, this can break

    Args:
        row_fields: List of strings pulled from site for each row,
            single chunk from `get_row_text()`

    Returns:
        d: Parsed dict with data extracted and transformed from text
            i.e. single pool
    """
    # Fill in the main headings
    d = {
        "strategy": row_fields[0],
        "pool": row_fields[1],
        "protocol": row_fields[2],
        "apr_min": parse_text_field(row_fields[3]),
        "leverage_min": parse_text_field(row_fields[5]),
        "leverage_max": parse_text_field(row_fields[6]),
    }

    # Try to find the rest
    for label, fieldname in FIELD_MAP.items():
        d[label] = get_field(row_fields, fieldname)
    return d


def get_field(chunk: List[str], s: str) -> Union[str, float, None]:
    """
    Search list of strings for a string and returns the following
    element, parsed.

    Args:
        chunk: List of strings to parse

    Returns:
        field: data field, fetched
    """
    field = None
    for i, t in enumerate(chunk):
        if s == t:
            try:
                field = parse_text_field(chunk[i + 1])
            except:  # NOQA
                pass
            return field


def parse_rows(all_row_text: List[str]) -> List[Dict[str, Union[str, float]]]:
    """
    Iterates through each row of data and parses into list of data objects

    Args:
        all_row_text: Output from `get_row_text()`

    Returns:
        data: All rows of data as a list (i.e. all pools)
    """
    data = list()

    start_inds = list()
    for i, t in enumerate(all_row_text):
        if t in START_KEYS:
            start_inds.append(i)

    for i in range(len(start_inds)):
        i_start = start_inds[i]
        if i < len(start_inds) - 1:
            i_stop = start_inds[i + 1]
        else:
            i_stop = len(all_row_text)
        t = all_row_text[i_start:i_stop]
        d = parse_row(t)
        data.append(d)
    return data


def pool_dict_to_obj(d: Dict[str, Union[str, float, int]]) -> AlphaHomoraPool:
    """Converts dictionary to object using gets to allow nulls"""
    # First make sure no weird NaN values by checking x != x
    # Ref: https://stackoverflow.com/a/944712
    for k, v in d.items():
        if v != v:
            # When nan, set to None
            d[k] = None
    
    # Create record
    record = AlphaHomoraPool(
        timestamp=d.get("timestamp"),
        chain=d.get("chain"),
        strategy=d.get("strategy"),
        pool=d.get("pool"),
        protocol=d.get("protocol"),
        leverage_min=d.get("leverage_min"),
        leverage_max=d.get("leverage_max"),
        leverage_highest_apr=d.get("leverage_highest_apr"),
        apr_min=d.get("apr_min"),
        apr_max=d.get("apr_max"),
        apy_trading_fee=d.get("apy_trading_fee"),
        apr_farming=d.get("apr_farming"),
        apr_reward=d.get("apr_reward"),
        apy_borrow=d.get("apy_borrow"),
        trading_volume_24h=d.get("trading_volume_24h"),
        tvl_pool=d.get("tvl_pool"),
        tvl_homora=d.get("tvl_homora"),
        positions=d.get("positions"),
    )
    return record


@app.command()
def scrape(
    config_file: Path,
    wallet_seed: str = typer.Option(None, prompt=True, hide_input=True),
    wallet_password: str = typer.Option(None, prompt=True, hide_input=True),
    db_password: str = typer.Option(None, prompt=True, hide_input=True),
    discord_webhook_url: str = typer.Option(None),
    once: bool = typer.Option(False),
) -> None:
    """
    Periodically scrapes Alpha Homora page using Selenium + Metamask
    extension, and then optionally saves it to database and/or file.

    See config file for additional parameters.

    Args:
        config_file: Path to YAML config file
        wallet_seed: Wallet seed to use to load Metamask.
            See warning at top of module
        wallet_password: Password to assign to wallet.
            See warning at top of module
        db_password: Password to connect to DB
        discord_webhook_url: Optional webhook to push logging.error() log
            events to Discord channel
        once: Flag to only run once if set to True. If False, runs forever

    Returns:
        None
    """
    # Parse config
    config = yaml.load(config_file.read_text(), Loader=yaml.Loader)
    extension_path = str(Path(config["selenium"]["metamask"]["path"]).resolve())
    executable_path = config["selenium"]["executable_path"]
    output_file = None
    if config["output"]["enabled"]:
        output_file = config["output"]["filename"]
    chains_to_check = config["alpha_homora"]["chains"]
    fetch_sleep = config["scrape"]["sleep"]["fetch"]
    load_sleep = config["scrape"]["sleep"]["load"]
    chain_sleep = config["scrape"]["sleep"]["chain"]
    max_waits = config["scrape"]["max_waits"]
    discord_username = config["discord"]["username"]

    # Initialize Discord notifier
    if discord_webhook_url:
        logging.info("Initialize Discord handler.")
        logging.getLogger().addHandler(
            DiscordHandler(
                discord_webhook_url, logging.ERROR, {"username": discord_username}
            )
        )

    # Initialize database
    db_config = config["database"]
    db = None
    if db_config["enabled"]:
        logging.info("Initializing database connection.")
        # Create database adapter
        hostname = db_config["hostname"]
        username = db_config["username"]
        database = db_config["database"]
        port = db_config["port"]
        db = DatabaseAdapter(hostname, username, db_password, database, port)

        # Intialize a session
        session = db.create_session()

    logging.info("Start the Web Driver and load Metamask extension.")
    driver = init_driver_firefox(
        executable_path=executable_path, extension_paths=[extension_path]
    )

    # Wait for metamask to load, and switch to that
    sleep(10.0)
    logging.info("Initializing Metamask settings and loading wallet ...")
    driver.switch_to.window(driver.window_handles[-1])
    load_wallet_metamask(driver, wallet_seed, wallet_password)

    # Switch and fill out main
    logging.info("Metamask wallet loaded. Loading URL and connecting Metamask to site.")
    base_handle = driver.window_handles[0]
    driver.switch_to.window(base_handle)
    load_url_and_connect_metamask(driver, URL_SCRAPE)

    # Mega loop to do the parsing
    logging.info("Initialization complete!")
    enabled = True
    while enabled:
        if once:
            # Disable after first run if not looping forever
            enabled = False
        data = list()

        # Check each chain
        for chain in chains_to_check:
            data_chain = list()
            timestamp = datetime.utcnow().isoformat()
            logging.info(f"Fetching data for chain={chain} at timestamp={timestamp}")

            # Switch chains
            try:
                click_chain(driver, chain)
            except Exception as e:  # NOQA
                logging.error(
                    f"Unable to click to chain: {chain}. Error: {e}. Skipping."
                )
                sleep(chain_sleep)
                continue

            # Pause a moment and refresh
            sleep(2)

            # Refresh. It can be slow to generate stuff
            try:
                logging.info(
                    f"\tLoading page and waiting {load_sleep} seconds since takes "
                    f"a moment for data to populate."
                )
                driver.get(URL_SCRAPE)
                sleep(load_sleep)
            except Exception as e:  # NOQA
                logging.error(
                    f"Unable to reload url: {URL_SCRAPE}. Error: {e}. Skipping."
                )
                sleep(chain_sleep)
                continue

            # Fetch the grid data
            wait_cnt = 0
            try:
                while wait_cnt < max_waits:
                    # Click on each tab to expand details
                    expand_details(driver)

                    # Get text of each pool row
                    els_text = get_row_text(driver)

                    # Decide if page fully loaded
                    if check_page_loaded(els_text):
                        break
                    else:
                        logging.info(
                            f"\tPage not loaded yet. Trying again in "
                            f"{load_sleep} seconds."
                        )
                        wait_cnt += 1
                        sleep(load_sleep)

                if wait_cnt == max_waits:
                    logging.warning(
                        "\tReached max waits. Giving up and skipping this chain."
                    )
                    continue

                # Loop through generating objects
                logging.info(f"\tFound {len(els_text)} raw text elements. Parsing.")
                data_chain = parse_rows(els_text)

                # Enrich each with global fields
                for d in data_chain:
                    d["chain"] = chain.lower()
                    d["timestamp"] = timestamp

            except Exception as e:  # NOQA
                logging.error(
                    f"Something bad happend while parsing. Skipping and moving along. "
                    f"Error: {e}."
                )

            # Add to main list
            logging.info(f"\tSuccessfully parsed {len(data_chain)} pools for {chain}.")

            # Add to this loop's data
            data += data_chain

            # wait before next chain
            sleep(chain_sleep)

        # Optionally save to file
        if output_file:
            logging.info(f"Adding {len(data)} records to {output_file}.")
            with jsonlines.open(output_file, mode="a") as writer:
                writer.write_all(data)

        # Optionally save to DB
        if db:
            logging.info(f"Writing {len(data)} records to database.")
            try:
                records = list()
                for d in data:
                    record = pool_dict_to_obj(d)
                    records.append(record)
                session.bulk_save_objects(records)
                session.commit()
            except Exception as e:  # NOQA                
                logging.error(
                    f"Something bad happend while trying to write records. "
                    f"Skipping and moving along. Error: {e}."
                )                

        # Wait until next cycle to check
        logging.info("Scrape complete!")
        if enabled:
            logging.info(f"Sleeping for {fetch_sleep} seconds ...")
            sleep(fetch_sleep)

    # Cleanup
    driver.quit()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
