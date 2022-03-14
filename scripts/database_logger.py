"""
This script fetches data periodically and logs it to a pre-configured database
as raw event data.

Usage Example:
$ python3 scripts/database_logger.py \
    30 \
    user \
    database.url.rds.amazonaws.com \
    database_name \
    3306 \
    --discord-webhook-url [OPTIONAL_DISCORD_WEBHOOK]
where 30 is number of seconds until poll.
"""

from adapters.apis.cream import CreamAdapter
from adapters.apis.alpha_homora import AlphaHomoraAdapter
from adapters.database_adapter import DatabaseAdapter, RawEvent
from datetime import datetime
from interfaces.handlers import DiscordHandler
import json
import logging
import time
import typer
from typing import List, Optional

app = typer.Typer()

# Define a list of available API interfaces, keyed on a string label
# and mapped to the requests functiont o call
API_INTERFACES = {
    "cream": CreamAdapter.get_all_current_token_states,
    "alpha_homora_apy": AlphaHomoraAdapter.get_apy,
    "alpha_homora_positions": AlphaHomoraAdapter.get_positions,
    "alpha_homora_pools": AlphaHomoraAdapter.get_pools,
}


@app.command()
def poll(
    sleep_dur: int,
    username: str,
    hostname: str,
    database: str,
    port: int,
    password: str = typer.Option(None, prompt=True, hide_input=True),
    interfaces: Optional[List[str]] = typer.Option(list(API_INTERFACES.keys())),
    discord_webhook_url: str = typer.Option(None),
    discord_username: str = typer.Option("CryptoDataFetcher"),
) -> None:
    """
    Polls periodically to retrieve data from API and save it to database.

    Args:
        sleep_dur: Number of seconds to sleep between polls
        username: Username to connect to DB
        hostname: hostname of DB
        database: database to connect to
        port: DB port
        password: Password to connect to DB
        interfaces: List of API interfaces to fetch data from. See keys of
            `API_INTERFACES` for options. Defaults to all available keys.
        discord_webhook_url: Optional webhook to push logging.error() log
            events to Discord channel
        discord_username: Username to use when sending messages to Discord
            channel

    Returns:
        None
    """
    # Initialize Discord notifier
    if discord_webhook_url:
        logging.getLogger().addHandler(
            DiscordHandler(
                discord_webhook_url, logging.ERROR, {"username": discord_username}
            )
        )

    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()

    first_run = True
    while True:
        if first_run:
            first_run = False
        else:
            time.sleep(sleep_dur)  # at beginning to handle continue calls

        # Fetch latest data
        for interface in interfaces:
            fn = API_INTERFACES[interface]
            logging.info(f"Fetching {interface} data via function: {fn} ...")
            try:
                results = fn()
                logging.info(f"Fetched {len(results)} records.")
            except Exception as e:
                logging.error(f"Unable to fetch data. Error: {e}", exc_info=True)
                continue

            logging.info("Logging results to database ...")
            metadata = {"source": interface, "type": "poll"}
            # Create event object
            try:
                timestamp = datetime.utcnow()
                events: List[RawEvent] = list()
                for r in results:
                    event = RawEvent(
                        timestamp=timestamp,
                        event=json.dumps(r),
                        metadata_=json.dumps(metadata),
                    )
                    events.append(event)
                session.add_all(events)
                session.commit()
                logging.info(f"Logged {len(events)} records.")
            except Exception as e:
                logging.error(f"Unable to log to database. Error: {e}", exc_info=True)
                continue


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
