"""
This script fetches data periodically and logs it to a pre-configured database
as raw event data.

Usage Example:
$ python3 scripts/database_logger.py \
    30 \
    user \
    database.url.rds.amazonaws.com \
    database_name \
    3306
where 30 is number of seconds until poll.
"""

from adapters.apis.cream import CreamAdapter
from adapters.database_adapter import DatabaseAdapter, RawEvent
from datetime import datetime
import json
import logging
import time
import typer
from typing import List

app = typer.Typer()


@app.command()
def poll(
    sleep_dur: int,
    username: str,
    hostname: str,
    database: str,
    port: int,
    password: str = typer.Option(None, prompt=True, hide_input=True),
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

    Returns:
        None
    """
    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()
    metadata = {"source": "cream", "type": "poll"}

    while True:
        time.sleep(sleep_dur)  # at beginning to handle continue calls

        # Fetch latest data
        logging.info("Fetching CREAM Finance token states ...")
        try:
            results = CreamAdapter.get_all_current_token_states()
            logging.info(f"Fetched data for {len(results)} tokens.")
        except Exception as e:
            logging.error(f"Unable to fetch data. Error: {e}", exc_info=True)
            continue

        logging.info("Logging results to database ...")
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
