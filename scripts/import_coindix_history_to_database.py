"""
Given JSON file from history export, inserts each record
into a table as a unique event

Example Usage:
python3 scripts/import_coindix_history_to_database.py \
    coindix_finance_history.json \
    coindix
    admin \
    db.rds.amazonaws.com \
    test_db \
    test_table \
    3306
"""
from adapters.database_adapter import DatabaseAdapter
from datetime import datetime
import json
import logging
import pandas as pd
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import typer
from typing import List

app = typer.Typer()
Base = declarative_base()


def create_datamodel(tablename: str) -> Base:
    """Dynamically creates sqlalchemy data model, defining table name"""

    class DataRecord(Base):
        """SQLAlchemy object to represent a raw event."""

        __tablename__ = tablename
        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime)
        event = Column(String)
        metadata_ = Column("metadata", String)
        source = Column(String)

    return DataRecord


@app.command()
def insert(
    input_path: str,
    label: str,
    username: str,
    hostname: str,
    database: str,
    table: str,
    port: int,
    password: str = typer.Option(None, prompt=True, hide_input=True),
) -> None:
    """
    Loads data from a file, and stores in a database

    Args:
        input_path: Name of JSON file to load
        label: label to add to metadata record column
        username: Username to connect to DB
        hostname: hostname of DB
        database: database to connect to
        table: raw table to write to
        port: DB port
        password: Password to connect to DB

    Returns:
        None
    """
    # Init
    DataRecord = create_datamodel(table)

    logging.info(f"Loading records from file: {input_path} ...")
    with open(input_path, "r") as f:
        raw_data = json.load(f)
    logging.info(f"Loaded {len(raw_data)} raw records.")

    # Unnest timeseries
    results = list()
    for r in raw_data:
        rr = r.copy()
        rr.pop("series")
        for s in r.get("series"):
            record = {**rr, **s}
            results.append(record)

    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()
    metadata = {"label": label, "type": "history"}
    source = "coindix"

    # Save off raw events, grouping-by-date into list of lists
    df = pd.DataFrame(results)
    inds_by_date = df.groupby("date").indices
    logging.info("Creating raw event objects ...")
    events: List[DataRecord] = list()
    for dt, inds in inds_by_date.items():
        # Create a raw event
        group_events = [results[i] for i in inds]
        # Remove date since not in real-time pull
        for g in group_events:
            g.pop("date")
        # Create event
        event = DataRecord(
            timestamp=datetime.fromisoformat(dt.split("Z")[0]),
            event=json.dumps([group_events]),
            metadata_=json.dumps(metadata),
            source=source,
        )
        events.append(event)

    # Save raw events
    logging.info(f"Bulk saving {len(events)} event records ...")
    session.bulk_save_objects(events)
    logging.info(f"Committing {len(events)} raw event records to database ...")
    session.commit()
    logging.info(f"Committed {len(events)} records.")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
