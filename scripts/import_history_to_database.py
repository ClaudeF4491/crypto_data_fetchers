"""
Given JSON file from history export, inserts each record
into a table as a unique event

Example Usage:
python3 scripts/import_history_to_database.py \
    cream_finance_history.json \
    cream
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
        table: table name to write to
        port: DB port
        password: Password to connect to DB

    Returns:
        None
    """
    # Init
    DataRecord = create_datamodel(table)

    logging.info(f"Loading records from file: {input_path} ...")
    with open(input_path, "r") as f:
        results = json.load(f)
    results = [item for sublist in results for item in sublist]
    logging.info(f"Loaded {len(results)} records.")

    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()
    metadata = {"label": label, "type": "history"}

    logging.info("Creating records to database ...")
    events: List[DataRecord] = list()
    for r in results:
        event = DataRecord(
            timestamp=datetime.fromisoformat(r["date"].split("Z")[0]),
            event=json.dumps(r),
            metadata_=json.dumps(metadata),
        )
        events.append(event)
    session.add_all(events)
    logging.info("Committing records to database ...")
    session.commit()
    logging.info(f"Committed {len(events)} records.")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
