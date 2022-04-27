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
from adapters.database_adapter import CreamFinanceState, DatabaseAdapter
from datetime import datetime
import json
import logging
import numpy as np
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
        results = json.load(f)
    results = [item for sublist in results for item in sublist]
    logging.info(f"Loaded {len(results)} records.")

    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()
    metadata = {"label": label, "type": "history"}

    # Save off state objects
    states: List[CreamFinanceState] = list()
    logging.info("Creating state objects ...")
    for r in results:
        decimals = int(r["underlyingDecimals"])
        # Create a structured event
        cream_state = CreamFinanceState(
            timestamp=datetime.fromisoformat(r["date"].split("Z")[0]),
            address=r["address"],
            comptroller=r["comptroller"],
            symbol=r["symbol"],
            underlying_symbol=r["underlying_symbol"],
            underlyingDecimals=decimals,
        )

        # Everything else is nullable, cast if applicable
        # TODO: Wow, this is ugly. Hardcoded copy-pasta. Make generic somehow
        t = r.get("borrow_apy")
        if t:
            cream_state.borrow_apy = float(t)
            if np.isnan(cream_state.borrow_apy):
                cream_state.borrow_apy = None

        t = r.get("supply_apy")
        if t:
            cream_state.supply_apy = float(t)
            if np.isnan(cream_state.supply_apy):
                cream_state.supply_apy = None

        t = r.get("utilization_rate")
        if t:
            cream_state.utilization_rate = float(t)
            if np.isnan(cream_state.utilization_rate):
                cream_state.utilization_rate = None

        t = r.get("cash")
        if t:
            cream_state.cash = float(int(t) / 10**decimals)
            if np.isnan(cream_state.cash):
                cream_state.cash = None

        t = r.get("cashUSD")
        if t:
            cream_state.cashUSD = float(t)
            if np.isnan(cream_state.cashUSD):
                cream_state.cashUSD = None

        t = r.get("totalBorrows")
        if t:
            cream_state.totalBorrows = float(int(t) / 10**decimals)
            if np.isnan(cream_state.totalBorrows):
                cream_state.totalBorrows = None

        t = r.get("totalBorrowsUSD")
        if t:
            cream_state.totalBorrowsUSD = float(t)
            if np.isnan(cream_state.totalBorrowsUSD):
                cream_state.totalBorrowsUSD = None

        t = r.get("totalReserves")
        if t:
            cream_state.totalReserves = float(int(t) / 10**decimals)
            if np.isnan(cream_state.totalReserves):
                cream_state.totalReserves = None

        t = r.get("totalReservesUSD")
        if t:
            cream_state.totalReservesUSD = float(t)
            if np.isnan(cream_state.totalReservesUSD):
                cream_state.totalReservesUSD = None

        t = r.get("borrowRatePerBlock")
        if t:
            cream_state.borrowRatePerBlock = float(int(t) / 10**decimals)
            if np.isnan(cream_state.borrowRatePerBlock):
                cream_state.borrowRatePerBlock = None

        t = r.get("supplyRatePerBlock")
        if t:
            cream_state.supplyRatePerBlock = float(int(t) / 10**decimals)
            if np.isnan(cream_state.supplyRatePerBlock):
                cream_state.supplyRatePerBlock = None

        t = r.get("exchangeRate")
        if t:
            cream_state.exchangeRate = float(int(t) / 10**decimals)
            if np.isnan(cream_state.exchangeRate):
                cream_state.exchangeRate = None

        states.append(cream_state)

    # Save states
    session.bulk_save_objects(states)
    logging.info(f"Committing {len(states)} state records to database ...")
    session.commit()
    logging.info(f"Committed {len(states)} records.")

    # Save off raw events
    logging.info("Creating raw event objects ...")
    events: List[DataRecord] = list()
    for r in results:
        # Create a raw event
        event = DataRecord(
            timestamp=datetime.fromisoformat(r["date"].split("Z")[0]),
            event=json.dumps(r),
            metadata_=json.dumps(metadata),
        )
        events.append(event)

    # Save raw events
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
