"""
Adapters for external databases via SQLAlchemy ORM.
"""
import json
import logging
from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus  # Needed for credential conditioning
import typer

app = typer.Typer()
Base = declarative_base()


def record_to_str(o: Base) -> str:
    """
    Creates string representation of SQLAlchemy record.
    TODO: consider placing this as __str__ definition in Base for all
    SQLAlchemy objects to inheerit.

    Args:
        o: SQLAlchemy record object

    Returns:
        s: String representation
    """
    d = {}
    for c in o.__table__.columns:
        d[c.name] = getattr(o, c.name)
    return str(d)


class DatabaseAdapter:
    """
    Convenience adapter to initialize database connections and execute
    SQLAlchemy queries so that database specifics are abstracted from
    user.
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        database: str,
        port: str,
    ) -> None:
        """
        Initializes a SQLAlchemy database connection engine.

        Args:
            hostname: name of host to connect to
            username: Username to connect with
            password: password to use (used only to create engine,
                not explicitly stored)
            database: Name of database to use
            port: Port to use
        """
        # Define engine used to create sessions, without storing password
        self.hostname = hostname
        self.username = username
        self.database = database
        self.port = port
        conn_url = f"mysql+pymysql://{self.username}:{quote_plus(password)}@{self.hostname}:{self.port}/{self.database}"  # NOQA
        self._engine = create_engine(conn_url)
        self._sessionmaker = sessionmaker(bind=self._engine)

    def create_session(self) -> sessionmaker.object_session:
        """
        Creates and returns a new database session.

        Args:
            None

        Returns:
            session object
        """
        return self._sessionmaker()


class CreamFinanceState(Base):
    """SQLAlchemy object to represent a Cream Finance state."""

    __tablename__ = "cream_finance_states"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    address = Column(String)
    comptroller = Column(String)
    symbol = Column(String)
    underlying_symbol = Column(String)
    borrow_apy = Column(Float)
    supply_apy = Column(Float)
    utilization_rate = Column(Float)
    cash = Column(Float)
    cashUSD = Column(Float)
    totalBorrows = Column(Float)
    totalBorrowsUSD = Column(Float)
    totalReserves = Column(Float)
    totalReservesUSD = Column(Float)
    borrowRatePerBlock = Column(Float)
    supplyRatePerBlock = Column(Float)
    exchangeRate = Column(Float)
    underlyingDecimals = Column(Integer)
    price = Column(Float)

    def __str__(self) -> str:
        return record_to_str(self)


class RawEvent(Base):
    """SQLAlchemy object to represent a raw event."""

    __tablename__ = "events_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    event = Column(String)
    metadata_ = Column("metadata", String)
    source = Column(String)

    def __str__(self) -> str:
        return record_to_str(self)


class AlphaHomoraPool(Base):
    """SQLAlchemy object to represent a Alpha Homora Pool data."""

    __tablename__ = "alpha_homora_pools_scrape"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    chain = Column(String)
    strategy = Column(String)
    pool = Column(String)
    protocol = Column(String)
    leverage_min = Column(Float)
    leverage_max = Column(Float)
    leverage_highest_apr = Column(Float)
    apr_min = Column(Float)
    apr_max = Column(Float)
    apy_trading_fee = Column(Float)
    apr_farming = Column(Float)
    apr_reward = Column(Float)
    apy_borrow = Column(Float)
    trading_volume_24h = Column(Float)
    tvl_pool = Column(Float)
    tvl_homora = Column(Float)
    positions = Column(Integer)

    def __str__(self) -> str:
        return record_to_str(self)


@app.command()
def run_example(
    username: str,
    hostname: str,
    database: str,
    port: int,
    password: str = typer.Option(None, prompt=True, hide_input=True),
):
    """
    Runs an example generation of raw event and saves to database.
    Example that creates a sample object, writes it to a database, and reads it back
    in a second commit.
    """
    # Create database adapter
    db = DatabaseAdapter(hostname, username, password, database, port)

    # Intialize a session
    session = db.create_session()

    # Create event object
    timestamp = datetime.utcnow()
    event = RawEvent(
        timestamp=timestamp,
        event=json.dumps({"test": 1234, "foo": {"bar": 1, "baz": 2}}),
    )
    logging.info(f"Created event: {event}")

    # Create event
    session.add(event)

    # Write event
    logging.info("Persisting event ...")
    session.commit()
    logging.info("Persist complete.")

    # Read back event
    logging.info("Querying for the event using filter ...\n")
    # results = session.query(RawEvent)
    results = session.query(RawEvent).order_by(RawEvent.timestamp.desc()).limit(1)
    logging.info(f"Executed query:\n{str(results)}\n")
    logging.info("Query Results: ")
    for r in results:
        logging.info(r)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
