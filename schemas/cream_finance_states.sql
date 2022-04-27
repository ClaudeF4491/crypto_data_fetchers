
CREATE TABLE IF NOT EXISTS cream_finance_states (
	id int AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP,
    address text,
    comptroller text,
    symbol text,
    underlying_symbol text,
    borrow_apy float NULL,
    supply_apy float NULL,
    utilization_rate float NULL,
    cash float NULL,
    cashUSD float NULL,
    totalBorrows float NULL,
    totalBorrowsUSD float NULL,
    totalReserves float NULL,
    totalReservesUSD float NULL,
    borrowRatePerBlock float NULL,
    supplyRatePerBlock float NULL,
    exchangeRate float NULL,
    underlyingDecimals int NULL,
    INDEX address (address),
    INDEX comptroller (symbol),
    INDEX symbol (symbol),
    INDEX underlying_symbol (underlying_symbol)
)  ENGINE=INNODB;
