CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW scrape_data.alpha_homora_pools_scrape_view AS
select
    ah.id AS id,
    ah.timestamp AS timestamp,
    ah.chain AS chain,
    ah.strategy AS strategy,
    ah.pool AS pool,
    ah.protocol AS protocol,
    ah.leverage_min AS leverage_min,
    ah.leverage_max AS leverage_max,
    ah.leverage_highest_apr AS leverage_highest_apr,
    ah.apr_min AS apr_min,
    ah.apr_max AS apr_max,
    ah.apy_trading_fee AS apy_trading_fee,
    ah.apr_farming AS apr_farming,
    ah.apr_reward AS apr_reward,
    ah.apy_borrow AS apy_borrow,
    ah.trading_volume_24h AS trading_volume_24h,
    ah.tvl_pool AS tvl_pool,
    ah.tvl_homora AS tvl_homora,
    ah.positions AS positions,
    if(ah.pool like '%USD%'
    or ah.pool like '%UST%'
    or ah.pool like '%DAI%'
    or ah.pool like '%pool%'
    or ah.pool like '%FRAX%',
    1,
    0) AS stable
from
    scrape_data.alpha_homora_pools_scrape ah;
