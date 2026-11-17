-- Example Athena queries. Always filter on date to keep scans (and cost) small.

-- 1. Daily activity: transactions, active senders, fees (ALGO)
SELECT date, count(*) AS txns, count(DISTINCT sender) AS active_senders,
       sum(fee) / 1e6 AS fees_algo
FROM algorand_mainnet.transactions
WHERE date BETWEEN '2026-11-01' AND '2026-11-16'
GROUP BY date ORDER BY date;

-- 2. Top assets by transfer count and unique holders touched (asset_id 0 = ALGO)
SELECT asset_id, count(*) AS transfers, count(DISTINCT receiver) AS receivers
FROM algorand_mainnet.asset_transfers
WHERE date = '2026-11-16'
GROUP BY asset_id ORDER BY transfers DESC LIMIT 20;

-- 3. Busiest apps, split by direct vs inner calls; ARC-4 method usage
SELECT app_id, method_selector,
       count_if(depth = 0) AS direct_calls, count_if(depth > 0) AS inner_calls
FROM algorand_mainnet.app_calls
WHERE date BETWEEN '2026-11-10' AND '2026-11-16'
GROUP BY app_id, method_selector ORDER BY direct_calls DESC LIMIT 50;

-- 4. Retention: senders active this week who were also active last week
WITH w AS (
  SELECT DISTINCT sender, date_trunc('week', from_unixtime(ts)) AS wk
  FROM algorand_mainnet.transactions
  WHERE date BETWEEN '2026-11-02' AND '2026-11-15')
SELECT a.wk, count(*) AS active, count(b.sender) AS retained
FROM w a LEFT JOIN w b ON a.sender = b.sender AND b.wk = a.wk - INTERVAL '7' DAY
GROUP BY a.wk ORDER BY a.wk;
