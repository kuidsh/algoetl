-- Athena DDL for the two tables not declared in infra/template.yaml.
-- Replace <bucket>. Partition projection means no crawler is needed.

CREATE EXTERNAL TABLE IF NOT EXISTS algorand_mainnet.blocks (
  round bigint, ts bigint, tx_count int, fees bigint, proposer string, genesis_id string)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/algorand/mainnet/blocks/'
TBLPROPERTIES ('projection.enabled'='true','projection.date.type'='date',
  'projection.date.format'='yyyy-MM-dd','projection.date.range'='2019-06-11,NOW',
  'storage.location.template'='s3://<bucket>/algorand/mainnet/blocks/date=${date}');

CREATE EXTERNAL TABLE IF NOT EXISTS algorand_mainnet.assets (
  round bigint, ts bigint, txid string, asset_id bigint, action string, sender string,
  creator string, name string, unit_name string, url string, decimals smallint,
  total decimal(20,0), default_frozen boolean, manager string, reserve string,
  freeze string, clawback string)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/algorand/mainnet/assets/'
TBLPROPERTIES ('projection.enabled'='true','projection.date.type'='date',
  'projection.date.format'='yyyy-MM-dd','projection.date.range'='2019-06-11,NOW',
  'storage.location.template'='s3://<bucket>/algorand/mainnet/assets/date=${date}');

CREATE EXTERNAL TABLE IF NOT EXISTS algorand_mainnet.app_calls (
  round bigint, ts bigint, txid string, app_id bigint, sender string,
  on_completion string, n_args smallint, depth smallint, method_selector string)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/algorand/mainnet/app_calls/'
TBLPROPERTIES ('projection.enabled'='true','projection.date.type'='date',
  'projection.date.format'='yyyy-MM-dd','projection.date.range'='2019-06-11,NOW',
  'storage.location.template'='s3://<bucket>/algorand/mainnet/app_calls/date=${date}');
