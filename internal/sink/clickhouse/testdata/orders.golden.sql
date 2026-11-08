CREATE TABLE IF NOT EXISTS `replica`.`orders`
(
    `id` UInt64 COMMENT 'mysql: BIGINT unsigned',
    `customer` Nullable(String) COMMENT 'mysql: VARCHAR 100',
    `total` Decimal(12, 2) COMMENT 'mysql: DECIMAL 12,2',
    `created_at` DateTime64(3, 'UTC') COMMENT 'mysql: DATETIME 3',
    `status` LowCardinality(Nullable(String)) COMMENT 'mysql: ENUM',
    `tags` Array(LowCardinality(String)) COMMENT 'mysql: SET',
    `is_vip` Int8 COMMENT 'mysql: TINYINT',
    `email` String COMMENT 'mysql: VARCHAR 100',
    `_version` UInt64,
    `_is_deleted` UInt8,
    `_synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`_version`, `_is_deleted`)
ORDER BY (`id`)
