CREATE TABLE IF NOT EXISTS `replica`.`order_lines`
(
    `order_id` UInt64 COMMENT 'mysql: BIGINT unsigned',
    `line` Int16 COMMENT 'mysql: SMALLINT',
    `sku` FixedString(16) COMMENT 'mysql: BINARY 16',
    `payload` Nullable(String) COMMENT 'mysql: JSON',
    `_version` UInt64,
    `_is_deleted` UInt8,
    `_synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`_version`, `_is_deleted`)
ORDER BY (`order_id`, `line`)
