CREATE TABLE IF NOT EXISTS `we\`ird`.`ta\`ble`
(
    `co l` Int32 COMMENT 'mysql: INT',
    `_version` UInt64,
    `_is_deleted` UInt8,
    `_synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`_version`, `_is_deleted`)
ORDER BY (`co l`)
