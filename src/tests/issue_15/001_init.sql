create table migration
(
    `app_id`     Int16,
    `created_at` DateTime64(3, 'Europe/Moscow') CODEC (Delta(2), ZSTD(22)),
    `uid`        String CODEC (ZSTD(22)),
    `user_level` Nullable(Int64),
    `session_id` Nullable(String) CODEC (ZSTD(22)),
    uid_old      Nullable(String) CODEC (ZSTD(22)),
    id           Int64,
    level_old    Nullable(Int64),
    social       Nullable(String) CODEC (ZSTD(22))
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        PRIMARY KEY app_id
        ORDER BY (app_id, created_at)
        SETTINGS index_granularity = 8192;

create table refund
(
    `app_id`       Int16,
    `created_at`   DateTime64(3, 'Europe/Moscow') CODEC (Delta(2), ZSTD(22)),
    `uid`          String CODEC (ZSTD(22)),
    transaction_id Nullable(String) CODEC (ZSTD(22)),
    id             Int64
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        PRIMARY KEY app_id
        ORDER BY (app_id, created_at)
        SETTINGS index_granularity = 8192;