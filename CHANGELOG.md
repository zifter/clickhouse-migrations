# Changelog

## [v0.8.0](https://github.com/zifter/clickhouse-migrations/tree/v0.8.0) (2024-08-18)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.7.1...v0.8.0)

**What's Changed:**
- Add option --fake/--no-fake, which can help update schema_version without executing statements from migration files #27. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28
- Add option --migrations which can help to specify explicitly migrations to apply. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28

**Breaking changes:**
- Drop python 3.8 support. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28
- Option --multi-statement, --dry-run, --secure now working without passing value. Just use --multi-statement/--no-multi-statement, --dry-run/--no-dry-run, --secure/--no-secure for enabling or disabling option. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28


## [v0.7.1](https://github.com/zifter/clickhouse-migrations/tree/v0.7.1) (2024-07-01)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.7.0...v0.7.1)

**What's Changed:**
- Allow default db name #24. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/26


## [v0.7.0](https://github.com/zifter/clickhouse-migrations/tree/v0.7.0) (2024-07-01)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.6.0...v0.7.0)

**What's Changed:**
- #24 Allow connection string for initialization of ClickhouseCluster. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/25
