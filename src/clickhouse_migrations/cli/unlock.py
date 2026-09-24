"""``unlock``: force-release the migration lock left behind by a dead run."""

from clickhouse_migrations.cli import common


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "unlock",
        help="Force-release the migration lock left behind by a dead run",
    )
    common.add_common_arguments(parser)
    return parser


def unlock(ctx) -> int:
    common.configure_logging(ctx)

    cluster = common.create_cluster(ctx)
    holder = cluster.force_unlock(db_name=ctx.db_name)
    if holder is None:
        print("No migration lock is held.")
    else:
        print(
            f"Released the migration lock held by {holder.owner} " f"for {holder.age}s."
        )

    return 0
