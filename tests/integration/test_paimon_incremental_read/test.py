# coding: utf-8

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, run_and_check


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

INCREMENTAL_WRITER_LOCAL_DIR = os.path.join(SCRIPT_DIR, "paimon-incremental-data")
INCREMENTAL_WRITER_REMOTE_DIR = "/root/paimon-incremental-data"
INCREMENTAL_WRITER_JAR = (
    "/root/paimon-incremental-data/target/paimon-incremental-writer-1.1.1.jar"
)
PAIMON_WAREHOUSE_URI = "file:///tmp/warehouse/"
PAIMON_TABLE_PATH = "/tmp/warehouse/test.db/test_table"
CH_TABLE_NAME = "paimon_inc_read"
CH_TABLE_NAME_WITH_LIMIT = "paimon_inc_read_with_limit"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


def _copy_directory_to_container(instance_id: str, local_dir: str, remote_dir: str):
    if not os.path.isdir(local_dir):
        raise RuntimeError(f"Directory does not exist: {local_dir}")

    run_and_check(
        [
            "docker cp {local} {cont_id}:{remote}".format(
                local=local_dir, cont_id=instance_id, remote=remote_dir
            )
        ],
        shell=True,
    )


def _prepare_incremental_writer(instance_id: str):
    _copy_directory_to_container(
        instance_id, INCREMENTAL_WRITER_LOCAL_DIR, INCREMENTAL_WRITER_REMOTE_DIR
    )

    # If chunks are provided, assemble fat jar in the container.
    run_and_check(
        [
            "docker exec {cont_id} bash -lc \"cd {remote} && "
            "if ls chunk_* >/dev/null 2>&1; then mkdir -p target && cat chunk_* > target/paimon-incremental-writer-1.1.1.jar; fi\"".format(
                cont_id=instance_id, remote=INCREMENTAL_WRITER_REMOTE_DIR
            )
        ],
        shell=True,
    )

    # Build output from local project is expected to be pre-generated.
    run_and_check(
        [
            "docker exec {cont_id} bash -lc 'test -f {jar}'".format(
                cont_id=instance_id, jar=INCREMENTAL_WRITER_JAR
            )
        ],
        shell=True,
    )


def _wait_until_query_result(
    query: str,
    expected: str,
    *,
    database: str,
    retries: int = 30,
    sleep_seconds: float = 0.5,
):
    last_result = ""
    for _ in range(retries):
        last_result = node.query(query, database=database)
        if last_result == expected:
            return
        time.sleep(sleep_seconds)

    raise AssertionError(
        f"Unexpected result for query: {query}\nExpected: {expected!r}\nActual: {last_result!r}"
    )


def _run_writer(
    instance_id: str,
    *,
    start_id: int,
    rows_per_commit: int,
    commit_times: int,
) -> None:
    writer_cmd = (
        f"java -jar {INCREMENTAL_WRITER_JAR} "
        f'"{PAIMON_WAREHOUSE_URI}" "test" "test_table" "{start_id}" "{rows_per_commit}" "{commit_times}"'
    )
    run_and_check(
        [
            "docker exec {cont_id} bash -lc '{cmd}'".format(
                cont_id=instance_id, cmd=writer_cmd
            )
        ],
        shell=True,
    )


def _create_clickhouse_table_for_paimon_incremental_read(table_name: str):
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        "CREATE TABLE {table_name} "
        "ENGINE = PaimonLocal('{table_path}') "
        "SETTINGS "
        "paimon_incremental_read = 1, "
        "paimon_keeper_path = '/clickhouse/tables/{uuid}', "
        "paimon_replica_name = '{replica}', "
        "paimon_metadata_refresh_interval_ms = 100".format(
            table_name=table_name, table_path=PAIMON_TABLE_PATH
        )
    )


def test_paimon_incremental_read_via_paimon_table_engine(started_cluster):
    instance_id = cluster.get_instance_docker_id("node")
    _prepare_incremental_writer(instance_id)

    # Clean warehouse for idempotent re-runs.
    run_and_check(
        [
            "docker exec {cont_id} bash -lc \"rm -rf /tmp/warehouse\"".format(
                cont_id=instance_id
            )
        ],
        shell=True,
    )

    # Create a normal Paimon data table from writer side (no REST catalog).
    _run_writer(instance_id, start_id=0, rows_per_commit=0, commit_times=0)

    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME)

    # First snapshot: 10 rows.
    _run_writer(instance_id, start_id=0, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Second snapshot: another 10 rows.
    _run_writer(instance_id, start_id=10, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Targeted snapshot reads are deterministic and do not advance stream state.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=1",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=1",
        "10\n",
        database="default",
    )

    # max_consume_snapshots limit: consume at most 2 snapshots per query.
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    run_and_check(
        [
            "docker exec {cont_id} bash -lc \"rm -rf /tmp/warehouse\"".format(
                cont_id=instance_id
            )
        ],
        shell=True,
    )

    # Recreate a clean normal Paimon table.
    _run_writer(instance_id, start_id=0, rows_per_commit=0, commit_times=0)
    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME_WITH_LIMIT)

    # Produce 3 snapshots, each snapshot contains 10 rows.
    _run_writer(instance_id, start_id=0, rows_per_commit=10, commit_times=3)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "20\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "0\n",
        database="default",
    )

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_WITH_LIMIT} SYNC;")
