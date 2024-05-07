import json
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List

import boto3
import botocore
import duckdb

logger = logging.getLogger()
logger.setLevel("INFO")

REGION_NAME = os.getenv("REGION_NAME")
MAX_POOL_CONNECTIONS = 50
MAX_WORKERS = 10
TMP_DIR = "/tmp"


def download_path(prefix: str) -> str:
    return os.path.join(TMP_DIR, prefix)


def create_table(conn: duckdb.DuckDBPyConnection, prefix: str) -> None:
    prefix_path = download_path(prefix)
    conn.execute(
        f"""
            CREATE TABLE {prefix} AS
            SELECT *
            FROM read_parquet('{prefix_path}/*.parquet')
            """
    )


def run_query(
    conn: duckdb.DuckDBPyConnection, query: str
) -> List[Dict[str, Any]]:
    logging.info("Running query")
    result = conn.sql(query)
    records = result.fetchall()
    column_names = result.columns
    logging.info("Query succeed")
    return [
        {column_names[index]: row[index] for index in range(len(column_names))}
        for row in records
    ]


def create_future_downloads(bucket: str, prefix: str) -> List[Future]:
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    bucket_resource = boto3.resource("s3").Bucket(bucket)
    client_config = botocore.config.Config(
        max_pool_connections=MAX_POOL_CONNECTIONS
    )
    client = boto3.client("s3", region_name=REGION_NAME, config=client_config)

    futures = []
    for obj in bucket_resource.objects.all():
        key = obj.key
        if not key.endswith(".parquet"):
            continue
        file_name = os.path.basename(key)
        tmp_file_path = os.path.join(TMP_DIR, prefix, file_name)
        futures.append(
            executor.submit(client.download_file, bucket, key, tmp_file_path)
        )
    return futures


def execute_futures(futures: List[Future]) -> bool:
    return all([future.result() for future in futures if future.result()])


def lambda_handler(event, context) -> Dict[str, Any]:
    logging.info("starting lambda")

    if "datasets" not in event or "query" not in event:
        error_message = "Invalid input: 'datasets' and 'query' are required"
        logger.error(error_message)
        return {"error": error_message}

    datasets = event["datasets"]
    query = event["query"]
    conn = duckdb.connect(":memory:")

    for dataset in datasets:
        bucket = dataset["bucket"]
        prefix = dataset["prefix"]

        prefix_path = download_path(prefix)
        if not os.path.isdir(prefix_path):
            os.makedirs(prefix_path)

        futures = create_future_downloads(bucket, prefix)

        logger.info("Starting downloads")
        files_downloaded = execute_futures(futures)
        if not files_downloaded:
            error_message = "Files have not been downloaded"
            logger.error(error_message)
            return {"error": error_message}
        logger.info(f"All files downloaded: {files_downloaded}")

        create_table(conn, prefix)

    result = run_query(conn, query)
    return {"result": json.dumps(result)}


if __name__ == "__main__":
    payload = {
        "datasets": [{"bucket": "duckdb-bench-bucket", "prefix": "titles"}],
        "query": "SELECT * from titles limit 10;",
    }
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName="duckdb_bench",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )
    response_str = response["Payload"].read().decode("utf-8")
    response_dict = json.loads(response_str)
    print(response_dict)
