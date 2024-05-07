import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore
import duckdb

logger = logging.getLogger()
logger.setLevel("INFO")

REGION_NAME = os.getenv("REGION_NAME")
MAX_POOL_CONNECTIONS = 50
MAX_WORKERS = 10
TMP_DIR = "/tmp"


def create_download_path(prefix: str) -> str:
    prefix_path = os.path.join(TMP_DIR, prefix)
    if not os.path.isdir(prefix_path):
        os.makedirs(prefix_path)
    return prefix_path


def create_table(conn: duckdb.DuckDBPyConnection, prefix: str) -> None:
    prefix_path = create_download_path(prefix)
    conn.execute(
        f"""
            CREATE TABLE {prefix} AS
            SELECT *
            FROM read_parquet('{prefix_path}/*.parquet')
            """
    )


def lambda_handler(event, context):
    logging.info("starting lambda")

    if "datasets" not in event or "query" not in event:
        logger.error("Invalid input: 'datasets' and 'query' are required")
        return {"error": "Invalid input: 'datasets' and 'query' are required"}

    datasets = event["datasets"]
    query = event["query"]

    logging.info("Setup")
    client_config = botocore.config.Config(
        max_pool_connections=MAX_POOL_CONNECTIONS
    )
    client = boto3.client("s3", region_name=REGION_NAME, config=client_config)

    conn = duckdb.connect(":memory:")
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    logging.info("Iterate")
    for dataset in datasets:
        bucket = dataset["bucket"]
        prefix = dataset["prefix"]
        bucket_resource = boto3.resource("s3").Bucket(bucket)

        prefix_path = os.path.join(TMP_DIR, prefix)
        if not os.path.isdir(prefix_path):
            os.makedirs(prefix_path)

        futures = []
        for obj in bucket_resource.objects.all():
            key = obj.key
            if not key.endswith(".parquet"):
                continue
            file_name = os.path.basename(key)
            tmp_file_path = os.path.join(TMP_DIR, prefix, file_name)
            futures.append(
                executor.submit(
                    client.download_file, bucket, key, tmp_file_path
                )
            )

        logger.info("Starting downloads")
        files_downloaded = all(
            [future.result() for future in futures if future.result()]
        )
        logger.info(f"All files downloaded: {files_downloaded}")

        create_table(conn, prefix)

    logging.info("Run query")
    result = conn.sql(query)
    logging.info("Query finished")
    records = result.fetchall()
    column_names = result.columns
    list_of_dicts = [
        {column_names[i]: row[i] for i in range(len(column_names))}
        for row in records
    ]
    return {"result": json.dumps(list_of_dicts)}


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
