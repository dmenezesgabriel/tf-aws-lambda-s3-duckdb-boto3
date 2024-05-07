import json
import os

import boto3
import duckdb


def lambda_handler(event, context):
    if "datasets" not in event or "query" not in event:
        return {"error": "Invalid input: 'datasets' and 'query' are required"}

    datasets = event["datasets"]
    query = event["query"]

    tmp_dir = "/tmp"

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    conn = duckdb.connect()

    for dataset in datasets:
        bucket = dataset["bucket"]
        prefix = dataset["prefix"]
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for index, obj in enumerate(page.get("Contents", [])):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                prefix_path = os.path.join(tmp_dir, prefix)
                if not os.path.isdir(prefix_path):
                    os.makedirs(prefix_path)
                file_name = os.path.basename(key)
                tmp_file_path = os.path.join(tmp_dir, prefix, file_name)
                client.download_file(bucket, key, tmp_file_path)
        conn.execute(
            f"""
            CREATE TABLE {prefix} AS
            SELECT *
            FROM read_parquet('{prefix_path}/*.parquet')
            """
        )
    result = conn.sql(query).fetchall()
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
