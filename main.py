import os
import zipfile

import boto3
import duckdb
from kaggle.api.kaggle_api_extended import KaggleApi

AWS_BUCKET = "duckdb-bench-bucket"


def upload_parquet_files_to_s3(directory_path, bucket_name, s3_prefix):
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if not file.endswith(".parquet"):
                return
            local_file_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_file_path, directory_path)
            upload_file_to_s3(
                local_file_path,
                bucket_name,
                os.path.join(s3_prefix, s3_key),
            )


def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.upload_file(local_file_path, bucket_name, s3_key)
    print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")


def extract_zip(zip_file, extract_dir):
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(extract_dir)


def create_titles_table(conn, data_dir):
    SQL_TITLES_PARTITIONS = """
    SET temp_directory = 'temp';
    SET memory_limit = '6GB';
    SET threads TO 8;
    SET enable_progress_bar = true;

    CREATE TABLE titles AS
    SELECT
    primaryTitle,
    originalTitle,
    isAdult,
    startYear,
    endYear,
    runtimeMinutes,
    genres
    FROM read_csv(
        'data/titles.csv',
        types={
            'tconst': 'VARCHAR(9)',
            'titleType': 'VARCHAR(5)',
            'primaryTitle': 'VARCHAR(60)',
            'originalTitle': 'VARCHAR(60)',
            'isAdult': 'VARCHAR(1)',
            'startYear': 'VARCHAR(4)',
            'endYear': 'VARCHAR(4)',
            'runtimeMinutes': 'VARCHAR(2)',
            'genres': 'VARCHAR(60)'
        }
    );

    COPY titles TO 'data/titles'(
        FORMAT PARQUET,
        PARTITION_BY (isAdult),
        OVERWRITE_OR_IGNORE,
        FILENAME_PATTERN "titles_{uuid}"
    );
    """
    conn.execute(SQL_TITLES_PARTITIONS)
    upload_parquet_files_to_s3(data_dir, AWS_BUCKET, "titles")


def kaggle_to_s3():
    api = KaggleApi()
    api.authenticate()

    base_dir = os.path.dirname(__file__)
    data_dir = os.path.join(base_dir, "data")
    kaggle_dataset_path = "rishabjadhav/imdb-actors-and-movies"
    dir_name = kaggle_dataset_path.split("/")[1]
    dataset_zip = f"{dir_name}.zip"
    dataset_path = os.path.join(data_dir, dataset_zip)
    prefix = "titles"

    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    if not os.path.isfile(dataset_path):
        api.dataset_download_files(kaggle_dataset_path, path=data_dir)

    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    extract_zip(dataset_path, data_dir)

    dataset_staging_path = os.path.join(data_dir, prefix)
    if not os.path.isdir(dataset_staging_path):
        os.makedirs(dataset_staging_path)

    conn = duckdb.connect(":memory:")
    create_titles_table(conn, data_dir)


def delete_prefix():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(AWS_BUCKET)
    bucket.objects.filter(Prefix="titles").delete()


if __name__ == "__main__":
    # kaggle_to_s3()
    delete_prefix()
