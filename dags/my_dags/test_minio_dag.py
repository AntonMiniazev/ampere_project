from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "minio_conn"
BUCKET = "ampere-prod-raw"
KEY = "connectivity_check/test_object.txt"
CONTENT = b"ok"


@dag(
    schedule=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["test", "minio"],
)
def test_minio_connection():
    @task
    def put_object():
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3.load_bytes(bytes_data=CONTENT, key=KEY, bucket_name=BUCKET, replace=True)
        assert s3.check_for_key(key=KEY, bucket_name=BUCKET)

    @task(trigger_rule="all_done")
    def cleanup():
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        if s3.check_for_key(key=KEY, bucket_name=BUCKET):
            s3.delete_objects(bucket=BUCKET, keys=[KEY])

    put_object()


test_minio_connection()
