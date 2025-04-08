import io
import os
import subprocess
from typing import Dict, Any

import arrow
import boto3
from aws_lambda_typing.context import Context

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

AWS_S3_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
AWS_S3_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
AWS_STORAGE_BUCKET_NAME = os.environ.get('AWS_STORAGE_BUCKET_NAME')
AWS_S3_REGION_NAME = os.environ.get('AWS_S3_REGION_NAME')

# Initialize the S3 client outside the function
client = boto3.client(
    's3',
    aws_access_key_id=AWS_S3_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_S3_SECRET_ACCESS_KEY
)


def dump_postgres_database(name):
    os.environ.setdefault('PGPASSWORD', DB_PASSWORD)
    command = ["pg_dump", "--host", DB_HOST, "--port", DB_PORT, "--username", DB_USER]
    extra_args = [
        '--format', 'p', '--encoding', "UTF8", '--no-owner', '--verbose',
        '--no-password', name
    ]
    process = subprocess.Popen(
        [*command, *extra_args], stdout=subprocess.PIPE, stdin=subprocess.PIPE,
    )
    result = process.communicate()[0]
    code = process.returncode
    if code != 0:
        raise ValueError('The task could not be accomplished.')
    file = io.BytesIO()
    file.write(result)
    file.seek(0)
    return file


def upload_file(folder_name, file):
    date_format = arrow.get().format('YYYY-MM-DD')
    path = f"{folder_name}/{date_format}/"
    objects = client.list_objects(Bucket=AWS_STORAGE_BUCKET_NAME, Prefix=path)
    count = len(objects.get('Contents', []))
    file_name = f"backup_{count + 1}.sql"
    return client.put_object(
        Body=file.getvalue(),
        Bucket=AWS_STORAGE_BUCKET_NAME,
        Key=path + file_name,
        ContentType='text/plain'
    )


def handler(event=None, context: Context = None) -> Dict[str, Any]:
    if event is None:
        event = {}
    try:
        db_name = event.get('database', "")
        db_file = dump_postgres_database(db_name)
        upload_file(db_name, db_file)
        return {
            "statusCode": 200,
            "body": {
                "message": 'Dump uploaded successfully.',
            },
            "headers": {
                "content-type": "application/json"
            }
        }
    except ValueError as e:
        return {
            "statusCode": 400,
            "body": {
                "message": f"Task failed: {e.args[0]}"
            }
        }
