import io
import json
import boto3
from datetime import datetime


def handler(event, context):
    # get AWS Secrets Manager
    secret_name = "AWS_LambdaKeys"
    region_name = "eu-north-1"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    # call secrets
    get_secrets = client.get_secret_value(SecretId=secret_name)
    secrets = get_secrets["SecretString"]
    secrets_json = json.loads(secrets)

    AWS_ACCESS_KEY_ID = secrets_json["Access_key_ID"]
    AWS_SECRET_ACCESS_KEY = secrets_json["Secret_Access_key"]

    # add runtime date to Bulk and MailBag files
    today = datetime.now()
    today = today.strftime("%d-%m-%Y")

    bucket_name = "bolt-projects"

    key_bulk = "purchasing-orders/zip-archive/BulkPO.zip"
    key_bag = "purchasing-orders/zip-archive/MailBag.csv"

    new_bulk = key_bulk.split(".")[0] + f"({today})" + ".zip"
    new_bag = key_bag.split(".")[0] + f"({today})" + ".csv"

    s3c = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # 1. Change Bulk PO.zip
    obj = s3c.get_object(Bucket=bucket_name, Key=key_bulk)
    body = io.BytesIO(obj["Body"].read())
    s3c.put_object(
        Body=body.getvalue(),
        Bucket=bucket_name,
        Key=new_bulk,
    )
    s3c.delete_object(Bucket=bucket_name, Key=key_bulk)

    # 2. Change MailBag.csv
    obj = s3c.get_object(Bucket=bucket_name, Key=key_bag)
    body = io.BytesIO(obj["Body"].read())
    s3c.put_object(
        Body=body.getvalue(),
        Bucket=bucket_name,
        Key=new_bag,
    )
    s3c.delete_object(Bucket=bucket_name, Key=key_bag)

    # delete input files
    s3r = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    bucket = s3r.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        if (
            len(key.split("/")) == 3
            and key.split("/")[1] == "input"
            and key.split("/")[2] != ""
        ):
            s3c.delete_object(Bucket=bucket_name, Key=key)

    return {
        "function_name": "Bolt-PO-CleanUp",
    }
