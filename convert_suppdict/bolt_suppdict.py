import io
import json
import pandas as pd
import boto3


def handler(event, context):
    # get AWS Secrets Manager
    secret_name = "AWS_AccessKeys"
    region_name = "eu-north-1"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # call secrets
    get_secrets = client.get_secret_value(SecretId=secret_name)
    secrets = get_secrets["SecretString"]
    secrets_json = json.loads(secrets)

    AWS_ACCESS_KEY_ID = secrets_json["Access_key_ID"]
    AWS_SECRET_ACCESS_KEY = secrets_json["Secret_Access_key"]

    source_file = "purchasing-orders/input/MapareFurnizori_Cadentar_WMS.xlsx"
    target_file = "purchasing-orders/input/dict_suppliers.xlsx"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    excel_buffer = io.BytesIO()
    bucket_name = "bolt-projects"

    # download from S3 and make changes
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=source_file,
    )

    df = pd.read_excel(io.BytesIO(obj["Body"].read()), engine="openpyxl")
    df.rename(
        columns={"Furnizor Cadentar": "supplier_cad", "Furnizor WMS": "supplier_wms"},
        inplace=True,
    )

    # save to S3
    df.to_excel(excel_buffer)
    s3.put_object(
        Body=excel_buffer.getvalue(),
        Bucket=bucket_name,
        Key=target_file,
    )

    return {
        "function_name": "Bolt-PO-Convert-SuppDict",
    }
