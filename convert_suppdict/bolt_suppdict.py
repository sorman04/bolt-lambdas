import io
import json
import logging
import pandas as pd
import boto3

from logging import INFO
from botocore.exceptions import ClientError

class ConvertException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

session = boto3.session.Session()
sm_client = session.client(
    service_name='secretsmanager',
    region_name='eu-north-1'
)

try:
    response = sm_client.get_secret_value(
        SecretId='AWS_AccessKeys'
    )
    secrets = json.loads(response['SecretString'])
except ClientError as e:
    logger.critical(f'Error getting secret: {str(e)}')
    reply = {
            "function_name": "Convert",
            "error_message": f"Secrets Manager Error: {str(e)}",
            "error_details": None
        }
    raise ConvertException(reply)

AWS_ACCESS_KEY_ID = secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = secrets["AWS_SECRET_ACCESS_KEY"]
BUCKET = "bolt-projects"

def handler(event, context):

    source_file = "purchasing-orders/input/MapareFurnizori_Cadentar_WMS.xlsx"
    target_file = "purchasing-orders/input/dict_suppliers.xlsx"

    s3 = boto3.client("s3", region="eu-north-1")

    # download from S3 and make changes
    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=source_file,
        )
    except Exception as e:
        logging.error(f"Reading {source_file} failed with exception {str(e)}")
        reply = {
                "function_name": "Bolt-PO-Convert-SuppDict",
                "error_message": "One or more input files could not be downloaded from or do not exist on S3",
                "error_details": None
            }
        return ConvertException(reply)
    
    try:
        df = pd.read_excel(io.BytesIO(obj["Body"].read()), engine="openpyxl")
        df.rename(
            columns={"Furnizor Cadentar": "supplier_cad", "Furnizor WMS": "supplier_wms"},
            inplace=True,
        )
    except Exception as e:
        logging.error(f"Structural error in {source_file}.")
        reply = {
                "function_name": "Bolt-PO-Convert-SuppDict",
                "error_message": f"Structural error in {source_file}.",
                "error_details": None
            }
        return ConvertException(reply)

    # save to S3
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer)
    s3.put_object(
        Body=excel_buffer.getvalue(),
        Bucket=BUCKET,
        Key=target_file,
    )

    response = {
        "function_name": "Bolt-PO-Convert-SuppDict",
        "error_message": None,
        "error_details": None
    }

    return json.dumps(response)