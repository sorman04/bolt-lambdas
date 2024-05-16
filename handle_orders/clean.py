import io, os
import pytz
import logging
import boto3

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


def delete_all_in_folder(bucket_name, folder_prefix, age):
    s3 = boto3.client("s3")

    threshold_date = datetime.now(pytz.timezone("Europe/Bucharest")) - timedelta(days=age)
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' in response:
        objects = [
            {'Key': obj['Key']} 
            for obj in response['Contents']
            if (obj['LastModified'] < threshold_date) & (os.path.basename(obj['Key']) != '')
            ]

        # Delete the objects
        if len(objects) > 0:
            delete_response = s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects}
            )
        else:
            logger.info(f"Nothing to delete in: {folder_prefix}")
    return


def handler(event, context):
    # add runtime date to Bulk and MailBag files
    today = datetime.now()
    today = today.strftime("%d-%m-%YT%H:%M")

    BUCKET = "bolt-projects"

    input_files = [
        "purchasing-orders/input/Bulk PO.zip",
        "purchasing-orders/input/MailBag.csv",
        "purchasing-orders/input/cadentar.xlsx",
        "purchasing-orders/input/emails.xlsx",
        "purchasing-orders/input/MapareFurnizori_Cadentar_WMS.xlsx",
    ]

    files = [
        [
            file,
            file.split(".")[0].replace("input", "zip-archive")
            + f"({today})."
            + file.split(".")[1],
        ]
        for file in input_files
    ]

    s3c = boto3.client("s3")

    # Change files names, save the files to archive folder and delete the rest of them
    failed_files = []
    for file in files:
        if file[0] == "purchasing-orders/zip-archive/Bulk PO.zip":
            file[0] = file[0].replace(" ", "")
            file[1] = file[0].split(".")[0] + f"({today})." + file[0].split(".")[1]
        try:
            obj = s3c.get_object(Bucket=BUCKET, Key=file[0])
            body = io.BytesIO(obj["Body"].read())
            s3c.put_object(
                Body=body.getvalue(),
                Bucket=BUCKET,
                Key=file[1],
            )
        except:
            failed_files.append(file[0])
            continue

        s3c.delete_object(Bucket=BUCKET, Key=file[0])

    # delete the rest of the files in the input subfolder
    input_prefix = "purchasing-orders/input/"
    delete_all_in_folder(BUCKET, input_prefix, 0)
    
    # delete all the files in the wrk subfolder
    wrk_prefix = "purchasing-orders/wrk/"
    delete_all_in_folder(BUCKET, wrk_prefix, 0)
    
    # delete all files from zip-archive subfolder if older than 30 days
    zip_prefix = "purchasing-orders/zip-archive/"
    delete_all_in_folder(BUCKET, zip_prefix, 30)
    
    if len(failed_files) > 0:
        response = {
            "function_name": "Bolt-PO-s3Cleaner",
            "error_message": "Some files have not been processed.",
            "error_details": failed_files
            }
    else:
        response = {
            "function_name": "Bolt-PO-s3Cleaner",
            "error_message": None,
            "error_details": None
            }
    
    return response
