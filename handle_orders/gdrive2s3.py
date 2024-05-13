""" module handles the downloading of some files from Google Drive to s3

    function assumes that the AWS secrets are stored in
    the secrets "AWS_LambdaKeys" and "GoogleCloud_Credentials" from "eu-north-1" region
    
    INPUT VARIABLES ==========================================
    function expects as input an event with the following format:

    {   
        "bucket": the name of the s3 bucket where the files will be stored
        "files": {
            s3_file_key: google_drive_file_id,
        }
        "google_cloud_account": name of Secrets Manager secret credentials for the Google Cloud account associated with current project
    }
    
    OUTPUT VARIABLES =========================================
    function creates an output JSON with the following format:
    {
        "function_name": name of function,
        "error_message": error message/None
    }

"""

import io
import boto3
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

#### ERRORS WITH MESSAGE
class GoogleDriveS3Exception(Exception): pass

def handler(event, context):

##### STEP 1 - OBTAIN SECRETS & PAYLOAD VARIABLES
    # get Payload variables
    BUCKET = event.get('bucket')
    files = event.get('files')
    google_cloud_account = event.get('google_cloud_account')
    
    # create boto session object for getting Google Drive credential secrets
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name= 'eu-north-1')
    get_secrets = client.get_secret_value(SecretId="GoogleCloud_Credentials")
    secrets_json = json.loads(get_secrets['SecretString'])
    googleDrive_credentials = secrets_json[google_cloud_account]

##### STEP 2 - INITIALISE GOOGLE CLOUD CREDENTIALS
    try:
        # Service Account key (in json format)
        SERVICE_ACCOUNT_INFO = json.loads(googleDrive_credentials, strict=False)
        # scope of operation, in this case readonly suffices for download
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        # create credentials
        creds = service_account.Credentials.from_service_account_info(
                SERVICE_ACCOUNT_INFO, scopes=SCOPES)
        # create drive service
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        message = {
            "function_name": "Generic-GoogleDrive-S3",
            "error_message": f"Google Drive authentication failure: {str(e)}",
            "error_details": None,
            }
        raise GoogleDriveS3Exception(message)

##### STEP 3 - DOWNLOAD FROM DRIVE AND UPLOAD TO S3
    # initialise S3 client
    s3 = boto3.client("s3", region="eu-north-1")
    # start doc loop
    for s3_file_path, file_id in files.items():
        try:
            # a) DOWNLOAD
            request = drive_service.files().export_media(
                fileId = file_id, 
                mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            file_obj = io.BytesIO()
            downloader = MediaIoBaseDownload(file_obj, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file_obj.seek(0)
        except Exception as e:
            message = {
                "function_name": "Generic-GoogleDrive-S3",
                "error_message": f"Google Drive download failure: {str(e)}",
                "error_details": None
                }
            raise GoogleDriveS3Exception(message)

        # b) UPLOAD
        s3.upload_fileobj(file_obj, BUCKET, s3_file_path)

    return {
        "function_name": "Generic-GoogleDrive-S3",
        "error_message": None,
        "error_details": None
    }