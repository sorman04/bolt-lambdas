""" module handles the update process for the basic MaiBag 

    the following suppliers are modified:
    1. Cristim
    2. JTI
    
    modules prepares the app to send separately Bucuresti and Cluj mails
    
    It needs two input variables:
    
    cluj_stores = ["Bolt Market Bună Ziua","Ice Cream Store","Tobacco Store"]
    
    cristim_addresses = {
        "buc": 24,
        "clj": 112
        }
        
    jti_addresses = {
        "buc": 48,
        "clj": 49
        }
    
"""

import os, ast, json
import logging
import pandas as pd
import boto3

from logging import INFO

class ModeTwoException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

BUCKET = "bolt-projects"


def modify_jti(cluj_stores, mails_address):
    """function modifies JTI mailing rules"""
    
    try:
        mail_bag = pd.read_csv(os.path.join("/tmp", "MailBag.csv"))
    except Exception as e:
        logger.info(f"Mail Bag file reading error: {str(e)}")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"Mail Bag file reading error: {str(e)}",
                "error_details": None
            }
        raise ModeTwoException(reply)

    try:
        jti_files = mail_bag[mail_bag["supplier"] == "J.T. INTERNATIONAL SRL"].iloc[
            0, 1
        ]
    except IndexError:
        logger.info("No JTI mails today")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": "No JTI mails today",
                "error_details": None
            }
        raise ModeTwoException(reply)

    files_list = ast.literal_eval(jti_files)

    buc_files = []
    clj_files = []

    for file in files_list:
        if file.split("-")[1] in cluj_stores:
            clj_files.append(file)
        else:
            buc_files.append(file)

    buc_files_cell = str(buc_files)
    clj_files_cell = str(clj_files)

    try:
        df_mails = pd.read_excel(
            os.path.join("/tmp", "emails.xlsx"), 
            sheet_name="Data Base V2"
            )
    except Exception as e:
        logger.info(f"Emails file reading error: {str(e)}")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"Emails file reading error: {str(e)}",
                "error_details": None
            }
        raise ModeTwoException(reply)
    
    buc_mails = df_mails.iloc[mails_address["buc"] - 2]["Email"]
    clj_mails = df_mails.iloc[mails_address["clj"] - 2]["Email"]
    buc_mails_cell = "[" + buc_mails + "]"
    clj_mails_cell = "[" + clj_mails + "]"
    
    # get is_green flag in order to preserve it
    df = mail_bag[mail_bag["supplier"] == "J.T. INTERNATIONAL SRL"]
    is_green = df["is_green"].iat[0]

    # delete old row
    mail_bag = mail_bag[mail_bag["supplier"] != "J.T. INTERNATIONAL SRL"]

    # add new rows
    new_rows = [
        {
            "supplier": "J.T. INTERNATIONAL SRL",
            "files": buc_files_cell,
            "address": buc_mails_cell,
            "is_green": is_green
        },
        {
            "supplier": "J.T. INTERNATIONAL SRL",
            "files": clj_files_cell,
            "address": clj_mails_cell,
            "is_green": is_green
        },
    ]

    for row in new_rows:
        temp = pd.DataFrame(row, columns=mail_bag.columns, index=[0])
        mail_bag = pd.concat([mail_bag, temp], ignore_index=True)

    # save the new mailbag in the database
    mail_bag.to_csv(os.path.join("/tmp", "MailBag.csv"), index=False)
    
    del df, temp, mail_bag

    logger.info("JTI files updated.")
    return


def modify_cristim(cluj_stores, mails_address):
    """function modifies Cristim mailing rules"""

    try:
        mail_bag = pd.read_csv(os.path.join("/tmp", "MailBag.csv"))
    except Exception as e:
        logger.info(f"Mail Bag file reading error: {str(e)}")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"Mail Bag file reading error: {str(e)}",
                "error_details": None
            }
        raise ModeTwoException(reply)

    try:
        crt_files = mail_bag[mail_bag["supplier"] == "CRIS-TIM COMPANIE DE FAMILIE SRL"].iloc[
            0, 1
        ]
    except IndexError:
        logger.info("No Cristim mails today")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": "No Cristim mails today",
                "error_details": None
            }
        raise ModeTwoException(reply)

    files_list = ast.literal_eval(crt_files)

    buc_files = []
    clj_files = []

    for file in files_list:
        if file.split("-")[2] in cluj_stores:  # here we used the third particle due to hyphen in CRIS-TIM name
            clj_files.append(file)
        else:
            buc_files.append(file)

    buc_files_cell = str(buc_files)
    clj_files_cell = str(clj_files)

    try:
        df_mails = pd.read_excel(
            os.path.join("/tmp", "emails.xlsx"), 
            sheet_name="Data Base V2"
            )
    except Exception as e:
        logger.info(f"Emails file reading error: {str(e)}")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"Emails file reading error: {str(e)}",
                "error_details": None
            }
        raise ModeTwoException(reply)
    
    buc_mails = df_mails.iloc[mails_address["buc"] - 2]["Email"]
    clj_mails = df_mails.iloc[mails_address["clj"] - 2]["Email"]
    buc_mails_cell = "[" + buc_mails + "]"
    clj_mails_cell = "[" + clj_mails + "]"
    
    # get is_green flag in order to preserve it
    df = mail_bag[mail_bag["supplier"] == "CRIS-TIM COMPANIE DE FAMILIE SRL"]
    is_green = df["is_green"].iat[0]

    # delete old row
    mail_bag = mail_bag[mail_bag["supplier"] != "CRIS-TIM COMPANIE DE FAMILIE SRL"]

    # add new rows
    new_rows = [
        {
            "supplier": "CRIS-TIM COMPANIE DE FAMILIE SRL",
            "files": buc_files_cell,
            "address": buc_mails_cell,
            "is_green": is_green
        },
        {
            "supplier": "CRIS-TIM COMPANIE DE FAMILIE SRL",
            "files": clj_files_cell,
            "address": clj_mails_cell,
            "is_green": is_green
        },
    ]

    for row in new_rows:
        temp = pd.DataFrame(row, columns=mail_bag.columns, index=[0])
        mail_bag = pd.concat([mail_bag, temp], ignore_index=True)

    # save the new mailbag in the database
    mail_bag.to_csv(os.path.join("/tmp", "MailBag.csv"), index=False)
    
    del df, temp, mail_bag
    
    logger.info("Cristim files updated.")
    return

def handler(event, context):
    
    cluj_stores = event.get('cluj_stores')
    cristim_addresses = event.get('cristim_addresses')
    jti_addresses = event.get('jti_addresses')
    
    # download missing S3 input files
    s3 = boto3.client("s3")

    bag = "purchasing-orders/input/MailBag.csv"
    eml = "purchasing-orders/input/emails.xlsx"
    
    file_bag = "/tmp/MailBag.csv"
    file_eml = "/tmp/emails.xlsx"

    # download the input files from S3 to local folder
    try:
        s3.download_file(BUCKET, bag, file_bag)
        s3.download_file(BUCKET, eml, file_eml)
    except Exception as e:
        logger.critical(f"Failed to download one or more input files from S3: {str(e)}")
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"One or more input files could not be downloaded from or do not exist on S3: {str(e)}",
                "error_details": None
            }
        raise ModeTwoException(reply)

    modify_cristim(cluj_stores=cluj_stores, mails_address=cristim_addresses)
    modify_jti(cluj_stores=cluj_stores, mails_address=jti_addresses)
    
    # save updated MailBag.csv in s3
    try:
        s3.upload_file(
            "/tmp/MailBag.csv", 
            "bolt-projects", 
            "purchasing-orders/input/MailBag.csv")
    except Exception as err:
        reply = {
                "function_name": "SuppMod-Two",
                "error_message": f"Cannot save MailBag to s3. Error: {str(err)}",
                "error_details": None
            }
        raise ModeTwoException(reply)
    
    response_json = {
        "function_name": "SuppMod-Two",
        "error_message": None,
        "error_details": None
        }
    
    return json.dumps(response_json)