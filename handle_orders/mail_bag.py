""" function handle the generation of the first iteration of the daily Mail Bag """

import logging
import html
import json
import boto3
import pandas as pd

from datetime import datetime
from zipfile import ZipFile
from logging import INFO

class BaggerException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

BUCKET = "bolt-projects"

def handler(event, context):
    # download the daily input files from S3
    s3 = boto3.client("s3")

    cad = "purchasing-orders/input/cadentar.xlsx"
    mls = "purchasing-orders/input/emails.xlsx"
    dic = "purchasing-orders/input/dict_suppliers.xlsx"
    mov = "purchasing-orders/input/mov_data.csv"
    zip = "purchasing-orders/input/Bulk PO.zip"

    file_cad = "/tmp/cadentar.xlsx"
    file_mails = "/tmp/emails.xlsx"
    file_dict = "/tmp/dict_suppliers.xlsx"
    file_mov = "/tmp/mov_data.csv"
    file_zip = "/tmp/Bulk PO.zip"

    # download the input files from S3 to local folder
    try:
        s3.download_file(BUCKET, cad, file_cad)
        s3.download_file(BUCKET, mls, file_mails)
        s3.download_file(BUCKET, dic, file_dict)
        s3.download_file(BUCKET, mov, file_mov)
        s3.download_file(BUCKET, zip, file_zip)
    except Exception as e:
        logger.critical(f"Failed to download one or more input files from S3: {str(e)}")
        reply = {
                "function_name": "MailBagger",
                "error_message": f"One or more input files could not be downloaded from or do not exist on S3: {str(e)}",
                "error_details": None
            }
        return BaggerException(reply)

    # load the scheduler (candecy)
    try:
        df_cad = pd.read_excel(
            file_cad,
            header=None,
            skiprows=3,
            usecols=[1, 14, 15, 16, 17, 18, 19, 20, 24],
        )
        columns = {
            1: "supplier_cad",
            14: 1,
            15: 2,
            16: 3,
            17: 4,
            18: 5,
            19: 6,
            20: 7,
            24: "has_go",
        }
        df_cad.rename(columns=columns, inplace=True)
    except Exception as e:
        logger.critical(f"Cadency file structural errors: : {str(e)}")
        reply = {
                "function_name": "MailBagger",
                "error_message": f"Cadency file structural errors: : {str(e)}",
                "error_details": None
            }
        return BaggerException(reply)

    # make sure we have no leading or trailing spaces in supplier_cad
    df_cad["supplier_cad"] = df_cad.apply(
        lambda row: row["supplier_cad"].strip(), axis=1
    )

    # filter only suppliers scheduled on the current day and which have a true send flag
    today = datetime.now()
    day = today.weekday() + 1

    mask = (df_cad[day] == "X") & (df_cad.has_go == True)
    df_scheduled = df_cad[mask].copy()
    
    del df_cad

    nr_scheds = len(df_scheduled)
    if nr_scheds == 0:
        # there are no suppliers scheduled today
        logger.info("There are no suppliers scheduled today. Abort")
        reply = {
                "function_name": "MailBagger",
                "error_message": "There are no suppliers scheduled today. Abort",
                "error_details": None
            }
        return BaggerException(reply)

    # map wms-cad supplier names and get the scheduled suppliers names
    try:
        df_map = pd.read_excel(file_dict)
        # make sure we have no leading or trailing spaces in supplier_cad and supplier_wms
        df_map["supplier_cad"] = df_map.apply(
            lambda row: row["supplier_cad"].strip(), axis=1
        )
        df_map["supplier_wms"] = df_map.apply(
            lambda row: row["supplier_wms"].strip(), axis=1
        )
        df_scheduled = df_scheduled.merge(df_map, how="left", on="supplier_cad")
    except:
        logger.info("Cannot merge dictionary and cadency files")
        reply = {
                "function_name": "MailBagger",
                "error_message": "Cannot merge dictionary and cadency files. Abort",
                "error_details": None
            }
        return BaggerException(reply)
    
    del df_map

    # list of suppliers scheduled for today action
    scheduled_suppliers = df_scheduled[~df_scheduled["supplier_wms"].isnull()][
        "supplier_wms"
    ].to_list()
    scheduled_suppliers = sorted(list(set(scheduled_suppliers)))

    # list of suppliers for which we have no mapping
    not_in_dict = df_scheduled[df_scheduled["supplier_wms"].isnull()][
        "supplier_cad"
    ].to_list()
    
    del df_scheduled

    # unzip Bulk PO.zip, extract files and generate a list with suppliers
    # 1. read and unzip bulk po
    try:
        with ZipFile(file_zip) as zipfile:
            bulk_name = zipfile.namelist()
            zipfile.extractall("/tmp")
    except Exception as e:
        logger.info(f"Zip extraction error: {str(e)}")
        reply = {
                "function_name": "MailBagger",
                "error_message": f"Zip extraction error: {str(e)}",
                "error_details": None
            }
        return BaggerException(reply)
    logger.info("Unzipped daily files.")

    # 2. check if we have order files
    nr_wms = len(bulk_name)
    if nr_wms == 0:
        # there are no orders generated in WMS
        logger.info("There are no orders generated in WMS today. Abort.")
        reply = {
                "function_name": "MailBagger",
                "error_message": "There are no orders generated in WMS today. Abort",
                "error_details": None
            }
        return BaggerException(reply)
    
    # 3. extract suppliers' names from WMS file names
    def extract_supname(text):
        res = text.split("-")
        if len(res) == 5:
            return res[0].rstrip()
        elif len(res) == 6:
            return res[0] + "-" + res[1]
        else:
            return ""

    wms_list = [[extract_supname(file_name), file_name] for file_name in bulk_name]
    df_wms = pd.DataFrame(wms_list, columns=["supplier_wms", "file_name"])
    wms_list = df_wms["supplier_wms"].tolist()
    wms_list = list(set(wms_list))
    wms_list = sorted(wms_list)

    not_in_cad = [
        supplier for supplier in wms_list if supplier not in scheduled_suppliers
    ]
    not_in_wms = [
        supplier for supplier in scheduled_suppliers if supplier not in wms_list
    ]

    try:
        df_mov = pd.read_csv(file_mov, names=["supplier", "store", "has_mov", "mov"])
        df_mov["supplier"] = df_mov.apply(lambda row: row["supplier"].strip(), axis=1)
        # take out the html codes from suppliers names
        df_mov["supplier"] = df_mov.apply(
            lambda row: html.unescape(row["supplier"]), axis=1
        )
    except:
        logger.info("Mov file structural/data errors. Abort.")
        reply = {
                "function_name": "MailBagger",
                "error_message": "Mov file structural/data errors. Abort",
                "error_details": None
            }
        return BaggerException(reply)

    # 4. make a list of suppliers that do not have sufficient mov
    not_in_mov = list(set(df_mov[df_mov.has_mov == False]["supplier"].to_list()))

    # 5. make a list of suppliers that have both true and false mov flag
    both_mov = [
        supplier
        for supplier in not_in_mov
        if len(df_mov[(df_mov.supplier == supplier) & (df_mov.has_mov == True)]) != 0
    ]

    # 6. make a list of suppliers scheduled for order
    to_be_sent = [
        supplier
        for supplier in scheduled_suppliers
        if (supplier in wms_list) & (supplier not in not_in_mov)
    ]

    # 7. make alist with suppliers with no info on mov
    mov_suppliers = df_mov["supplier"].to_list()
    mov_notfound = [
        supplier for supplier in to_be_sent if supplier not in mov_suppliers
    ]
    
    del df_mov

    # 8. attach mail addresses to the list
    try:
        df_mails = pd.read_excel(
            file_mails, sheet_name="Data Base V2", usecols=[0, 1, 2]
        )
        df_mails.rename(
            columns={
                "Supplier WMS": "supplier_wms",
                "Email": "mail_adresses",
                "Auto-send order?": "is_green",
            },
            inplace=True,
        )
        df_mails["supplier_wms"] = df_mails.apply(
            lambda row: row["supplier_wms"].strip(), axis=1
        )
    except:
        logger.info("Mail file structural/data errors. Abort.")
        reply = {
                "function_name": "MailBagger",
                "error_message": "Mail file structural/data errors. Abort",
                "error_details": None
            }
        return BaggerException(reply)

    duplicated_mails = df_mails[df_mails.duplicated()]
    if len(duplicated_mails) != 0:
        df_mails.drop_duplicates(keep="first", inplace=True)

    # 9. generate the final list and save it
    wms_files = [
        [
            supplier,
            df_wms[df_wms.supplier_wms == supplier]["file_name"].tolist(),
            df_mails[df_mails.supplier_wms == supplier]["mail_adresses"].to_list(),
            df_mails[df_mails.supplier_wms == supplier]["is_green"].to_list(),
        ]
        for supplier in to_be_sent
    ]
    
    del df_mails
    del df_wms

    df_final = pd.DataFrame(
        wms_files, columns=["supplier", "files", "address", "is_green"]
    )
    df_final.to_csv("/tmp/MailBag.csv", index=False)
    
    del df_final

    response_json = {
        "function_name": "MailBagger",
        "error_message": None,
        "error_details": {
            "not-in-cad": not_in_cad,
            "not-in-wms": not_in_wms,
            "not-in-mov": not_in_mov,
            "both-mov": both_mov,
            "no-mov": mov_notfound,
            "not-in-dict": not_in_dict,
        },
    }

    with open("/tmp/data.json", "w", encoding="utf-8") as f:
        json.dump(response_json, f, ensure_ascii=False, indent=4)
        
    # 10. save the files to S3
    try:
        s3.upload_file(
            "/tmp/MailBag.csv", 
            "bolt-projects", 
            "purchasing-orders/input/MailBag.csv")
    except Exception as err:
        reply = {
                "function_name": "MailBagger",
                "error_message": f"Cannot save MailBag to s3. Error: {str(err)}",
                "error_details": None
            }
        raise BaggerException(reply)
    
    try:
        s3.upload_file(
            "/tmp/data.json", 
            "bolt-projects", 
            "purchasing-orders/input/data.json")
    except Exception as err:
        reply = {
                "function_name": "MailBagger",
                "error_message": f"Cannot save data.json to s3. Error: {str(err)}",
                "error_details": None
            }
        raise BaggerException(reply)
    
    logger.info("procedure finalized and stopped successfully")

    return {
        "function_name": "MailBagger",
        "error_message": None,
        "error_details": None
        }
