""" module handles the update process for the basic MaiBag 

    the following suppliers are modified:
    
    1. Danone: add column to the PO files
    2. Star Foods: rename the PO files
    3. Auchan: modify the PO files content and rename them
    4. Coca Cola: add info to PO files, related to the delivery form - boxes vs units
    5. Quadrant: same as Coca Cola plus rename the PO files
    
"""
import os, ast, json
import logging
import pandas as pd
import boto3

from datetime import datetime
from zipfile import ZipFile
from logging import INFO

class ModeOneException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

today = datetime.now()
BUCKET = "bolt-projects"

tmp_folder = "/tmp/wrk/"
os.makedirs(tmp_folder)

def danone_mods(name_elements, old_name):
    # add column in Danone PO files
    try:
        store_name = name_elements[1]
    except IndexError:
        logger.critical("Errors in parsing the file name - Danone -")
        reply = {
                "function_name": "SuppMod-One",
                "error_message": "Errors in parsing the file name - Danone -",
                "error_details": None
            }
        raise ModeOneException(reply)
    store_name = store_name.encode("utf-8")

    if store_name == b"Bolt Market Vitan":
        store_id = 250217543
    elif store_name == b"Bolt Market Central":
        store_id = 250217544
    elif store_name == b"Bolt Market Apaca":
        store_id = 250217541
    elif store_name in [
        b"Bolt Market Bun\xc4\x83 Ziua",
        b"Bolt Market Buna\xcc\x86 Ziua",
    ]:
        store_id = 250217542
    else:
        logger.critical(f"Store name {store_name} untreated. Danone files")
        reply = {
                "function_name": "SuppMod-One",
                "error_message": f"Store name {store_name} untreated. Danone files",
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        df = pd.read_excel(os.path.join(tmp_folder, old_name))
    except FileNotFoundError:
        logger.critical(f"Could not find {old_name}")
        reply = {
                "function_name": "SuppMod-One",
                "error_message": f"Could not find {old_name}",
                "error_details": None
            }
        raise ModeOneException(reply)

    df["Cod magazin"] = store_id
    df["EAN"] = df["EAN"].astype(str)
    df.to_excel(os.path.join(tmp_folder, old_name), index=False)
    
    del df
    
    logger.info("Danone files updated and saved locally")
    return


def starfoods_mods(name_elements):
    # generate the new name
    try:
        store_name = name_elements[1]
    except IndexError:
        message = "Errors in parsing the file name - StarFoods -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    store_name = store_name.encode("utf-8")

    if store_name == b"Bolt Market Vitan":
        store_id = 200751576
    elif store_name == b"Bolt Market Central":
        store_id = 200764451
    elif store_name == b"Bolt Market Apaca":
        store_id = 200751579
    elif store_name in [
        b"Bolt Market Bun\xc4\x83 Ziua",
        b"Bolt Market Buna\xcc\x86 Ziua",
    ]:
        store_id = 200770772
    else:
        message = f"Store name {store_name} untreated. - StarFoods -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    numar_po = "-".join(
        [name_elements[-3], name_elements[-2], name_elements[-1].split(".")[0]]
    )

    new_name = (
        " ".join(
            [
                "Comanda",
                numar_po,
                "Star Foods",
                str(store_id),
            ]
        )
        + ".xlsx"
    )

    logger.info("Starfoods files updated, new name generated")
    return new_name


def auchan_mods(name_elements, old_name):
    # modify the file content
    # 1. generate file's new name
    try:
        store_name = name_elements[1]
    except IndexError:
        message = "Errors in parsing the file name - Auchan -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    store_name = store_name.encode("utf-8")

    if store_name == b"Bolt Market Vitan":
        store_tag = "Bolt 03"
    elif store_name == b"Bolt Market Central":
        store_tag = "Bolt 05"
    elif store_name == b"Bolt Market Apaca":
        store_tag = "Bolt 04"
    elif store_name in [
        b"Bolt Market Bun\xc4\x83 Ziua",
        b"Bolt Market Buna\xcc\x86 Ziua",
    ]:
        store_tag = "Bolt 01"
    else:
        message = f"Store name {store_name} untreated. - Auchan -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    new_name = (
        "_".join(
            [
                store_tag.replace(" ", ""),
                "comenzi",
                today.strftime("%Y%m%d"),
                today.strftime("%H%M%S"),
            ]
        )
        + ".xlsx"
    )

    # 2. modify content
    try:
        df_po = pd.read_excel(os.path.join(tmp_folder, old_name))
    except FileNotFoundError:
        message = f"File {old_name} not found - Auchan -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        df_po.rename(
            columns={
                "PO #": "Cod comanda (intern client)",
                "Plan Qty": "Cantitate",
                "Supplier SKU": "Cod Produs",
                "Req. delivery time": "Timestamp",
            },
            inplace=True,
        )

        df_po.drop(
            columns=[
                "No.",
                "Product Name",
                "EAN",
                "Supplier Name",
                "Store Name",
                "Provider Id",
                "Bolt SKU",
                "Unit",
            ],
            inplace=True,
        )
    except Exception as e:
        message = f"Errors in Cod Auchan structure: {str(e)}"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    df_po["Cod Client"] = store_tag
    df_po["Data plasare comenzi"] = today.strftime("%d.%m.%Y")
    df_po["Denumire Produs"] = ""
    df_po["Unitate Masura"] = ""
    df_po["Pret Unitar"] = ""
    df_po["Pret"] = ""
    df_po["Total Comanda"] = ""
    df_po["Timestamp"] = today.strftime("%d.%m.%Y") + " 10:30"

    # 3. fill Cod Produs with leading zeros
    try:
        df_po["Cod Produs"] = df_po["Cod Produs"].astype(int).astype(str).str.zfill(6)
    except Exception as e:
        message = f"Auchan CodProdus conversion error: {str(e)}"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    # 4. save the file
    df = df_po[
        [
            "Cod Client",
            "Data plasare comenzi",
            "Cod comanda (intern client)",
            "Cod Produs",
            "Denumire Produs",
            "Cantitate",
            "Unitate Masura",
            "Pret Unitar",
            "Pret",
            "Total Comanda",
            "Timestamp",
        ]
    ].copy()
    df.to_excel(os.path.join(tmp_folder, old_name), index=False)
    
    del df, df_po

    logger.info("Auchan files updated and saved.")
    return new_name


def cocacola_mods(old_name):
    # modify file content
    try:
        df_po = pd.read_excel(os.path.join(tmp_folder, old_name))
    except FileNotFoundError:
        message = f"File {old_name} not found - CocaCola/Stockday -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        df_bx = pd.read_excel(
            os.path.join("/tmp", "Cerinte comanda minima.xlsx"),
            sheet_name=1,
            usecols="A:D",
        )
        df_bx.rename(columns={"Bulk quantity, units": "Bax"}, inplace=True)
    except Exception as e:
        message = f"Eroare fisier baxaj - CocaCola/Stockday - : {str(e)}"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        df_po = df_po.merge(
            df_bx[["SKU", "Bax"]], how="left", left_on="Bolt SKU", right_on="SKU"
        )
    except:
        message = "Merge operation failed - CocaCola/Stockday -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    df_po["Plan Qty"] = round(df_po["Plan Qty"] / df_po["Bax"])
    df_po["EAN"] = df_po["EAN"].astype(str)

    df_po.drop(columns=["Bax", "SKU"], inplace=True)

    df_po.to_excel(os.path.join(tmp_folder, old_name), index=False)
    
    del df_po, df_bx

    logger.info("CocaCola files updated and saved")
    return


def quadrant_mods(name_elements, old_name):
    # modify file content
    try:
        df_po = pd.read_excel(os.path.join(tmp_folder, old_name))
    except FileNotFoundError:
        message = f"Could not find {old_name} - Quadrant -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)
    
    try:
        df_bx = pd.read_excel(
            os.path.join("/tmp", "Cerinte comanda minima.xlsx"),
            sheet_name=1,
            usecols="A:D",
        )
        df_bx.rename(columns={"Bulk quantity, units": "Bax"}, inplace=True)
    except Exception as e:
        message = f"Eroare fisier baxaj - Quadrant - : {str(e)}"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        df_po = df_po.merge(
            df_bx[["SKU", "Bax"]], how="left", left_on="Bolt SKU", right_on="SKU"
        )
    except:
        message = "Merge operation failed - Quadrant -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    df_po["Plan Qty"] = round(df_po["Plan Qty"] / df_po["Bax"])

    df_po.drop(columns=["Bax", "SKU", "No."], inplace=True)
    df_po["EAN"] = df_po["EAN"].astype(str)

    df_po.to_excel(os.path.join(tmp_folder, old_name), index=False)

    # rename file
    try:
        store_name = name_elements[2]
    except IndexError:
        message = "Errors in parsing the file name - Quadrant -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    store_name = store_name.encode("utf-8")

    numar_po = "-".join(
        [name_elements[-3], name_elements[-2], name_elements[-1].split(".")[0]]
    )

    if store_name == b"Bolt Market Vitan":
        store_id = 200751576
    elif store_name == b"Bolt Market Central":
        store_id = 200764451
    elif store_name == b"Bolt Market Apaca":
        store_id = 200751579
    elif store_name in [
        b"Bolt Market Bun\xc4\x83 Ziua",
        b"Bolt Market Buna\xcc\x86 Ziua",
    ]:
        store_id = 200770772
    else:
        message = f"Store name {store_name} untreated. Quadrant -"
        logger.critical(message)
        reply = {
                "function_name": "SuppMod-One",
                "error_message": message,
                "error_details": None
            }
        raise ModeOneException(reply)

    new_name = (
        " ".join(
            [
                "Comanda",
                numar_po,
                "Quadrant",
                str(store_id),
            ]
        )
        + ".xlsx"
    )

    del df_bx, df_po
    
    logger.info("Quadrant files updated and saved")
    return new_name


def handler(event, context):
    # download missing S3 input files
    s3 = boto3.client("s3", region="eu-north-1")

    bax = "purchasing-orders/input/Cerinte comanda minima.xlsx"
    auc = "purchasing-orders/input/Coduri Auchan.xlsx"
    bag = "purchasing-orders/input/MailBag.csv"
    zip = "purchasing-orders/input/Bolt PO.zip"

    file_baxaj = "/tmp/Cerinte comanda minima.xlsx"
    file_auchan = "/tmp/Coduri Auchan.xlsx"
    file_zip = "/tmp/Bolt PO.zip"
    file_bag = "/tmp/MailBag.csv"

    # download the input files from S3 to local folder
    try:
        s3.download_file(BUCKET, bax, file_baxaj)
        s3.download_file(BUCKET, auc, file_auchan)
        s3.download_file(BUCKET, bag, file_bag)
        s3.download_file(BUCKET, zip, file_zip)
    except:
        logger.critical("Failed to download one or more input files from S3")
        reply = {
                "function_name": "SuppMod-One",
                "error_message": "One or more input files could not be downloaded from or do not exist on S3",
                "error_details": None
            }
        raise ModeOneException(reply)

    # unzip the daily orders file
    try:
        with ZipFile(file_zip) as zipfile:
            zipfile.extractall("/tmp")
    except Exception as e:
        logger.info(f"Zip extraction error: {str(e)}")
        reply = {
                "function_name": "SuppMod-One",
                "error_message": f"Zip extraction error: {str(e)}",
                "error_details": None
            }
        raise ModeOneException(reply)
    logger.info(f"Unzipped daily files.")

    # read the original Mail Bag and proceed with modifications
    df = pd.read_csv(file_bag)

    for i in range(len(df)):
        supplier = df["supplier"].at[i]
        files_list = df["files"].at[i]
        files = ast.literal_eval(files_list)
        
        logger.info(f"Supplier: {supplier} found")

        if supplier.split(" ")[0] == 'DANONE':
            for file in files:
                name_elements = file.split("-")
                # execute changes
                danone_mods(name_elements, file)

        elif supplier == "STAR FOODS E.M. SRL":
            for file in files:
                name_elements = file.split("-")
                # execute changes
                new_name = starfoods_mods(name_elements)

                # rename the actual file in the tmp folder
                os.rename(
                    os.path.join(tmp_folder, file), os.path.join(tmp_folder, new_name)
                )

                # replace the item in MailBag
                files_list = files_list.replace(file, new_name)
            df["files"].at[i] = str(files_list)
            df.to_csv(os.path.join("/tmp", "MailBag.csv"), index=False)

        elif supplier.split(" ")[0] == "AUCHAN":
            for file in files:
                logger.info(f"File {file} in process")
                name_elements = file.split("-")
                # execute changes
                new_name = auchan_mods(name_elements, file)
                if new_name is None:
                    logger.error(f"Could not modify {file}")
                    continue
                # rename the actual file in the tmp folder
                os.rename(
                    os.path.join(tmp_folder, file), os.path.join(tmp_folder, new_name)
                )
                # replace the item in MailBag
                files_list = files_list.replace(file, new_name)
            df["files"].at[i] = str(files_list)
            df.to_csv(os.path.join("/tmp", "MailBag.csv"), index=False)

        elif supplier == "QUADRANT-AMROQ BEVERAGES SRL":
            for file in files:
                logger.info(f"File {file} in process")
                name_elements = file.split("-")
                # execute changes
                new_name = quadrant_mods(name_elements, file)

                # rename the actual file in the tmp folder
                os.rename(
                    os.path.join(tmp_folder, file), os.path.join(tmp_folder, new_name)
                )

                # replace the item in MailBag
                files_list = files_list.replace(file, new_name)
            df["files"].at[i] = str(files_list)
            df.to_csv(os.path.join("/tmp", "MailBag.csv"), index=False)

        elif supplier in ["COCA COLA HBC ROMANIA SRL", "STOCKDAY SRL"]:
            for file in files:
                logger.info(f"File {file} in process")
                name_elements = file.split("-")
                # execute changes
                cocacola_mods(file)
                
    # save all working files (orders and updated MailBag.csv) in s3
    try:
        s3.upload_file(
            "/tmp/MailBag.csv", 
            "bolt-projects", 
            "purchasing-orders/input/MailBag.csv")
    except Exception as err:
        reply = {
                "function_name": "SuppMod-One",
                "error_message": f"Cannot save MailBag to s3. Error: {str(err)}",
                "error_details": None
            }
        raise ModeOneException(reply)

    try:
        files = os.listdir(tmp_folder)

        for file_name in files:
            file_path = os.path.join(tmp_folder, file_name)
            s3_key = f'purchasing-orders/wrk/{file_name}'
            s3.upload_file(file_path, BUCKET, s3_key)
    except Exception as err:
        reply = {
                "function_name": "SuppMod-One",
                "error_message": f"Cannot save Orders to s3. Error: {str(err)}",
                "error_details": None
            }
        raise ModeOneException(reply)
    
    response_json = {
        "function_name": "SuppMod-One",
        "error_message": None,
        "error_details": None
        }
    
    return json.dumps(response_json)