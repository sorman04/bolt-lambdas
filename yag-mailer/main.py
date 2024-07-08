""" module handles the generation and sending of daily emails 

    if needs a payload:
    
    "mailing_context": string_variable
    
    where string_variable can be:
        "test-intern"
        "test-bolt"
        "test-sorin"
        "live"
    
"""

import os, csv, ast
import logging
import pytz
import yagmail
import json
import pandas as pd
import boto3

from datetime import datetime
from logging import INFO
from botocore.exceptions import ClientError

class MailerException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

session = boto3.session.Session()
sm_client = session.client(
    service_name='secretsmanager',
    region_name='eu-central-1'
)

try:
    response = sm_client.get_secret_value(
        SecretId='BoltPo-Robot'
    )
    secrets = json.loads(response['SecretString'])
except ClientError as e:
    logger.critical(f'Error getting secret: {str(e)}')
    reply = {
            "function_name": "Mailer",
            "error_message": f"Secrets Manager Error: {str(e)}",
            "error_details": None
        }
    raise MailerException(reply)

MAIL_SENDER = secrets["MAIL_SENDER"]
MAIL_PASSWORD = secrets["MAIL_PASSWORD"]

tmp_folder = "/tmp/wrk"
os.makedirs(tmp_folder)


RL_TO_RECIPIENTS = ["sorin@robotlab.ro", "cosmin@robotlab.ro"]
RL_CC_RECIPIENTS = ["office@robotlab.ro"]
BL_RECIPIENTS = ["rosupplychain@bolt.eu"]

MASK_SENDER = {MAIL_SENDER: BL_RECIPIENTS[0]}


def handler(event, context):
    
    BUCKET = "bolt-projects"
    
    mailing_context = event.get("mailing_context")
    
    # download the working files
    s3 = boto3.client("s3")

    bag = "purchasing-orders/input/MailBag.csv"
    jsn = "purchasing-orders/input/data.json"
    
    file_bag = "/tmp/MailBag.csv"
    file_jsn = "/tmp/data.json"

    # download the input files from S3 to local folder
    try:
        s3.download_file(BUCKET, bag, file_bag)
        s3.download_file(BUCKET, jsn, file_jsn)
    except:
        message = "Failed to download one or more input files from S3"
        logger.critical(message)
        reply = {
                "function_name": "Mailer",
                "error_message": message,
                "error_details": None
            }
        raise MailerException(reply)
    
    # download all orders from s3 wrk subfolder
    s3_prefix = "purchasing-orders/wrk/"
    try:
        # List objects in the specified S3 folder
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)

        # Download each file to the local directory  
        # ====== It returns the first 1000 files. use paginator if more than 1000 files are to be downloaded ==== #
        for obj in objects.get('Contents', []):
            s3_key = obj['Key']
            file_name = os.path.basename(s3_key)
            if file_name != "":
                local_file_path = os.path.join(tmp_folder, file_name)
                s3.download_file(BUCKET, s3_key, local_file_path)
    except Exception as e:
        message = f"Orders download failure: {str(e)}"
        logger.critical(message)
        reply = {
                "function_name": "Mailer",
                "error_message": message,
                "error_details": None
            }
        raise MailerException(reply)
    logger.info("Orders successfully downloaded")
    
    # read summary details for bolt daily mail
    with open("/tmp/data.json", "r", encoding="utf-8") as file:
        orders_summary = json.load(file)
        not_in_cad = orders_summary["error_details"]["not-in-cad"]
        not_in_wms = orders_summary["error_details"]["not-in-wms"]
        not_in_mov = orders_summary["error_details"]["not-in-mov"]
        no_mov = orders_summary["error_details"]["no-mov"]
        both_mov = orders_summary["error_details"]["both-mov"]
    logger.info("Exceptions loaded")

    # start scanning the Mailbag and sending mails
    no_addresses = []
    failed_mails = []  # we will append suppliers name for which mailing failed
    sent_mails = []  # list with the mails sent

    with open("/tmp/MailBag.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            supplier = row["supplier"]
            logger.info(f"started processing {supplier}")

            recipients = row["address"]
            recipients = (
                recipients.replace("]", "")
                .replace("[", "")
                .replace("'", "")
                .split(", ")
            )
            # check if we have email addresses
            if len(recipients) == 0:
                no_addresses.append(supplier)
                logger.info(f"no identified recipients for {supplier}. abort")
                continue
            else:
                logger.info(f"we found the following recipients: {recipients}")

            is_green = row["is_green"]
            is_green = (
                is_green.replace("]", "")
                .replace("[", "")
                .replace("'", "")
                .split(", ")[0]
            )
            if is_green != "da":
                logger.info(f"{supplier} has green status: {is_green}. Abort")
                continue

            files = row["files"]
            files = ast.literal_eval(files)
            files = [file.strip() for file in files]
            attachments = [os.path.join(tmp_folder, file) for file in files]
            logger.info(f"there are {len(attachments)} orders attached for {supplier}")   

            logger.info(f"{supplier} mail compose started")

            # select email procedure context and addresses
            if mailing_context == "test-intern":
                TO_RECIPIENTS = RL_TO_RECIPIENTS
                CC_RECIPIENTS = RL_CC_RECIPIENTS
            elif mailing_context == "test-bolt":
                TO_RECIPIENTS = BL_RECIPIENTS
                CC_RECIPIENTS = RL_CC_RECIPIENTS
            elif mailing_context == "test-sorin":
                TO_RECIPIENTS = ["sorin@robotlab.ro"]
                CC_RECIPIENTS = []
            elif mailing_context == "live":
                TO_RECIPIENTS = recipients
                CC_RECIPIENTS = BL_RECIPIENTS
            else:
                message = "Context unknown"
                logger.critical(message)
                reply = {
                        "function_name": "Mailer",
                        "error_message": message,
                        "error_details": None
                    }
                raise MailerException(reply)

            # compose message
            supp_body = """
            Buna ziua,
            
            Va rugam sa gasiti atasat o noua comanda.
            Avem rugamintea sa <b>atasati codurile de #PO, de pe a doua coloana, pe fiecare factura aferenta livrarilor 
            pentru fiecare locatie.</b>

            Pentru orice intrebare, va rugam sa ne contactati.
            
            Multumim,
            Echipa Bolt Romania
            """
            
            yag = yagmail.SMTP(MASK_SENDER, MAIL_PASSWORD)
            if supplier != "STOCKDAY SRL":
                # send mail with all attachments in one message (all except Stockday SRL)
                try:
                    yag.send(
                        to=TO_RECIPIENTS,
                        cc=CC_RECIPIENTS,
                        subject=f'PO - {datetime.now(pytz.timezone("Europe/Bucharest")).strftime("%d.%m.%Y")}',
                        contents=supp_body,
                        attachments=attachments,
                    )
                except Exception as err:
                    logger.info(f"mail to {supplier} failed with error: {str(err)}")
                    failed_mails.append(supplier)
                    continue

                stores = [
                    attachment.split("/")[-1]
                    .replace(row["supplier"], "")
                    .strip()[1:]
                    .split("-")[0]
                    for attachment in attachments
                ]
                for store in stores:
                    sent_mails.append([supplier, store])

                logger.info(f"mail sent to {supplier}")
            else:
                # if is Stockday SRL send mail, attachment by attachment
                for attachment in attachments:
                    try:
                        yag.send(
                            to=TO_RECIPIENTS,
                            cc=CC_RECIPIENTS,
                            subject=f'PO - {datetime.now(pytz.timezone("Europe/Bucharest")).strftime("%d.%m.%Y")}',
                            contents=supp_body,
                            attachments=attachment,
                        )
                    except Exception as err:
                        logger.info(f"stockday mail to {supplier} failed with error: {str(err)}")
                        failed_mails.append(supplier)
                        continue

                    store = attachment.split("/")[-1].replace(row["supplier"], "").strip()[1:].split("-")[0]
                    sent_mails.append([supplier, store])

                    logger.info(f"mail sent to {supplier}")

        logger.info(failed_mails)

    # send summary mail to Bolt
    sent_file_name = f"SummaryPO_{datetime.now().strftime('%d-%m-%Y')}.xlsx"
    df = pd.DataFrame(sent_mails, columns=["furnizori", "store"])
    df.to_excel(os.path.join("/tmp/", sent_file_name))

    if df.shape[0] == 0:
        # nu s-a trimis nimic
        summary_attached = []
    else:
        summary_attached = [os.path.join("/tmp/", sent_file_name)]

    bolt_body = f"""
                Buna ziua,
                
                Procesul de trimitere POs a rulat cu succes. Regasiti aici rezumatul rularii.

                1. O lista cu mesajele trimise catre furnizori a fost atasata la prezentul mesaj.
                2. Urmatorii furnizori prezinta o serie de discrepante, dupa cum se mentioneaza mai jos:
                    - Furnizori fara adresa de email: {no_addresses}
                    - Furnizori pentru care s-a gasit PO, dar care nu apar in Cadentar: {not_in_cad}
                    - Furnizori care apar in Cadentare, dar pentru care nu s-a gasit PO: {not_in_wms}
                    - Furnizori a caror comenzi nu indeplinesc cerintele minime de cantitate: {not_in_mov}
                    - Furnizori care au unele comenzi care indeplinesc si altele care nu indeplinesc cerintele minime de cantitate: {both_mov}
                    - Furnizori pentru care nu s-a gasit nicio informatie legata de cerintele minime de cantitate: {no_mov}
                    
                3. Nu s-a reusit trimiterea mailurilor catre urmatorii furnizori : {failed_mails}

                Mentionam ca nu s-au trimis email-uri catre furnizorii prezenti in <b>oricare</b> din cele 3 liste legate de cerintele minime de cantitate.</i>

                Multumim,
                Echipa RobotLab
                """

    # reconfigure addressees for summary
    if mailing_context == "test-intern":
        TO_RECIPIENTS = RL_TO_RECIPIENTS
        CC_RECIPIENTS = RL_CC_RECIPIENTS
    elif mailing_context == "test-bolt":
        TO_RECIPIENTS = BL_RECIPIENTS
        CC_RECIPIENTS = RL_CC_RECIPIENTS
    elif mailing_context == "test-sorin":
        TO_RECIPIENTS = ["sorin@robotlab.ro"]
        CC_RECIPIENTS = []
    elif mailing_context == "live":
        TO_RECIPIENTS = BL_RECIPIENTS
        CC_RECIPIENTS = RL_CC_RECIPIENTS
    else:
        message = "Context unknown"
        logger.critical(message)
        reply = {
                "function_name": "Mailer",
                "error_message": message,
                "error_details": None
            }
        raise MailerException(reply)

    yag = yagmail.SMTP(MASK_SENDER, MAIL_PASSWORD)
    try:
        yag.send(
            to=TO_RECIPIENTS,
            cc=CC_RECIPIENTS,
            subject=f'PO - {datetime.now(pytz.timezone("Europe/Bucharest")).strftime("%d.%m.%Y")}',
            contents=bolt_body,
            attachments=summary_attached,
        )
    except Exception as err:
        message = f"Mail to Bolt failed: {str(err)}"
        logger.critical(message)
        reply = {
                "function_name": "Mailer",
                "error_message": message,
                "error_details": None
            }
        raise MailerException(reply)
    
    return {
        "function_name": "Mailer",
        "error_message": None,
        "error_details": None
        }