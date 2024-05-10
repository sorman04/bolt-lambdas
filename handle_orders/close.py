
""" function updates the portal robots table 
    and sends the warning mails
"""

import json
import logging
import boto3 
import pytz
import croniter
import yagmail

from logging import INFO
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

session = boto3.session.Session()
sm_client = session.client(
    service_name='secretsmanager',
    region_name='eu-central-1'
)

def handler(event, context):
    
    try:
        response = sm_client.get_secret_value(
            SecretId='RoboPortal-secrets'
        )
        secrets = json.loads(response['SecretString'])
        MAIL_PASSWORD = secrets["MAIL_PASSWORD"]
    except ClientError as e:
        logger.critical(f'Error getting secret: {str(e)}')
        reply = {
                "function_name": "RobotClose",
                "error_message": f"Secrets Manager Error: {str(e)}",
                "error_details": None
            }
        return json.dumps(reply)
    
    ddb_region = event.get('ddb_region')
    spf_region = event.get('spf_region')
    robot_arn = event.get('robot_arn')
    robot_version = event.get('robot_version')
    mail_target = ['sorin@robotlab.ro']
    
    if robot_version == 0:
        robot_full = robot_arn
    else:
        robot_full = ":".join([robot_arn, str(robot_version)])
    
    try:
        error_message = None
        error_details = None
        last_message = "Succeeded"
        
        # 1. retrieve the robot execution's start timestamp
        sfn_client = boto3.client('stepfunctions', region_name = spf_region)
        
        response = sfn_client.list_executions(
            stateMachineArn=robot_full,
            maxResults=1,
            )
        
        crt_execution = response.get('executions')[0]
        crt_run_date = crt_execution['startDate']
        current_exe_name = crt_execution['name']
    
        # 2. calculate the robot execution's next run timestamp
        ### get the active robot schedule
        sch_client = boto3.client('scheduler', region_name = spf_region)
        
        response = sch_client.list_schedules(GroupName='default')
        schedules_list = response["Schedules"]
        robot_schedules = [schedule['Name'] for schedule in schedules_list if (schedule['Target']['Arn'] == robot_full) and (schedule['State'] == 'ENABLED')]
    
        if len(robot_schedules) == 0:
            ### no active schedules
            next_run_date = 'N/A'
        else:
            active_schedule = sch_client.get_schedule(
                GroupName="default",
                Name= robot_schedules[0]
                )
            cron_expression = active_schedule["ScheduleExpression"]

            ### convert expression to croniter
            cron_expression = cron_expression[5:len(cron_expression)-1].replace("?", "*")
            now = datetime.now(pytz.timezone("Europe/Bucharest"))
            
            cron_obj = croniter.croniter(cron_expression, now)
            next_run_date = cron_obj.get_next(datetime)
            next_run_date = next_run_date.strftime('%Y-%m-%d %H:%M:%S')
            
        # 3. collect the error message
        if event.get("error_info") is not None:
            # we have an error
            msg = event["error_info"]["Cause"]
            msg_dict = json.loads(msg)
            val = msg_dict['errorMessage'].replace("'", "/").replace('"', "'").replace('/', '"').replace("None", '"N/A"')
            err_dict = json.loads(val)
            error_message = err_dict["error_message"]
            error_details = err_dict["error_details"]
            last_message = "Failed"
        
        # 4. Save the runtime data into RobotRuntimeData    
        dynamodb = boto3.resource('dynamodb', region_name = ddb_region)
        table = dynamodb.Table('RobotRuntimeData')
        
        response = table.update_item(
            Key={'robot_arn': robot_arn, 'version': robot_version},
            UpdateExpression='SET execution_name=:val1, last_run=:val2, next_run=:val3, last_message=:val4, last_error=:val5, error_details=:val6',
            ExpressionAttributeValues={
                ':val1': current_exe_name,
                ':val2': crt_run_date.strftime('%Y-%m-%d %H:%M:%S'),
                ':val3': next_run_date,
                ':val4': last_message,
                ':val5': error_message,
                ':val6': error_details
            }
        )
    except Exception as e:
        logger.error(f"Exception occured: {str(e)}")
        last_message = "Failed"
    
    # 5. Send error mail
    if last_message == "Failed":
        try:
            yag = yagmail.SMTP('noreply@robotlab.ro', MAIL_PASSWORD)
            subject = f"URGENT - Eroare Robot {robot_arn.split(':')[-1]}"
            body_text = f"Executia s-a incheiat cu erori. Verificati log-urile executiei!"
            yag.send(mail_target, subject, body_text)
        except Exception as e:
            logger.warn(f"Nu s-a putut trimite mailul erori ref {robot_arn.split(':')[-1]}. Eroare: {str(e)}")
            reply = {
                'function_name': "RobotClose",
                'error_message': f"Nu s-a putut trimite mailul erori ref {robot_arn.split(':')[-1]}. Eroare: {str(e)}",
                'error_details': error_details
            }
            return json.dumps(reply)

        reply = {
                'function_name': "RobotClose",
                'error_message': "Executia s-a incheiat cu erori. Verificati detaliile",
                'error_details': error_details
            }
        return json.dumps(reply)
    
    reply = {
        'function_name': "RobotClose",
        'error_message': None,
        'error_details': None
        }
    
    return json.dumps(reply)