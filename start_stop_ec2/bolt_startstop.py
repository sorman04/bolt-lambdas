""" function start or stops the ec2 machines associated with Bolt-PO project


    function expects an event with the following format:
    
    {
        "instances": "jumpbox,scrap-machine-dev",
        "action": "Stop"
    }

    {
        "instances": "jumpbox,scrap-machine-dev",
        "action": "Start"
    }

"""

import json
import boto3
from time import sleep

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

WAIT_TIME = 10

region = "eu-central-1"
ec2 = boto3.client(
    "ec2",
    region_name=region,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def get_instance_ids(instance_names):
    all_instances = ec2.describe_instances()

    instance_ids = []

    # get instance-id based on instance name
    for instance_name in instance_names:
        for reservation in all_instances["Reservations"]:
            for instance in reservation["Instances"]:
                if "Tags" in instance:
                    for tag in instance["Tags"]:
                        if tag["Key"] == "Name" and tag["Value"] == instance_name:
                            instance_ids.append(instance["InstanceId"])

    return instance_ids


def handler(event, context):
    """instance_names = event["instances"].split(",")
    action = event["action"]"""

    instance_names = event[list(event)[-1]]["EC2_fc_instance"].split(",")
    action = event[list(event)[-1]]["EC2_fc_action"]

    instance_ids = get_instance_ids(instance_names)

    if action == "Start":
        ec2.start_instances(InstanceIds=instance_ids)
        # check if machines are running
        for instance in instance_ids:
            for i in range(10):
                sleep(12)
                response = ec2.describe_instance_status(
                    InstanceIds=[
                        instance,
                    ],
                )
                try:
                    if response["InstanceStatuses"][0]["InstanceState"]["Code"] == 16:
                        break
                except:
                    if i == 9:
                        return {
                            "function_name": "Bolt-PO-StartStopEC2",
                            "instance_id": instance,
                            "message": "start failed in 120 s. Abort",
                        }
    elif action == "Stop":
        ec2.stop_instances(InstanceIds=instance_ids)

    return {
        "function_name": "Bolt-PO-StartStopEC2",
    }
