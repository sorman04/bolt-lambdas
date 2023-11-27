import requests
import json


def handler(event, context):
    url = "http://18.153.175.246:8080/api/admin/check-health"
    response = requests.get(url)

    return {
        "function_name": "Bolt-PO-CheckHealth",
        "output": json.loads(response.content),
    }
