import requests


def handler(event, context):
    url = "http://18.153.175.246:8080/api/admin/check-health"
    _ = requests.get(url)

    return {
        "function_name": "Bolt-PO-CheckHealth",
    }
