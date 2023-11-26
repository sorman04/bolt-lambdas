import requests
import json

def handler(event, context):
    url = "http://18.153.175.246:8080/api/admin/check-health"
    response = requests.get(url)
    res = {
        "event": event,
        "output": json.loads(response.content),
        "context": context,
    }
    print(res)
    
    return None
