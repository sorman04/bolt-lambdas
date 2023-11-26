import requests

def handler(event, context):
    url = "https://jsonplaceholder.typicode.com/todos/1"
    response = requests.get(url)
    res = {
        "event": event,
        "output": response.json(),
        "context": context,
    }
    print(res)
    
    return None