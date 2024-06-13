import requests
import base64
import json
import mimetypes
from datetime import datetime



def get_access_token():
    url = "https://0359f11dtrial.authentication.us10.hana.ondemand.com/oauth/token"
    client_id = "YOUR CLIENT ID"
    client_secret = "YOUR CLIENT SECRET"
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    data = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }

    token = requests.request("POST", url, headers=headers, data=data)
    if token.status_code >= 400:
        raise Exception("Could not obtain token")
    return token.json()["access_token"]


def create_job(filepath, token):
    url = "https://aiservices-trial-dox.cfapps.us10.hana.ondemand.com/document-information-extraction/v1/document/jobs"

    options = {
        "schemaId": "cf8cc8a9-1eee-42d9-9a3e-507a61baac23",
        "clientId": "default",
        "documentType": "invoice",
        "receivedDate": datetime.today().strftime('%Y-%m-%d')
    }

    # The payload needs to be formatted as form-data
    payload = {
        'options': json.dumps(options)
    }

    mime_type, _ = mimetypes.guess_type(filepath)
    if mime_type is None:
        mime_type = 'application/octet-stream'

    # Ensure the file is opened in binary mode
    files = {
        'file': ('sample-invoice-3.pdf', open(filepath, 'rb'), mime_type)
    }

    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }

    # Make the request with data and files
    response = requests.post(url, headers=headers, data=payload, files=files)
    print(response.json())
    return response.json()


def get_job_status(job_id, token):
    url = f"https://aiservices-trial-dox.cfapps.us10.hana.ondemand.com/document-information-extraction/v1/document/jobs/{job_id}"
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.json())
    return response.json()
