from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.mgmt.storage import StorageManagementClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

subscription_id = os.environ['SUBSCRIPTION_ID']
resource_group_name = os.environ['RESOURCE_GROUP_NAME']
client_id = os.environ['AZURE_CLIENT_ID']
tenant_id = os.environ['AZURE_TENANT_ID']
client_secret = os.environ['AZURE_CLIENT_SECRET']
account_url = os.environ['AZURE_STORAGE_URL']
storage_account_key = os.environ['STORAGE_ACCOUNT_KEY']
storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
container_name = os.environ['CONTAINER_NAME']
blob_name = "upload_sample.txt"

credentials = ClientSecretCredential(
    client_id=client_id,
    client_secret=client_secret,
    tenant_id=tenant_id
)


def get_blob_service_client():
    return BlobServiceClient(
        account_url=account_url, credential=credentials)


def get_container_client(blob_service_client, container_name):
    return blob_service_client.get_container_client(
        container=container_name)


def get_blob_data(container_name, blob_name):

    blob_service_client = get_blob_service_client()

    blob_client = get_container_client(
        blob_service_client, container_name)

    blob_client = blob_client.get_blob_client(blob=blob_name)

    data = blob_client.download_blob().readall().decode("utf-8")
    return data


def list_blob(container_name):
    blob_names_list = []

    blob_service_client = get_blob_service_client()

    container_name = blob_service_client.get_container_client(
        container=container_name)

    for blob in container_name.list_blobs():
        blob_names_list.append(blob.name)

    return blob_names_list


def get_multi_blob_data(container_name):

    # get all blobs names
    list_of_blobs = list_blob(container_name)
    for blob in list_of_blobs:
        blob_data = get_blob_data(container_name, blob)
        print(blob_data)


def upload_blob(container_name, filename):
    try:
        blob_service_client = get_blob_service_client()

        blob_client = get_container_client(
            blob_service_client, container_name)

        if not blob_client.exists():
            blob_service_client.create_container()
            print(f"Container '{container_name}' created.")

        else:
            blob_client = blob_client.get_blob_client(filename)
            with open(filename, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"File '{filename}' uploaded as '{filename}'.")

    except Exception as e:
        print(f"Error: {e}")


def get_storage_account_keys():
    storage_client = StorageManagementClient(credentials, subscription_id)
    try:
        keys = storage_client.storage_accounts.list_keys(
            resource_group_name, storage_account_name
        )

        key1 = keys.keys[0].value
        return key1
    except Exception as e:
        print(f"Error: {e}")
        return None


def generate_sas_token(container_name, blob_name):
    try:
        sas_token = generate_blob_sas(
            account_name=storage_account_name,
            account_key=get_storage_account_keys(),
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=24)
        )
        return sas_token
    except Exception as e:
        print(f"Error generating SAS token: {e}")
        return None

# print(get_blob_data(container_name, 'sample1.txt'))
# print(list_blob(container_name))
# get_multi_blob_data(container_name)


# upload_blob(container_name, blob_name)

sas_token = generate_sas_token(container_name, blob_name)
if sas_token:
    blob_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    print("File URL with SAS Token:")
    print(blob_url)
