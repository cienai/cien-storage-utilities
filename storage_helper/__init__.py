# import boto3  # type: ignore
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, generate_container_sas  # type: ignore
from azure.core.exceptions import ResourceExistsError  # type: ignore
from azure.storage.filedatalake import DataLakeServiceClient  # type: ignore

from urllib.parse import urlparse
import json
from typing import Union
import time
import os
from typing import Tuple, Optional
from datetime import datetime, timedelta
import glob

# disable annoying logging
import logging

# Set the logging level for all azure-storage-* libraries
logger = logging.getLogger('azure.storage')
logger.setLevel(logging.INFO)

# Set the logging level for all azure-* libraries
logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)


"""
Connections

The following environment variables are required to be set in the environment
where the code is running:
    For AWS:
    BUCKET_URI: The URI of the cloud storage bucket
    AWS_ACCESS_KEY_ID: The AWS access key ID
    AWS_SECRET_ACCESS_KEY: The AWS secret access key

    For Azure:
    BUCKET_URI: The URI of the cloud storage bucket
    AZURE_STORAGE_ACCOUNT: The Azure storage account name
    AZURE_STORAGE_ACCESS_KEY: The Azure storage account access key
"""


def safe_conn(conn: Union[str, dict]) -> dict:
    """
    Returns a safe connection object
    """
    if isinstance(conn, str):
        return json.loads(conn)
    elif isinstance(conn, dict):
        return conn
    else:
        raise Exception('Invalid connection object')


def parse_cloud_storage_uri(uri: str) -> Tuple[str, str, str]:
    """
    Parses a cloud storage uri and returns the bucket, prefix and scheme.
    Examples include the following
        aws_uri = "s3://your-bucket-name/path/to/object"
        gcp_uri = "gs://your-bucket-name/path/to/object"
        azure_blob_uri = "wasbs://your-container-name@your-account-name.blob.core.windows.net/path/to/blob"
        azure_container_uri = "wasb://your-container-name@your-account-name.blob.core.windows.net/path/to/object"
    """
    parsed_url = urlparse(uri)
    scheme = parsed_url.scheme
    bucket = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    return (bucket, prefix, scheme)


def parse_wasb_url(wasb_url):
    """
    returns the account name, container name, and path/filename from a wasb URL
    URL can be in the following formats:
    wasb//{container_name}@{account_name}.blob.core.windows.net/{path_and_filename}
    wasb//{account_name}.blob.core.windows.net/{container_name}/{path_and_filename}
    """
    # Remove "wasb://" prefix
    url_without_prefix = wasb_url.replace("wasb://", "")
    url_without_prefix = wasb_url.replace("wasbs://", "")
    # Split the URL by "/"
    components = url_without_prefix.split("/")
    # Extract account name, container name, and path/filename
    if "@" in components[0]:
        # URL format: wasb//{container_name}@{account_name}.blob.core.windows.net/{path_and_filename}
        container_name, account_name = components[0].split("@")
    else:
        if len(components) == 1:
            # container is not specified
            account_name = components[0]
            container_name = ""
        else:
            # URL format: wasb//{account_name}.blob.core.windows.net/{container_name}/{path_and_filename}
            account_name = components[0].split(".")[0]
            container_name = components[1]
    path_and_filename = "/".join(components[2:])

    clean_storage_account_name = account_name.replace(".blob.core.windows.net", "")
    return clean_storage_account_name, container_name, path_and_filename


def get_credentials(conn: Union[str, dict]) -> Tuple[str, Optional[str]]:
    """
    Returns credentials needed to initialize storage client
    tuple of key, secret
    """
    conn = safe_conn(conn)
    uri = conn['BUCKET_URI']
    _, _, scheme = parse_cloud_storage_uri(uri)

    if scheme == 's3':
        aws_access_key_id = conn['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = conn['AWS_SECRET_ACCESS_KEY']
        return aws_access_key_id, aws_secret_access_key

    if scheme == 'gs':
        raise Exception('Google Cloud Storage not supported')
    if scheme == 'wasbs':
        # check if conn contains AZURE_STORAGE_ACCESS_KEY
        account_key = None
        if 'AZURE_STORAGE_ACCESS_KEY' in conn:
            account_key = conn['AZURE_STORAGE_ACCESS_KEY']
        elif 'AZURE_STORAGE_SAS_TOKEN' in conn:
            account_key = conn['AZURE_STORAGE_SAS_TOKEN']

        return account_key, None


def get_storage_client(conn: Union[str, dict]):
    """
    Returns an S3 client object
    """
    conn = safe_conn(conn)
    uri = conn['BUCKET_URI']
    _, _, scheme = parse_cloud_storage_uri(uri)

    # if scheme == 's3':
    #     aws_access_key_id, aws_secret_access_key = get_credentials(conn)
    #     return boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    if scheme == 'gs':
        raise Exception('Google Cloud Storage not supported')
    if scheme == 'wasbs':
        # check if conn contains AZURE_STORAGE_ACCESS_KEY
        account_key, _ = get_credentials(conn)
        account_name, _, _ = parse_wasb_url(uri)
        return BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)


def get_storage_client_type(conn: Union[str, dict]) -> str:
    """
    Returns the type of the storage client
    """
    conn = safe_conn(conn)
    uri = conn['BUCKET_URI']
    _, _, scheme = parse_cloud_storage_uri(uri)
    if scheme == 's3':
        return 'aws'
    if scheme == 'gs':
        return 'google'
    if scheme == 'wasbs':
        return 'azure'
    return None


def is_compressed_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .gz
    """
    return file_name.lower().endswith('.gz') or file_name.lower().endswith('.gzip') or \
        file_name.lower().endswith('.tar') or file_name.lower().endswith('.zip')


def is_json_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .json
    """
    return (
        file_name.lower().endswith('.json') or file_name.lower().endswith('.json.gz')
    )


def is_csv_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .csv
    """
    return (
        file_name.lower().endswith('.csv') or file_name.lower().endswith('.csv.gz')
    )


def is_txt_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .txt
    """
    return file_name.lower().endswith('.txt') or file_name.lower().endswith('.txt.gz')


def safe_uri(job_conn, prefix: str) -> str:
    """
    Returns a safe URI
    """
    bucket_uri = job_conn['BUCKET_URI']
    if not bucket_uri.endswith('/'):
        bucket_uri = f"{bucket_uri}/"
    return f"{bucket_uri}{prefix}"


def cleanout_prefix(conn, key):
    conn = safe_conn(conn)
    storage_type = get_storage_client_type(conn)
    bucket_uri = conn['BUCKET_URI']
    _, replace_prefix, _ = parse_cloud_storage_uri(bucket_uri)
    storage_type = get_storage_client_type(conn)
    if storage_type == 'azure':
        # get the container name from the url
        _, container_name, _ = parse_wasb_url(bucket_uri)
        if replace_prefix.startswith(container_name):
            replace_prefix = replace_prefix[len(container_name):]

    if replace_prefix.startswith("/"):
        replace_prefix = replace_prefix[1:]
    if not replace_prefix.endswith("/"):
        replace_prefix = replace_prefix + "/"

    # remove the replace_prefix from the key
    if key.startswith(replace_prefix):
        key = key[len(replace_prefix):]

    if key is not None and key.startswith("/"):
        key = key[1:]
    return key


def list_files(conn: Union[str, dict], prefix: str, return_details: bool = False):
    """
    Returns a list of files in bucket matching the prefix
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    # get the storage type (one of azure, aws, google)
    storage_type = get_storage_client_type(conn)
    # append the prefix to the bucket_uri
    full_uri = safe_uri(conn, prefix)
    # handle the aws case
    if storage_type == 'aws':
        bucket, real_prefix, _ = parse_cloud_storage_uri(full_uri)
        # print(f'[storage_helper.list_files(aws)] bucket: {bucket}, real_prefix: {real_prefix}')
        response = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_prefix)
        if 'Contents' in response:
            if not return_details:
                matching_files = [obj['Key'] for obj in response['Contents']]
            else:
                matching_files = [{
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'created_on': obj['LastModified'],
                    'last_modified': obj['LastModified'],
                } for obj in response['Contents']]
        else:
            matching_files = []
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_prefix = parse_wasb_url(full_uri)
        # print(f'[storage_helper.list_files(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_prefix: {real_prefix}')
        container_client = storage_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=real_prefix)
        if not return_details:
            matching_files = [blob.name for blob in blob_list]
        else:
            matching_files = [{
                'key': blob.name,
                'size': blob.size,
                'created_on': blob.creation_time,
                'last_modified': blob.last_modified,
            } for blob in blob_list]
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')

    # clean out any prefix that exists in the bucket_uri
    res = []
    if return_details:
        for obj in matching_files:
            obj['key'] = cleanout_prefix(conn, obj['key'])
            if obj['key'] != prefix:
                res.append(obj)
    else:
        for file in matching_files:
            file = cleanout_prefix(conn, file)
            if file != prefix:
                res.append(file)
    return res


def write_file(conn: Union[str, dict], key: str, data) -> None:
    """
    Writes a file to cloud storage
    """
    storage_client = get_storage_client(conn)
    try:
        storage_type = get_storage_client_type(conn)
        full_uri = safe_uri(conn, key)

        # handle the aws case
        if storage_type == 'aws':
            # parse out the bucket and the new prefix
            bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
            # print(f'[storage_helper.write_file(aws)] bucket: {bucket}, real_key: {real_key}')
            storage_client.put_object(Bucket=bucket, Key=real_key, Body=data)
        # handle the azure case
        elif storage_type == 'azure':
            _, container_name, real_key = parse_wasb_url(full_uri)
            # print(f'[storage_helper.write_file(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
            container_client = storage_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(real_key)
            blob_client.upload_blob(data, overwrite=True)
        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')
    except Exception as e:
        print(f"[storage_helper] Error writing file '{key}': {str(e)}")
    finally:
        storage_client = None


def read_file(conn: Union[str, dict], key: str) -> Union[str, bytes]:
    """
    Reads a file from an S3 bucket
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    try:
        full_uri = safe_uri(conn, key)
        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':
            bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
            # print(f'[storage_helper.read_file(aws)] bucket: {bucket}, real_key: {real_key}')
            response = storage_client.get_object(Bucket=bucket, Key=real_key)
            body = response['Body']
            data = body.read()
            # convert to string if it's a json file
            if is_json_file(key) or is_csv_file(key) or is_txt_file(key):
                if not is_compressed_file(key):
                    data = data.decode('utf-8')
            return data
        # handle the azure case
        elif storage_type == 'azure':
            _, container_name, real_key = parse_wasb_url(full_uri)
            # print(f'[storage_helper.read_file(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
            container_client = storage_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(real_key)
            data = blob_client.download_blob(offset=None, length=None, timeout=300).readall()
            if is_json_file(key) or is_csv_file(key) or is_txt_file(key):
                if not is_compressed_file(key):
                    data = data.decode('utf-8')
            return data
        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')
    except Exception as e:  # noqa
        # print(f"[storage_client] Error reading file '{key}': {str(e)}")
        return None
    finally:
        storage_client = None


def delete_file(conn: Union[str, dict], key: str) -> None:
    """
    Deletes a file
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    try:
        full_uri = safe_uri(conn, key)
        storage_type = get_storage_client_type(conn)

        # handle the aws case
        if storage_type == 'aws':
            bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
            # print(f'[storage_helper.delete_file(aws)] bucket: {bucket}, real_key: {real_key}')
            storage_client.delete_object(Bucket=bucket, Key=real_key)
        # handle the azure case
        elif storage_type == 'azure':
            _, container_name, real_key = parse_wasb_url(full_uri)
            # print(f'[storage_helper.delete_file(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
            container_client = storage_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(real_key)
            blob_client.delete_blob()
        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')
    except Exception:  # noqa
        pass
    storage_client = None


def delete_folder(conn: Union[str, dict], folder_to_delete: str) -> None:
    """
    Deletes a folder
    """
    try:
        conn = safe_conn(conn)
        storage_client = get_storage_client(conn)
        if not folder_to_delete.endswith('/'):
            folder_to_delete = folder_to_delete + '/'

        full_uri = safe_uri(conn, folder_to_delete)

        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':
            bucket, real_folder_to_delete, _ = parse_cloud_storage_uri(full_uri)
            # print(f'[storage_helper.delete_folder(aws)] bucket: {bucket}, real_folder_to_delete: {real_folder_to_delete}')
            objects_to_delete = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_folder_to_delete)
            if 'Contents' in objects_to_delete:
                for obj in objects_to_delete['Contents']:
                    storage_client.delete_object(Bucket=bucket, Key=(obj['Key']))
            storage_client.delete_object(Bucket=bucket, Key=real_folder_to_delete)
        # handle the azure case
        elif storage_type == 'azure':
            _, container_name, real_key = parse_wasb_url(full_uri)
            # print(f'[storage_helper.delete_folder(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
            container_client = storage_client.get_container_client(container_name)
            # List blobs with the specified prefix
            blob_list = container_client.list_blobs(name_starts_with=real_key)
            # Delete each blob with the specified prefix
            for blob in blob_list:
                try:
                    container_client.delete_blob(blob.name, delete_snapshots='include')
                except Exception:
                    pass

            # do the same again to delete empty folders
            blob_list = container_client.list_blobs(name_starts_with=real_key)
            for blob in blob_list:
                try:
                    container_client.delete_blob(blob.name)
                except Exception:
                    pass

        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')
    except Exception as e:
        print(f"Error deleting folder '{folder_to_delete}': {str(e)}")
    finally:
        storage_client = None


def rename_file(conn: Union[str, dict], old_file_key: str, new_file_key: str) -> None:
    """
    Renames a file
    """
    try:
        conn = safe_conn(conn)
        delete_file(conn, new_file_key)
        storage_client = get_storage_client(conn)

        full_old_uri = safe_uri(conn, old_file_key)
        full_new_uri = safe_uri(conn, new_file_key)

        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':
            bucket, real_old_key, _ = parse_cloud_storage_uri(full_old_uri)
            bucket, real_new_key, _ = parse_cloud_storage_uri(full_new_uri)
            # print(f'[storage_helper.rename_file(aws)] bucket: {bucket}, real_old_key: {real_old_key}, real_new_key: {real_new_key}')
            storage_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket,  'Key': real_old_key}, Key=real_new_key)
            delete_file(conn, old_file_key)
        # handle the azure case
        elif storage_type == 'azure':
            _, container_name, real_old_key = parse_wasb_url(full_old_uri)
            _, container_name, real_new_key = parse_wasb_url(full_new_uri)
            # print(f'[storage_helper.rename_file(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_old_key: {real_old_key}, real_new_key: {real_new_key}')
            container_client = storage_client.get_container_client(container_name)
            source_blob = container_client.get_blob_client(real_old_key)
            dest_blob = container_client.get_blob_client(real_new_key)

            # Copy the blob to the new location
            dest_blob.start_copy_from_url(source_blob.url)
            # Check the status of the copy operation
            while dest_blob.get_blob_properties().copy.status == 'pending':
                time.sleep(0.1)  # Wait for 100 ms before checking again
            source_blob.delete_blob()
        else:
            raise Exception('Unknown storage client')
    except Exception as e:
        print(f"Error renaming file '{old_file_key}' to '{new_file_key}': {str(e)}")
    finally:
        storage_client = None


def rename_folder(conn: Union[str, dict], old_folder_key: str, new_folder_key: str) -> None:
    """
    Renames a folder in cloud storage
    """
    conn = safe_conn(conn)
    if old_folder_key.endswith('/'):
        old_folder_key = old_folder_key[:1]
    if new_folder_key.endswith('/'):
        new_folder_key = new_folder_key[:1]
    # delete the new folder if it exists
    try:
        delete_folder(conn, new_folder_key)
    except Exception:
        pass

    # list all the files in the old folder
    files = list_files(conn, old_folder_key)
    for file in files:
        # rename each file in the old folder to the new folder
        new_key = new_folder_key + file[len(old_folder_key):]
        rename_file(conn, file, new_key)
    # delete the old folder
    delete_folder(conn, old_folder_key)


def check_if_file_exists(conn: Union[str, dict], key: str) -> bool:
    """
    Checks if a file exists
    """
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, key)
    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        try:
            # parse out the bucket and the new prefix
            bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
            # print(f'[storage_helper.check_if_file_exists(aws)] bucket: {bucket}, real_key: {real_key}')
            # Attempt to head the object (check if it exists)
            storage_client.head_object(Bucket=bucket, Key=real_key)
            return True  # The object exists
        except Exception:
            return False
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_key = parse_wasb_url(full_uri)
        container_client = storage_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(real_key)
        return blob_client.exists()  # The object exists
    return False


def copy_file_to_local(conn: Union[str, dict], key: str, local_file_path: str) -> None:
    """
    Copies a file from the cloud storage to the local file system
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)

    full_uri = safe_uri(conn, key)
    # ensure that the path to local_file_path exists
    local_folder_path = os.path.dirname(local_file_path)
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
        # print(f'[storage_helper.copy_file_to_local(aws)] bucket: {bucket}, real_key: {real_key}')
        storage_client.download_file(bucket, real_key, local_file_path)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_key = parse_wasb_url(full_uri)
        # print(f'[storage_helper.copy_file_to_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
        container_client = storage_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(real_key)
        with open(local_file_path, "wb") as my_blob:
            blob_data = blob_client.download_blob(offset=None, length=None, timeout=300)
            blob_data.readinto(my_blob)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def copy_file_from_local(conn: Union[str, dict], local_file_path: str, key: str) -> None:
    """
    Copies a file from the local file system to the cloud storage
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, key)
    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
        # print(f'[storage_helper.copy_file_from_local(aws)] bucket: {bucket}, real_key: {real_key}')
        storage_client.upload_file(local_file_path, bucket, real_key)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_key = parse_wasb_url(full_uri)
        # print(f'[storage_helper.copy_file_from_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_key: {real_key}')
        container_client = storage_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(real_key)
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def copy_folder_to_local(conn: Union[str, dict], folder_key: str, local_folder_path: str) -> None:
    """
    Copies a folder from the cloud storage to the local file system
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, folder_key)

    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'
    # ensure that local_folder_path exists
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_to_local(aws)] bucket: {bucket}, real_folder_key: {real_folder_key}')
        objects_to_copy = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_folder_key)
        if 'Contents' in objects_to_copy:
            for obj in objects_to_copy['Contents']:
                obj_key = obj['Key']
                filename = os.path.basename(obj_key)
                local_file_path = f"{local_folder_path}/{filename}"
                storage_client.download_file(bucket, obj_key, local_file_path)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_folder_key = parse_wasb_url(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_to_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_folder_key: {real_folder_key}')
        container_client = storage_client.get_container_client(container_name)
        blob_list = container_client.walk_blobs(name_starts_with=real_folder_key)
        for blob in blob_list:
            blob_key = blob.name
            filename = os.path.basename(blob_key)
            if len(filename) > 0:
                local_file_path = f"{local_folder_path}/{filename}"
                blob_client = container_client.get_blob_client(blob_key)
                with open(local_file_path, "wb") as my_blob:
                    blob_data = blob_client.download_blob(offset=None, length=None, timeout=300)
                    blob_data.readinto(my_blob)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def copy_folder_from_local(conn: Union[str, dict], local_folder_path: str, folder_key: str) -> None:
    """
    Copies a folder from the local file system to the cloud storage
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, folder_key)
    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        # parse out the bucket and the new prefix
        bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_from_local(aws)] bucket: {bucket}, real_folder_key: {real_folder_key}')
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                key = f"{real_folder_key}{file}"
                storage_client.upload_file(local_file_path, bucket, key)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_folder_key = parse_wasb_url(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_from_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_folder_key: {real_folder_key}')
        container_client = storage_client.get_container_client(container_name)
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                if len(file) > 0:
                    local_file_path = os.path.join(root, file)
                    key = f"{real_folder_key}{file}"
                    # print(f"file: {file}, key: {key}")
                    blob_client = container_client.get_blob_client(key)
                    with open(local_file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def copy_folder_to_local_v2(conn: Union[str, dict], folder_key: str, local_folder_path: str) -> None:
    """
    Copies a folder from the cloud storage to the local file system
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, folder_key)

    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'
    # ensure that local_folder_path exists
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_to_local(aws)] bucket: {bucket}, real_folder_key: {real_folder_key}')
        objects_to_copy = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_folder_key)
        if 'Contents' in objects_to_copy:
            for obj in objects_to_copy['Contents']:
                obj_key = obj['Key']
                filename = os.path.basename(obj_key)
                local_file_path = f"{local_folder_path}/{filename}"
                storage_client.download_file(bucket, obj_key, local_file_path)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_folder_key = parse_wasb_url(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_to_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_folder_key: {real_folder_key}')
        container_client = storage_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=real_folder_key)
        for blob in blob_list:
            blob_key = blob.name
            _, ext = os.path.splitext(blob_key)
            if not ext:
                continue

            local_file_path = local_folder_path + blob_key.replace(real_folder_key, '')
            print(f'Downloading: {blob_key} -> {local_file_path}')
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            blob_client = container_client.get_blob_client(blob_key)
            with open(local_file_path, "wb") as my_blob:
                blob_data = blob_client.download_blob(offset=None, length=None, timeout=300)
                blob_data.readinto(my_blob)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def copy_folder_from_local_v2(conn: Union[str, dict], local_folder_path: str, folder_key: str) -> None:
    """
    Copies a folder from the local file system to the cloud storage
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    full_uri = safe_uri(conn, folder_key)
    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':
        # parse out the bucket and the new prefix
        bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_from_local(aws)] bucket: {bucket}, real_folder_key: {real_folder_key}')
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                key = f"{real_folder_key}{file}"
                storage_client.upload_file(local_file_path, bucket, key)
    # handle the azure case
    elif storage_type == 'azure':
        _, container_name, real_folder_key = parse_wasb_url(full_uri)
        # ensure that folder_key ends with a '/'
        if not real_folder_key.endswith('/'):
            real_folder_key = real_folder_key + '/'
        # print(f'[storage_helper.copy_folder_from_local(azure)] storage_account_name: {storage_account_name}, container_name: {container_name}, real_folder_key: {real_folder_key}')
        container_client = storage_client.get_container_client(container_name)
        for file in glob.glob(local_folder_path + "**/*.*", recursive=True):
            key = file.replace(local_folder_path, f'{real_folder_key}/').replace('//', '/')
            print(f"Uploading: {file} -> {key}")
            blob_client = container_client.get_blob_client(key)
            with open(file, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')


def create_container(conn: Union[str, dict], container_name: str) -> str:
    """
    Creates a new container with container_name in the storage account and returns SAS token.
    Raises Exception if container with container_name already exists
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    storage_type = get_storage_client_type(conn)

    if storage_type != 'azure':
        raise Exception(f'Storage type "{storage_type}" for "create_container" not yet supported')

    try:
        storage_client.create_container(container_name)

        uri = conn['BUCKET_URI']
        storage_account, _, _ = parse_wasb_url(uri)
        sas_token = generate_container_access_token(conn, container_name)
        return {
            'BUCKET_URI': f"wasbs://{storage_account}.blob.core.windows.net/{container_name}",
            'AZURE_STORAGE_SAS_TOKEN': sas_token
        }
    except ResourceExistsError:
        raise Exception(f'Container "{container_name}" already exists')


def generate_container_access_token(conn: Union[str, dict], container_name: str, expiry: datetime = None) -> str:
    """
    Generates SAS token for container_name and optional expiration date. One year from now if "expiry" not provided.
    """
    conn = safe_conn(conn)
    storage_type = get_storage_client_type(conn)

    if storage_type != 'azure':
        raise Exception(f'Storage type "{storage_type}" for "generate_access_token" not yet supported')

    account_key, _ = get_credentials(conn)
    full_uri = safe_uri(conn, "")
    account_name, _, _ = parse_wasb_url(full_uri)
    return generate_container_sas(
        account_name,
        container_name,
        account_key,
        permission=ContainerSasPermissions(read=True, write=True, delete=True, list=True),
        expiry=expiry or datetime.now() + timedelta(days=365),
        start=datetime.now()
    )


def move_file(src_key: str, dest_key: str, src_conn, dest_conn=None, delete_src_key=False):
    """
    Moves a file from src_key to dest_key
    params:
        src_key: source key
        dest_key: destination key
        src_conn: source connection
        dest_conn: destination connection (set to src_conn if not provided)
        delete_src_key: delete the source key after moving
    """
    src_conn = safe_conn(src_conn)
    if dest_conn is None:
        dest_conn = src_conn
    else:
        dest_conn = safe_conn(dest_conn)

    src_full_uri = safe_uri(src_conn, src_key)
    dest_full_uri = safe_uri(dest_conn, dest_key)

    _, src_container_name, src_real_key = parse_wasb_url(src_full_uri)
    src_storage_client = get_storage_client(src_conn)
    src_container_client = src_storage_client.get_container_client(src_container_name)
    src_blob_client = src_container_client.get_blob_client(src_real_key)

    _, dest_container_name, dest_real_key = parse_wasb_url(dest_full_uri)
    dest_storage_client = get_storage_client(dest_conn)
    dest_container_client = dest_storage_client.get_container_client(dest_container_name)
    dest_blob_client = dest_container_client.get_blob_client(dest_real_key)

    # Start the copy operation
    dest_blob_client.start_copy_from_url(src_blob_client.url)
    # Wait for the copy to complete
    properties = dest_blob_client.get_blob_properties()
    while properties.copy.status == "pending":
        properties = dest_blob_client.get_blob_properties()
    # Check if the copy was successful
    if properties.copy.status != "success":
        raise Exception(f"Failed to copy blob from {src_key} to {dest_key}: {properties.copy.status}")

    # Delete the source blob
    if delete_src_key:
        src_blob_client.delete_blob()


def rename_directory(conn, src_key, dest_key):
    """
    new version to rename a directory (only supports azure right now)
    """
    conn = safe_conn(conn)
    storage_type = get_storage_client_type(conn)
    if storage_type != 'azure':
        raise Exception(f'Storage type "{storage_type}" for "rename_directory" not yet supported')

    src_full_uri = safe_uri(conn, src_key)
    dest_full_uri = safe_uri(conn, dest_key)

    # get the container name using the standard wasbs safe uri
    (storage_account, container_name, source_directory) = parse_wasb_url(src_full_uri)
    (_, _, destination_directory) = parse_wasb_url(dest_full_uri)

    account_url = f'https://{storage_account}.dfs.core.windows.net'
    credential, _ = get_credentials(conn)

    service_client = DataLakeServiceClient(account_url=account_url, credential=credential, api_version='2024-05-04')
    file_system_client = service_client.get_file_system_client(container_name)
    source_directory_client = file_system_client.get_directory_client(source_directory)
    destination_path = f"{container_name}/{destination_directory}"
    source_directory_client.rename_directory(destination_path)


def delete_directory(conn, key):
    """
    new version to delete a director (only supports azure right now)
    """
    conn = safe_conn(conn)
    storage_type = get_storage_client_type(conn)
    if storage_type != 'azure':
        raise Exception(f'Storage type "{storage_type}" for "delete_directory" not yet supported')

    full_uri = safe_uri(conn, key)
    # get the container name using the standard wasbs safe uri
    (storage_account, container_name, directory_name) = parse_wasb_url(full_uri)
    account_url = f'https://{storage_account}.dfs.core.windows.net'
    credential, _ = get_credentials(conn)

    service_client = DataLakeServiceClient(account_url=account_url, credential=credential, api_version='2024-05-04')
    file_system_client = service_client.get_file_system_client(container_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    directory_client.delete_directory()


def create_directory(conn, key):
    """
    new version to create a directory (only supports azure right now)
    """
    conn = safe_conn(conn)
    storage_type = get_storage_client_type(conn)
    if storage_type != 'azure':
        raise Exception(f'Storage type "{storage_type}" for "create_directory" not yet supported')

    full_uri = safe_uri(conn, key)
    # get the container name using the standard wasbs safe uri
    (storage_account, container_name, directory_name) = parse_wasb_url(full_uri)
    account_url = f'https://{storage_account}.dfs.core.windows.net'
    credential, _ = get_credentials(conn)

    service_client = DataLakeServiceClient(account_url=account_url, credential=credential, api_version='2024-05-04')
    file_system_client = service_client.get_file_system_client(container_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    directory_client.create_directory()
