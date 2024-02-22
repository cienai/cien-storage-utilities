import boto3
from azure.storage.blob import BlobServiceClient

from urllib.parse import urlparse
import json
from typing import Union
import time
import os

"""
Connections

The following environment variables are required to be set in the environment where the code is running:
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

def parse_cloud_storage_uri(uri: str) -> (str, str, str):
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

def get_storage_client(conn: Union[str, dict]):
    """
    Returns an S3 client object
    """
    conn = safe_conn(conn)
    uri = conn['BUCKET_URI']
    _, _, scheme = parse_cloud_storage_uri(uri)

    if scheme == 's3':        
        aws_access_key_id = conn['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = conn['AWS_SECRET_ACCESS_KEY']
        return boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    if scheme == 'gs':
        raise Exception('Google Cloud Storage not supported')
    if scheme == 'wasbs':
        account_name = conn['AZURE_STORAGE_ACCOUNT']
        account_key = conn['AZURE_STORAGE_ACCESS_KEY']
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

def is_json_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .json
    """
    return file_name.lower().endswith('.json') or file_name.lower().endswith('.json.gz')


def is_csv_file(file_name: str) -> bool:
    """
    Returns True if the file name ends with .csv
    """
    return file_name.lower().endswith('.csv') or file_name.lower().endswith('.csv.gz')

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

def list_files(conn: Union[str, dict], prefix: str, return_details: bool=False):
    """
    Returns a list of files in bucket matching the prefix
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)
    
    try:        
        # get the storage type (one of azure, aws, google)
        storage_type = get_storage_client_type(conn)
        # append the prefix to the bucket_uri
        full_uri = safe_uri(conn, prefix)      
        # parse out the bucket and the new prefix  
        bucket, real_prefix, _ = parse_cloud_storage_uri(full_uri)
        
        # handle the aws case
        if storage_type == 'aws':            
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
            parts = bucket.split("@")
            container_name = parts[0]
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
    except Exception as e:
        print(f"[storage_helper] Error listing files in '{bucket}/{prefix}': {str(e)}")
        matching_files = []
    finally:
        storage_client = None

    # clean out any prefix that exists in the bucket_uri
    bucket, replace_prefix, _ = parse_cloud_storage_uri(conn["BUCKET_URI"])
    res = []
    for file in matching_files:
        # replace the file prefix
        file = file.replace(replace_prefix, "")
        if file.startswith("/"):
            file = file[1:]
        res.append(file)
    return res


def write_file(conn: Union[str, dict], key: str, data) -> None:
    """
    Writes a file to an S3 bucket
    """
    storage_client = get_storage_client(conn)
    try:
        storage_type = get_storage_client_type(conn)
        full_uri = safe_uri(conn, key)      
        # parse out the bucket and the new prefix  
        bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
        
        # handle the aws case
        if storage_type == 'aws':      
            storage_client.put_object(Bucket=bucket, Key=real_key, Body=data)
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
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
        # parse out the bucket and the new prefix  
        bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
        
        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':
            response = storage_client.get_object(Bucket=bucket, Key=real_key)
            body = response['Body']
            data = body.read()
            # convert to string if it's a json file
            if is_json_file(key) or is_csv_file(key) or is_txt_file(key):
                data = data.decode('utf-8')
            return data
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
            container_client = storage_client.get_container_client(container_name)            
            blob_client = container_client.get_blob_client(real_key)
            data = blob_client.download_blob().readall()
            if is_json_file(key) or is_csv_file(key) or is_txt_file(key):
                data = data.decode('utf-8')
            return data
        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')        
    except Exception as e:  # noqa
        print(f"[storage_client] Error reading file '{key}': {str(e)}")
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
        # parse out the bucket and the new prefix  
        bucket, real_key, _ = parse_cloud_storage_uri(full_uri)

        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':            
            storage_client.delete_object(Bucket=bucket, Key=real_key)
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
            container_client = storage_client.get_container_client(container_name)            
            blob_client = container_client.get_blob_client(real_key)
            blob_client.delete_blob()
        # handle the google case (not implemented yet)
        else:
            raise Exception('Unknown storage client')
    except Exception as e:  # noqa
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
        # parse out the bucket and the new prefix  
        bucket, real_folter_to_delete, _ = parse_cloud_storage_uri(full_uri)

        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':            
            objects_to_delete = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_folter_to_delete)
            if 'Contents' in objects_to_delete:
                for obj in objects_to_delete['Contents']:
                    storage_client.delete_object(Bucket=bucket, Key=(obj['Key']))
            storage_client.delete_object(Bucket=bucket, Key=real_folter_to_delete)
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
            container_client = storage_client.get_container_client(container_name)
            # List blobs with the specified prefix
            blob_list = container_client.walk_blobs(name_starts_with=real_folter_to_delete)

            # Delete each blob with the specified prefix            
            for blob in blob_list:
                container_client.get_blob_client(blob.name).delete_blob()
            # make sure the folder is also deleted
            folder_name = folder_to_delete.rstrip('/')
            container_client.get_blob_client(folder_name).delete_blob()
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
        # parse out the bucket and the new prefix  
        bucket, real_old_key, _ = parse_cloud_storage_uri(full_old_uri)
        bucket, real_new_key, _ = parse_cloud_storage_uri(full_new_uri)

        storage_type = get_storage_client_type(conn)
        # handle the aws case
        if storage_type == 'aws':            
            storage_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket,  'Key': real_old_key}, Key=real_new_key)
            delete_file(conn, old_file_key)
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
            container_client = storage_client.get_container_client(container_name)
            source_blob = container_client.get_blob_client(real_old_key)
            # container_client2 = storage_client.get_container_client(container_name)
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
    Renames a folder in an S3 bucket
    """
    try:
        conn = safe_conn(conn)
        if not old_folder_key.endswith('/'):
            old_folder_key = old_folder_key + '/'
        if not new_folder_key.endswith('/'):
            new_folder_key = new_folder_key + '/'     
        print(f"Renaming folder {old_folder_key} to {new_folder_key}")
        # delete the new folder if it exists
        try:
            delete_folder(conn, new_folder_key)
        except:
            pass

        storage_client = get_storage_client(conn)
        
        full_old_uri = safe_uri(conn, old_folder_key)
        full_new_uri = safe_uri(conn, new_folder_key)
        # parse out the bucket and the new prefix  
        bucket, real_old_key, _ = parse_cloud_storage_uri(full_old_uri)
        bucket, real_new_key, _ = parse_cloud_storage_uri(full_new_uri)

        storage_type = get_storage_client_type(conn)        
        # handle the aws case
        if storage_type == 'aws':            
            objects_to_copy = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_old_key)
            if 'Contents' in objects_to_copy:
                for obj in objects_to_copy['Contents']:
                    old_obj_key = obj['Key']
                    new_obj_key = old_obj_key.replace(real_old_key, real_new_key)
                    storage_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': old_obj_key}, Key=new_obj_key)
            delete_folder(conn, old_folder_key)
        # handle the azure case
        elif storage_type == 'azure':
            parts = bucket.split("@")
            container_name = parts[0]
            container_client = storage_client.get_container_client(container_name)
            # List blobs with the specified prefix
            blob_list = container_client.walk_blobs(name_starts_with=old_folder_key)
            for blob in blob_list:
                old_blob_key = blob.name
                new_blob_key = old_blob_key.replace(old_folder_key, real_new_key)
                source_blob = container_client.get_blob_client(old_blob_key)
                dest_blob = container_client.get_blob_client(new_blob_key)
                dest_blob.start_copy_from_url(source_blob.url)
                while dest_blob.get_blob_properties().copy.status == 'pending':
                    time.sleep(0.1)
            delete_folder(conn, old_folder_key)            
    except Exception as e:
        print(f"Error renaming folder '{old_folder_key}' to '{new_folder_key}': {str(e)}")
    finally:
        storage_client = None

def check_if_file_exists(conn: Union[str, dict], key: str) -> bool:
    """
    Checks if a file exists
    """
    storage_client = get_storage_client(conn)
    
    full_uri = safe_uri(conn, key)
    # parse out the bucket and the new prefix  
    bucket, real_key, _ = parse_cloud_storage_uri(full_uri)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':                        
        try:
            # Attempt to head the object (check if it exists)
            storage_client.head_object(Bucket=bucket, Key=real_key)
            return True  # The object exists
        except:
            return False
    # handle the azure case
    elif storage_type == 'azure':
        parts = bucket.split("@")
        container_name = parts[0]
        container_client = storage_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(real_key)
        return blob_client.exists() # The object exists
    return False
    
def copy_file_to_local(conn: Union[str, dict], key: str, local_file_path: str) -> None:
    """
    Copies a file from the cloud storage to the local file system
    """
    conn = safe_conn(conn)
    storage_client = get_storage_client(conn)

    full_uri = safe_uri(conn, key)
    # parse out the bucket and the new prefix  
    bucket, real_key, _ = parse_cloud_storage_uri(full_uri)
    print(f"Copying from bucket: {bucket}, key: {real_key} to: {local_file_path}...")

    # ensure that the path to local_file_path exists
    local_folder_path = os.path.dirname(local_file_path)
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':        
        storage_client.download_file(bucket, real_key, local_file_path)
    # handle the azure case
    elif storage_type == 'azure':
        parts = bucket.split("@")
        container_name = parts[0]
        container_client = storage_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(real_key)
        with open(local_file_path, "wb") as my_blob:
            blob_data = blob_client.download_blob()
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
    # parse out the bucket and the new prefix  
    bucket, real_key, _ = parse_cloud_storage_uri(full_uri)    

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':        
        storage_client.upload_file(local_file_path, bucket, real_key)
    # handle the azure case
    elif storage_type == 'azure':
        parts = bucket.split("@")
        container_name = parts[0]
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
    # parse out the bucket and the new prefix  
    bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)    

    # ensure that folder_key ends with a '/'
    if not real_folder_key.endswith('/'):
        real_folder_key = real_folder_key + '/'
    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'
    # ensure that local_folder_path exists
    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':        
        objects_to_copy = storage_client.list_objects_v2(Bucket=bucket, Prefix=real_folder_key)
        if 'Contents' in objects_to_copy:
            for obj in objects_to_copy['Contents']:
                obj_key = obj['Key']
                filename = os.path.basename(obj_key)
                local_file_path = f"{local_folder_path}/{filename}"
                storage_client.download_file(bucket, obj_key, local_file_path)
    # handle the azure case
    elif storage_type == 'azure':
        parts = bucket.split("@")
        container_name = parts[0]
        container_client = storage_client.get_container_client(container_name)
        blob_list = container_client.walk_blobs(name_starts_with=real_folder_key)
        for blob in blob_list:
            blob_key = blob.name
            filename = os.path.basename(blob_key)
            if len(filename) > 0:            
                local_file_path = f"{local_folder_path}/{filename}"
                blob_client = container_client.get_blob_client(blob_key)
                with open(local_file_path, "wb") as my_blob:
                    blob_data = blob_client.download_blob()
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
    # parse out the bucket and the new prefix  
    bucket, real_folder_key, _ = parse_cloud_storage_uri(full_uri)    

    # ensure that folder_key ends with a '/'
    if not real_folder_key.endswith('/'):
        real_folder_key = real_folder_key + '/'
    # ensure that local_folder_path ends with a '/'
    if not local_folder_path.endswith('/'):
        local_folder_path = local_folder_path + '/'

    storage_type = get_storage_client_type(conn)
    # handle the aws case
    if storage_type == 'aws':        
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                key = f"{real_folder_key}{file}"
                storage_client.upload_file(local_file_path, bucket, key)
    # handle the azure case
    elif storage_type == 'azure':
        parts = bucket.split("@")
        container_name = parts[0]
        container_client = storage_client.get_container_client(container_name)
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                if len(file) > 0:
                    local_file_path = os.path.join(root, file)
                    key = f"{real_folder_key}{file}"
                    print(f"file: {file}, key: {key}")
                    blob_client = container_client.get_blob_client(key)
                    with open(local_file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
    # handle the google case (not implemented yet)
    else:
        raise Exception('Unknown storage client')