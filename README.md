# cien-storage-utilities

`cien-storage-utilities` is a Python package that provides helper functions to handle operations with AWS S3, Azure Blobs, and GCP Cloud Storage (GS). This package aims to simplify common tasks related to interacting with object storage services across different cloud providers.

Note: GS is currently not supported

## Installation

You can install `cien-storage-utilities` via pip:

```bash
pip install git+https://github.com/cienai/cien-storage-utilities.git
```

## Usage
```python
import storage_helper

# define a connection (see section on Connectoins for more information)
conn = {
    "BUCKET_URI": f"wasbs://{azure_storage_account}.blob.core.windows.net/{container_name}/{prefix}",
    "AZURE_STORAGE_ACCESS_KEY": azure_access_key
}

# another format for was strings are as follows
conn = {
    "BUCKET_URI": f"wasbs://{container_name}@{azure_storage_account}.blob.core.windows.net/{prefix}",
    "AZURE_STORAGE_ACCESS_KEY": azure_access_key
}

bucket, prefix, scheme = storage_helper.parse_cloud_storage_uri(conn["BUCKET_URI"])
print(f'bucket={bucket}, prefx={prefix}, scheme={scheme}')
storage_type = storage_helper.get_storage_client_type(conn)
print(f'storage_type={storage_type}')

# write a file
key = f'{prefix}/test.txt'
storage_helper.write_file(conn, key, 'this is a test')

# read a file
read_data = storage_helper.read_file(conn, key)
print(read_data)

# list files
files = storage_helper.list_files(conn, bucket, prefix)
for x in files:
    print(x)

# delete a file
storage_helper.delete_file(conn, key)

# rename a file
storage_helper.rename_file(conn, key, new_key)

# rename a folder
storage_helper.rename_folder(conn, key, new_key)

# copy from cloud to local
storage_helper.copy_file_to_local(conn, key, local_file_path)

# copy from local to cloud
storage_helper.copy_file_from_local(conn, local_file_path, key)

# copy to cloud folder from local
storage_helper.copy_folder_from_local(conn, local_folder_path, key)

# copy from cloud folder to local folder
storage_helper.copy_folder_to_local(conn, key, local_folder_path)

```
## Connection Strings
All methods require a connection string parameter.  Connections can be defined as dict or a json string.

### AWS S3 Example:
```python
{
    "BUCKET_URI": f's3://{bucket}/{prefix}',
    "AWS_ACCESS_KEY_ID": aws_access_key_id,
    "AWS_SECRET_ACCESS_KEY": aws_secret_access_key
}
```

### Azure Example:
```python
{
    "BUCKET_URI": f"wasbs://{container_name}@{azure_storage_account}.blob.core.windows.net/{prefix}",
    "AZURE_STORAGE_ACCOUNT": azure_storage_account,
    "AZURE_STORAGE_ACCESS_KEY": azure_access_key
}
```