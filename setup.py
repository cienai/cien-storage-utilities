from setuptools import setup

setup(
    name='storage_helper',
    version='0.1.0',
    description='Cloud storage helper for AWS, Azure and Google Cloud Storage',
    url='https://github.com/cienai/cien-storage-utilities',
    author='cien',
    author_email='dev@cien.ai',
    packages=[
        'storage_helper'
    ],
    install_requires=[
        "boto3",
        "azure.storage.blob",
        "azure-storage-file-datalake"],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
