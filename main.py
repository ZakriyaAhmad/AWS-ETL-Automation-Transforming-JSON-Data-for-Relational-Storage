import json
import hashlib
import os
import shutil
import zipfile
from botocore.exceptions import NoCredentialsError
import boto3
from Meta_Data_Ingstion import meta_data_func
from Batch_Data_Ingestion import batch_data
import psycopg2
from psycopg2 import sql
import uuid
from TargetCompounds import target_compounds
from Target_Qualifier import target_qualifiers
from peaks import peaks_ingestion
from PeakQualifiers import peaksqualifiers_ingestion
from Calibration import calibration_qualifiers
import paramiko
import psycopg2
import sshtunnel
from conn_db import main



source_directory = 'sample_files'

# Get the list of files in the source directory
files = os.listdir(source_directory)

# Loop through each file in the directory
for file_name in files:
    if file_name.endswith('.zip'):  # Check if it's a zip file
        unique_id = uuid.uuid4()
        zip_file_path = os.path.join(source_directory, file_name)
        # Create destination directory with the same name as the zip file
        destination_directory = os.path.join(os.path.dirname(source_directory), os.path.splitext(file_name)[0])
        os.makedirs(destination_directory, exist_ok=True)
        print(destination_directory)
        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_directory)
        print(f"---Unique ID of {file_name} is {unique_id}")
        # Open and read the JSON files
        json_file_path = os.path.join(destination_directory, f"{os.path.splitext(file_name)[0]}.json")
        metadata_file_path = os.path.join(destination_directory, 'metadata.json')
        conn = main() 
        if os.path.exists(metadata_file_path):
            with open(metadata_file_path, 'r') as metadata_file:
                metadata_data = json.load(metadata_file)
                meta_data_func(metadata_data,'path','33715041554E524536567677466170376377386C4F413D3D',conn,unique_id)

        else:
            print(f"No metadata.json file found in {destination_directory}")
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as json_file:
                json_data = json.load(json_file)
                batch_data(json_data, unique_id,conn)
                target_compounds(json_data, unique_id,conn)
                target_qualifiers(json_data, unique_id,conn)
                peaks_ingestion(json_data, unique_id,conn)
                peaksqualifiers_ingestion(json_data, unique_id,conn)
                calibration_qualifiers(json_data, unique_id,conn)
                print(f"Content of {os.path.basename(json_file_path)}:")
        else:
            print(f"No JSON file found in {destination_directory}")
        try:
            shutil.rmtree(destination_directory)
        except Exception as e:
            print('exception at del folder',destination_directory)
        print(f"Removed ---------- {destination_directory} ------------")
