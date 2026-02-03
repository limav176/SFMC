import os
import zipfile
import boto3
from pyspark.sql import SparkSession
from datetime import date

LOCAL_FILES='./'
MANUAL_DATE = date.today().strftime("%d-%m-%Y")
BOTO3_BUCKET =  "ftp-will-salesforce-prod"
RAW_BUCKET = "data-raw-zone-will-prod"
S3 = boto3.client("s3")


def process_zip_files(file_name, bucket_key):
    """Unzip files in local folder and uploads them to bucket"""
    with open(file_name, "rb") as zipscr:
        zfile = zipfile.ZipFile(zipscr)
        for member in zfile.infolist():
            if member.filename.endswith('.csv'):
                member.filename = f'{file_name[:-4]}.csv'
                with zfile.open(member) as infile:
                    print(f"Uploading {member.filename} to raw_bucket")
                    S3.upload_fileobj(infile, RAW_BUCKET, bucket_key)
                    print(f'Finished uploading {bucket_key} to raw_bucket')

def process_txt_files(file_name, bucket_key):
    """Renames and upload files ot bucket"""
    print(f"Uploading {file_name} to raw_bucket")
    with open(file_name, 'rb') as infile:
        S3.upload_fileobj(infile, RAW_BUCKET, bucket_key)
        print(f'Finished uploading {bucket_key} to raw_bucket')

def delete_zip_files(local_path):
    """deletes zipfiles in tmp"""
    print('Deleting zip files...')
    files=os.listdir(local_path)
    for file in files:
        if file.endswith('.zip'):
            os.remove(str(os.path.join(local_path, file)))
    print('Finish deleting zip files.')

def delete_txt_files(local_path):
    """deletes csv files in tmp"""
    print('Deleting txt files...')
    files=os.listdir(local_path)
    for file in files: 
        if file.endswith('.csv'):
            os.remove(str(os.path.join(local_path, file)))
    print('Finished deleting txt files.')

def sort_files_into_raw():
    """Fetch files from source_ftp and sort them into proper buckets in raw zone"""

    today_date = MANUAL_DATE
    list_obj = []

    for content in S3.list_objects(Bucket= BOTO3_BUCKET)['Contents']:
        last_modified = content['LastModified'].strftime("%d-%m-%Y")
        if last_modified == today_date:
            list_obj.append(content['Key'])
    
    print("Downloading files...")
    for file_name in list_obj:
        local_file_name = LOCAL_FILES+file_name
        if file_name[-4:] == ".zip":
            subfolder = file_name.split('_')[0]
            key = file_name[:-4] + '.csv'
            file_key = f'salesforce/{subfolder}/{today_date}/{key}'
        else:
            subfolder = file_name.split('_')[1]
            subfolder = subfolder.replace('.csv','')
            key = file_name
            file_key = f'salesforce/{subfolder}/{today_date}/{key}'
        
        print("Downloading " + file_name)
        S3.download_file(BOTO3_BUCKET, file_name, local_file_name)

        if file_name[-3:] == 'zip':
            process_zip_files(local_file_name, file_key)
        else:
            process_txt_files(local_file_name, file_key)

        print(f'Cleaning area..')
        delete_zip_files(LOCAL_FILES)
        delete_txt_files(LOCAL_FILES)
    print("Finished processing files")

    spark = SparkSession.builder.appName("sort_files_into_raw").getOrCreate()

if __name__ == "__main__":
    sort_files_into_raw()
