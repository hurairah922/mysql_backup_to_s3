#!/usr/bin/env python3

import os
import subprocess
import boto3
import logging
from datetime import datetime, timedelta
import zipfile
import json
from dotenv import load_dotenv

load_dotenv()


# Set up logging
logging.basicConfig(
    filename=str(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "backup.log")
    ),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# AWS and S3 details
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_BACKUP_FOLDER = os.getenv("AWS_S3_BACKUP_FOLDER")
AWS_REGION = os.getenv("AWS_REGION")

# Docker container details
PATH_TO_SQL_DUMPS = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.getenv("PATH_TO_SQL_DUMPS")
)

# Database details
DATABASES = json.loads(os.getenv("DATABASES"))
MYSQL_CNF_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.getenv("MYSQL_CNF_PATH")
)
LOCAL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


# S3 connection
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)


# Function to run MySQL dump command
def dump_database(db_name):
    try:
        backup_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            PATH_TO_SQL_DUMPS,
            f"{db_name}_{datetime.now().strftime(LOCAL_DATE_FORMAT)}.sql",
        )
        # Adding credentials file for the dump command
        # Executing the dump command
        subprocess.run(
            f"mysqldump --defaults-extra-file={MYSQL_CNF_PATH} {db_name} > {backup_file}",
            shell=True,
            check=True,
        )

        # Compress the backup file
        compressed_file = backup_file + ".zip"
        with zipfile.ZipFile(compressed_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(backup_file, os.path.basename(backup_file))

        # Remove uncompressed SQL file
        os.remove(backup_file)

        logging.info(f"Backup and compression successful for database: {db_name}")
        return compressed_file
    except Exception as e:
        logging.error(f"Error backing up database {db_name}: {str(e)}")
        return None


# Upload to S3
def upload_to_s3(file_path):
    file_name = os.path.basename(file_path).__str__()
    try:
        s3_path = os.path.join(AWS_S3_BACKUP_FOLDER, file_name)
        s3_client.upload_file(file_path, AWS_S3_BUCKET_NAME, s3_path)
        logging.info(f"Uploaded {file_name} to S3 bucket {AWS_S3_BUCKET_NAME}")
        os.remove(file_path)  # Remove the local compressed file after upload
    except Exception as e:
        logging.error(f"Error uploading {file_name} to S3: {str(e)}")


# Delete old backups from S3
def delete_old_backups():
    try:
        retention_date = datetime.now() - timedelta(days=14)
        backups_to_delete = []

        # List backups in the S3 bucket
        response = s3_client.list_objects_v2(
            Bucket=AWS_S3_BUCKET_NAME, Prefix=AWS_S3_BACKUP_FOLDER
        )
        if "Contents" in response:
            for obj in response["Contents"]:
                # Extract the date from the backup filename
                try:
                    backup_date_str = obj["Key"].split("_")[-1].replace(".sql.zip", "")
                    backup_date = datetime.strptime(backup_date_str, LOCAL_DATE_FORMAT)
                except Exception as e:
                    logging.info(f"Error getting backup date: {str(e)}")
                    backup_date = obj.get("LastModified").replace(tzinfo=None)

                if backup_date < retention_date:
                    backups_to_delete.append(obj["Key"])

        # Delete the old backups
        for backup in backups_to_delete:
            s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=backup)
            logging.info(f"Deleted old backup: {backup} from S3")
    except Exception as e:
        logging.error(f"Error deleting old backups from S3: {str(e)}")


# Main backup function
def perform_backup():
    backups_generated = []
    # Loop through the databases and back them up
    for db in DATABASES:
        backup_file = dump_database(db)
        if backup_file:
            upload_to_s3(backup_file)
            backups_generated.append(backup_file)

    # If backups are successfully generated, delete old backups
    if backups_generated:
        delete_old_backups()
    else:
        logging.warning("No backups generated. Skipping deletion of old backups.")


# Entry point
if __name__ == "__main__":
    perform_backup()
