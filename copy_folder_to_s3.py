import os
import boto3

def copy_folder_to_s3(local_folder_path, bucket_name, s3_folder_key):
    """Copy a local folder and its contents to an S3 bucket."""
    s3_client = boto3.client("s3")

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            s3_file_key = os.path.join(s3_folder_key, relative_path)
            s3_client.upload_file(local_file_path, bucket_name, s3_file_key)

if __name__ == "__main__":
    local_folder_path = "./workspace_example"  # Replace with the path to your local workspace_example folder
    bucket_name = "serrademo"  # Replace with the name of your S3 bucket
    s3_folder_key = "workspace_example"  # Replace with the desired destination folder in the bucket

    copy_folder_to_s3(local_folder_path, bucket_name, s3_folder_key)