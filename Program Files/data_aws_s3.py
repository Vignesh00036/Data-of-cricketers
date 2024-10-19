import os
import boto3
# import subprocess

#before this process you should configure aws in your system
s3=boto3.client('s3')
files =['players_information','odi','test','t20','ipl']

# this function is going to upload all files including folders into s3 bucket
def upload(team_name):
    replaced_team_name=team_name.replace(' ', '_')
    path={your_file_path}
    if os.path.exists(path):
        for idx,i in enumerate(os.listdir(path)):
            file_path={your_file_path/files[idx].csv}
            bucket_name={s3_bucket_name}
            s3_path={your_s3_path}
            s3.upload_file(file_path, bucket_name, s3_path)

# Method -2
# uploading files using aws cli
# def uploading_to_s3(team_name):
#     for file in files:
#         upload_statement=[
#             '/snap/bin/aws', 's3', 'cp', f'/media/beast/Beast/DE/Python_programms/1_OG/data/{team_name}', 
#             f's3://s3-bucket-for-data/{file}_records/', '--recursive', '--exclude', '*', '--include', f'{file}_records.csv'
#         ]
#         subprocess.run(upload_statement, stdout=subprocess.DEVNULL)
