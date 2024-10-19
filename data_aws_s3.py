import os
import boto3
# import subprocess

s3=boto3.client('s3')
files =['players_information','odi','test','t20','ipl']

def upload(team_name):
    replaced_team_name=team_name.replace(' ', '_')
    path=f'/media/beast/Beast/DE/Python_programms/1_OG/data/{replaced_team_name}'
    if os.path.exists(path):
        for idx,i in enumerate(os.listdir(path)):
            file_path=f'/media/beast/Beast/DE/Python_programms/1_OG/data/{replaced_team_name}/{files[idx]}_records.csv'
            bucket_name='s3-bucket-for-data'
            s3_path=f'{files[idx]}_records/{files[idx]}_{team_name}.csv'
            s3.upload_file(file_path, bucket_name, s3_path)

# def uploading_to_s3(team_name):
#     for file in files:
#         upload_statement=[
#             '/snap/bin/aws', 's3', 'cp', f'/media/beast/Beast/DE/Python_programms/1_OG/data/{team_name}', 
#             f's3://s3-bucket-for-data/{file}_records/', '--recursive', '--exclude', '*', '--include', f'{file}_records.csv'
#         ]
#         subprocess.run(upload_statement, stdout=subprocess.DEVNULL)