import streamlit as st
import json
import pandas as pd

def list_files_s3(bucket:str, path:str):
    """List files in S3"""
    import boto3
    s3 = boto3.client('s3',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name='us-east-1')

    response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
    files = [f["Key"] for f in response["Contents"]]
    return files

def load_csv_s3(bucket:str, path:str, filename:str):
    """Load csv file from S3"""
    import io, boto3
    s3 = boto3.client('s3',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name='us-east-1')

    obj = s3.get_object(Bucket=bucket, Key=path+filename)

    if len(obj) != 0:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep="|")
    else:
        df = pd.DataFrame()
    return df

def load_json_s3(bucket:str, filename:str, path:str):
    """Load file from S3"""
    import io, boto3
    s3 = boto3.client('s3',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name='us-east-1')

    obj = s3.get_object(Bucket=bucket, Key=path+filename)

    if len(obj) != 0:
        j = json.load(io.BytesIO(obj['Body'].read()))
    else:
        j = {}
    return j

def load_event_raw(filename, path):

    return load_json_s3(bucket="prod-track-sources-s3stack-dumpbucketbe480749-wch2mlfw0oh0", filename=filename, path=path)