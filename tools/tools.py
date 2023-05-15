import streamlit as st
import json
import pandas as pd
from st_aggrid import GridOptionsBuilder

# Config

def agrid_options(dataframe, page_size):
    grid_options_builder = GridOptionsBuilder.from_dataframe(dataframe)
    grid_options_builder.configure_pagination(enabled=True, paginationPageSize=page_size, paginationAutoPageSize=False)
    grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
    grid_options_builder.configure_grid_options(domLayout='normal')
    grid_options_builder.configure_selection("single")
    #grid_options_builder.configure_column("Entrega",
    #                    headerName="Entrega",
    #                    cellRenderer=JsCode('''function(params) {return '<a href="https://www.google.com">${params.value}</a>'}'''))
    return grid_options_builder.build()

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

def delete_page(main_script_path_str, page_name):
    from streamlit.source_util import (
        page_icon_and_name, 
        calc_md5, 
        get_pages,
        _on_pages_changed
    )
    current_pages = get_pages(main_script_path_str)

    for key, value in current_pages.items():
        if value['page_name'] == page_name:
            del current_pages[key]
            break
        else:
            pass
    _on_pages_changed.send()

def setup_ambient(ambient:str):
    if ambient == "Arauco":
        delete_page("Main.py","Vecna_explorer")