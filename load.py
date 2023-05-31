import os
import streamlit as st
import json
import pandas as pd
from tools.tools import load_csv_s3
from datetime import datetime

def create_warehouse_engine(env):
    from sqlalchemy import create_engine
    if env == "prod":
        conn_bd = f"postgresql://{st.secrets['wh_username']}:{st.secrets['wh_password']}@{st.secrets['wh_host']}:{st.secrets['wh_port']}/{st.secrets['wh_db']}"
    elif env == "staging":
        conn_bd = f"postgresql://{st.secrets['wh_username']}:{st.secrets['wh_password']}@{st.secrets['wh_host']}:{st.secrets['wh_port']}/{st.secrets['wh_db']}"
    return create_engine(conn_bd)

def create_warehouse_vecna_engine(env):
    from sqlalchemy import create_engine
    conn_bd = f"postgresql://{st.secrets['wh_vecna_username']}:{st.secrets['wh_vecna_password']}@{st.secrets['wh_vecna_host']}:{st.secrets['wh_vecna_port']}/{st.secrets['wh_vecna_db']}"
    return create_engine(conn_bd)

def create_prisma_engine():
    from sqlalchemy import create_engine
    conn_bd = f"postgresql://{st.secrets['prisma_username']}:{st.secrets['prisma_password']}@{st.secrets['prisma_host']}:{st.secrets['prisma_port']}/{st.secrets['prisma_database']}"
    return create_engine(conn_bd)

@st.cache_data
def load_subscriptions(env) -> pd.DataFrame:
    """Load subscriptions from Warehouse Vecna"""
    
    warehouse_engine = create_warehouse_vecna_engine(env)
    
    if env == "prod":
        schema = "public"
        subscriptions_table = "prod_vecna_subscription"
    elif env == "staging":
        schema = "staging"
        subscriptions_table = "dev_vecna_subscription"

    # All Vecna subscriptions
    query = f'''
with subs as (
    select
        *
        ,row_number() over (partition by "subscription_doc" order by "subscription_created_at" desc) as row_number
    from
    (
        select
            *
            ,case
                when "subscription_bl" != 'NOT AVAILABLE' then "subscription_bl"
                when "subscription_container" != 'NOT AVAILABLE' then "subscription_container"
                when "subscription_booking" != 'NOT AVAILABLE' then "subscription_booking"
            end as "subscription_doc"
        from
            {schema}.{subscriptions_table}
    )
)
select
    *
from
    subs
where
    --row_number = 1 and
    subscription_created_at >= '2023-01-14';
    '''
    subscriptions = pd.read_sql_query(query, warehouse_engine)
    return subscriptions

@st.cache_data
def load_events(env) -> pd.DataFrame:
    """Load events from Warehouse Vecna"""
    
    warehouse_engine = create_warehouse_vecna_engine(env)
    
    if env == "prod":
        schema = "public"
        events_table = "prod_vecna_event_consolidated"
    elif env == "staging":
        schema = "staging"
        events_table = "dev_vecna_event_consolidated"

    # All Vecna events
    query = f'''
select
    "vecna_event_id"
    ,"vecna_event_container"
    ,"subscription_id"
    ,"subscription_type"
    ,"subscription_bl"
    ,"subscription_booking"
    ,"subscription_container"
    ,case
        when "subscription_bl" != 'NOT AVAILABLE' then "subscription_bl"
        when "subscription_container" != 'NOT AVAILABLE' then "subscription_container"
        when "subscription_booking" != 'NOT AVAILABLE' then "subscription_booking"
    end as "subscription_doc"
    ,"subscription_carrier_code"
    ,"vecna_event_created_at"
    ,"raw_event_gh"
    ,"raw_event_oi"
from
    {schema}.{events_table}
where "vecna_event_created_at" >= '2023-03-14'
    '''
    events = pd.read_sql_query(query, warehouse_engine)
    return events

@st.cache_data
def load_containers_by_subscription(env) -> pd.DataFrame:
    """Load events from Warehouse Vecna"""
    
    warehouse_engine = create_warehouse_vecna_engine(env)
    
    if env == "prod":
        schema = "public"
        events_table = "prod_vecna_event_consolidated"
    elif env == "staging":
        schema = "staging"
        events_table = "dev_vecna_event_consolidated"

    # All Vecna events
    query = f'''
select distinct
    "subscription_id"
    ,"vecna_event_container"
from
    {schema}.{events_table}
where "vecna_event_created_at" >= '2023-01-01'
    '''
    containers_by_subscription = pd.read_sql_query(query, warehouse_engine)
    return containers_by_subscription


@st.cache_data
def load_shipments_prisma(subscriptionId) -> pd.DataFrame:
    prisma_engine = create_prisma_engine()

    query = f'''
select
    *
from
    "public"."vw_arauco_wide"
where
    "subscriptionId" = '{subscriptionId}'
    '''
    return pd.read_sql_query(query, prisma_engine)

@st.cache_data
def load_events_vecna(doctype, doc, env):
    if doctype == "mbl":
        where = f"where \"subscription_bl\" = '{doc}'"
    elif doctype == "container":
        where = f"where \"subscription_container\" = '{doc}'"
    elif doctype == "booking":
        where = f"where \"subscription_booking\" = '{doc}'"

    warehouse_engine = create_warehouse_engine()
    query = f'''
        select
            *
        from
            "staging"."stg_vecna_event_consolidated_2"
        {where}
        '''
    events = pd.read_sql_query(query, warehouse_engine)
    return events

@st.cache_data
def load_events_prisma(container, _date):
    prisma_engine = create_prisma_engine()
    query = f'''
select
    "a"."body"
    ,"a"."container_number"
    ,"a"."createdAt"
from 
(
    select
        "body"::json as "body"
        ,"body"::json -> 'shipment' ->> 'container_number' as "container_number"
        ,"createdAt" as "createdAt"
    from "OiEvent"
    where "createdAt" >= '{_date}'
    order by "createdAt" desc
) as "a"
where
    "a"."container_number" = '{container}'
order by
    "a"."createdAt" desc
'''
    events = pd.read_sql_query(query, prisma_engine)
    return events

@st.cache_data
def load_event_vecna(env, vecna_event_id, subscription_id=None, event_created_at=None, event_container=None):

    if env == "prod":
        schema = "public"
        event_table = "prod_vecna_event_consolidated"
    elif env == "staging":
        schema = "staging"
        event_table = "dev_vecna_event_consolidated"

    warehouse_engine = create_warehouse_vecna_engine(env)
    query = f'''
        select
            *
        from
            {schema}.{event_table}
        where
            "vecna_event_id" = '{vecna_event_id}'
        '''
            #("vecna_event_id" = '{vecna_event_id}' or "vecna_event_id" is null)
            #and "subscription_id" = '{subscription_id}'
            #and "vecna_event_created_at" = '{event_created_at}'
            #and "vecna_event_container" = '{event_container}'
        #'''
    event = pd.read_sql_query(query, warehouse_engine)
    return event

def load_event_raw(filename, path):
    import io, boto3
    bucket = "prod-track-sources-s3stack-dumpbucketbe480749-wch2mlfw0oh0"
    
    s3 = boto3.client('s3',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name='us-east-1')

    obj = s3.get_object(Bucket=bucket, Key=path+filename)

    if len(obj) != 0:
        j = json.load(io.BytesIO(obj['Body'].read()))
        #j = obj['Body'].read().decode('utf-8') 
    else:
        j = {}
    return j

def load_event_vecna_back(subscription_id, env):
    import requests

    if env == "prod":
        endpoint = f"https://vecna.klog.co/subscriptions/{subscription_id}"
        token = "Y2xkYWl4a3R0MDAwZDM3NnF0YW1qd2U5bjp4ZGM3ZDN0aDNnWWV0ZUMwVXViM29m="
    elif env == "staging":
        endpoint = f"https://staging.vecna.klog.co/subscriptions/{subscription_id}"
        token = "Y2w4NHVhNzU5MDAwMTA5bDNnYTVjOXl1dDpETVNLQURLTUxTUUE="

    response = requests.get(
        endpoint,
        headers={'Authorization': f'Token {token}'}
    )
    return response.text

def load_event_dynamo(subscription_id, env):
    import boto3

    if env == "prod":
        endpoint = f"prod-vecna-Subscription"
    elif env == "staging":
        endpoint = f"staging-vecna-Subscription"

    dynamo_client = boto3.client('dynamodb',
                        aws_access_key_id=st.secrets["aws_access_key_id"],
                        aws_secret_access_key=st.secrets["aws_secret_access_key"],
                        region_name='us-east-1')
    response = dynamo_client.query(
        TableName=f'{endpoint}',
        KeyConditionExpression='id = :id',
        ExpressionAttributeValues={
            ':id': {"S": subscription_id}
        }
    )
    return response

@st.cache_data
def load_data_quality(client="Arauco") -> pd.DataFrame:
    if client == "Arauco":
        data = pd.read_csv(f"https://klog.metabaseapp.com/public/question/c2c3c38d-e8e6-4482-ab6c-33e3f9317cce.csv")
    return data

@st.cache_data
def load_data_quality_historic(date, client="Arauco") -> pd.DataFrame:
    if client == "Arauco":
        data = load_csv_s3("klog-lake","raw/arauco_snapshots/",f"{date.strftime('%Y%m%d')}-arauco_snapshot.csv")
    return data

@st.cache_data
def load_itinerarios():
    # Read all json files from data directory
    data_dir = "data/"
    files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f)) and f.endswith(".json")]
    data = []
    for file in files:
        # Read JSON
        with open(data_dir+file) as f:
            d = json.load(f)
            data += [d]
    results = [i for sublist in [d["results"] for d in data] for i in sublist]
    return pd.DataFrame(results)