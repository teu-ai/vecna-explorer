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
        events_table = "stg_vecna_event_consolidated" #"dev_vecna_event_consolidated"

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
        events_table = "stg_vecna_event_consolidated" #"dev_vecna_event_consolidated"

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

def load_events_vecna(doctype, doc, env):

    if doctype == "mbl":
        where = f"where \"subscription_bl\" = '{doc}'"
    elif doctype == "container":
        where = f"where \"subscription_container\" = '{doc}'"
    elif doctype == "booking":
        where = f"where \"subscription_booking\" = '{doc}'"

    warehouse_engine = create_warehouse_vecna_engine(env)
    query = f'''
        select
            *
        from
            "public"."prod_vecna_event_consolidated"
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
        event_table = "stg_vecna_event_consolidated" #"dev_vecna_event_consolidated"

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


data_quality_columns = {
        'entrega': 'Entrega',
        'estado': 'Estado',
        'mbl': 'MBL',
        'contenedor': 'Contenedor',
        'cliente': 'Cliente',
        'naviera': 'Naviera',
        'pol': 'POL',
        'pod': 'POd',
        'nave': 'Nave',
        'viaje': 'Viaje',
        'envío de datos': 'Envío de datos',
        'shipment_id': 'Shipment_id',
        'fecha_creacion_embarque': 'Fecha_Creacion_Embarque',
        'subscriptionid': 'subscriptionId',
        'booking confirmado': 'Booking confirmado',
        'carga lista estimada (sch)': 'Carga lista estimada (Sch)',
        'carga lista actual (sch)': 'Carga lista actual (Sch)',
        'fecha arribo a bodega': 'Fecha arribo a bodega',
        'retiro actual (sch)': 'Retiro actual (Sch)',
        'retiro estimado (sch)': 'Retiro estimado (Sch)',
        'pol llegada': 'POL Llegada',
        'pol carga': 'POL Carga',
        'pol zarpe': 'POL Zarpe',
        'etd inicial date (sch)': 'ETD Inicial Date (Sch)',
        'etd inicial (sch)': 'ETD Inicial (Sch)',
        'etd final date (sch)': 'ETD Final Date (Sch)',
        'etd final (sch)': 'ETD Final (Sch)',
        'etd date (sch)': 'ETD Date (Sch)',
        'etd (sch)': 'ETD (Sch)',
        'atd date (sch)': 'ATD Date (Sch)',
        'atd (sch)': 'ATD (Sch)',
        'tr1 puerto': 'TR1 Puerto',
        'tr1 nave': 'TR1 Nave',
        'tr1 eta': 'TR1 ETA',
        'tr1 ata': 'TR1 ATA',
        'tr1 ata (m)': 'TR1 ATA (M)',
        'tr1 descarga': 'TR1 Descarga',
        'tr1 carga': 'TR1 Carga',
        'tr1 etd': 'TR1 ETD',
        'tr1 atd': 'TR1 ATD',
        'tr1 atd (m)': 'TR1 ATD (M)',
        'tr2 puerto': 'TR2 Puerto',
        'tr2 nave': 'TR2 Nave',
        'tr2 eta': 'TR2 ETA',
        'tr2 ata': 'TR2 ATA',
        'tr2 ata (m)': 'TR2 ATA (M)',
        'tr2 descarga': 'TR2 Descarga',
        'tr2 carga': 'TR2 Carga',
        'tr2 etd': 'TR2 ETD',
        'tr2 atd': 'TR2 ATD',
        'tr2 atd (m)': 'TR2 ATD (M)',
        'tr3 puerto': 'TR3 Puerto',
        'tr3 nave': 'TR3 Nave',
        'tr3 eta': 'TR3 ETA',
        'tr3 ata': 'TR3 ATA',
        'tr3 ata (m)': 'TR3 ATA (M)',
        'tr3 descarga': 'TR3 Descarga',
        'tr3 carga': 'TR3 Carga',
        'tr3 etd': 'TR3 ETD',
        'tr3 atd': 'TR3 ATD',
        'tr3 atd (m)': 'TR3 ATD (M)',
        'tr4 puerto': 'TR4 Puerto',
        'tr4 nave': 'TR4 Nave',
        'tr4 eta': 'TR4 ETA',
        'tr4 ata': 'TR4 ATA',
        'tr4 ata (m)': 'TR4 ATA (M)',
        'tr4 descarga': 'TR4 Descarga',
        'tr4 carga': 'TR4 Carga',
        'tr4 etd': 'TR4 ETD',
        'tr4 atd': 'TR4 ATD',
        'tr4 atd (m)': 'TR4 ATD (M)',
        'eta date (sch)': 'ETA Date (Sch)',
        'eta (sch)': 'ETA (Sch)',
        'eta inicial date (sch)': 'ETA Inicial Date (Sch)',
        'eta inicial (sch)': 'ETA Inicial (Sch)',
        'eta final date (sch)': 'ETA Final Date (Sch)',
        'eta final (sch)': 'ETA Final (Sch)',
        'ata date (sch)': 'ATA Date (Sch)',
        'ata (sch)': 'ATA (Sch)',
        'pod llegada': 'POD Llegada',
        'pod descarga': 'POD Descarga',
        'pod devuelto vacío': 'POD Devuelto vacío',
        'w. sin bl': 'W. Sin BL',
        'w. sin contenedor': 'W. Sin contenedor',
        'w. sin pol': 'W. Sin POL',
        'w. sin pod': 'W. Sin POD',
        'w. iniciando': 'W. Iniciando',
        'w. atd e iniciando': 'W. ATD e Iniciando',
        'w. no tiene suscripción': 'W. No tiene suscripción',
        'w. pol = pod': 'W. POL = POD',
        'w. sin nave': 'W. Sin nave',
        'w. sin viaje': 'W. Sin viaje',
        'w. sin naviera': 'W. Sin naviera',
        'w. sin etd': 'W. Sin ETD',
        'w. etd en el pasado sin atd': 'W. ETD en el pasado sin ATD',
        'w. sin atd y ya zarpó': 'W. Sin ATD y ya zarpó',
        'w. atd >= eta': 'W. ATD >= ETA',
        'w. tr1 sin eta': 'W. TR1 sin ETA',
        'w. gran error eta tr1 - ata tr1': 'W. Gran error ETA TR1 - ATA TR1',
        'w. eta tr1 = etd total': 'W. ETA TR1 = ETD Total',
        'w. eta tr1 < etd total': 'W. ETA TR1 < ETD Total',
        'w. eta tr1 = eta total': 'W. ETA TR1 = ETA Total',
        'w. eta tr1 > eta total': 'W. ETA TR1 > ETA Total',
        'w. tr2 sin eta': 'W. TR2 sin ETA',
        'w. eta tr2 = etd total': 'W. ETA TR2 = ETD Total',
        'w. eta tr2 < etd total': 'W. ETA TR2 < ETD Total',
        'w. eta tr2 = eta total': 'W. ETA TR2 = ETA Total',
        'w. eta tr2 > eta total': 'W. ETA TR2 > ETA Total',
        'w. gran error eta tr2 - ata tr2': 'W. Gran error ETA TR2 - ATA TR2',
        'w. tr3 sin eta': 'W. TR3 sin ETA',
        'w. eta tr3 = etd total': 'W. ETA TR3 = ETD Total',
        'w. eta tr3 < etd total': 'W. ETA TR3 < ETD Total',
        'w. eta tr3 = eta total': 'W. ETA TR3 = ETA Total',
        'w. eta tr3 > eta total': 'W. ETA TR3 > ETA Total',
        'w. gran error eta tr3 - ata tr3': 'W. Gran error ETA TR3 - ATA TR3',
        'w. tr4 sin eta': 'W. TR4 sin ETA',
        'w. eta tr4 = etd total': 'W. ETA TR4 = ETD Total',
        'w. eta tr4 < etd total': 'W. ETA TR4 < ETD Total',
        'w. eta tr4 = eta total': 'W. ETA TR4 = ETA Total',
        'w. eta tr4 > eta total': 'W. ETA TR4 > ETA Total',
        'w. gran error eta tr4 - ata tr4': 'W. Gran error ETA TR4 - ATA TR4',
        'w. atasco transbordo': 'W. Atasco Transbordo',
        'w. ts1 = ts2': 'W. TS1 = TS2',
        'w. ts2 = ts3': 'W. TS2 = TS3',
        'w. ts3 = ts4': 'W. TS3 = TS4',
        'w. ts2 < ts1': 'W. TS2 < TS1',
        'w. ts3 < ts2': 'W. TS3 < TS2',
        'w. ts4 < ts3': 'W. TS4 < TS3',
        'w. port ts1 = port ts2': 'W. Port TS1 = Port TS2',
        'w. port ts2 = port ts3': 'W. Port TS2 = Port TS3',
        'w. port ts3 = port ts4': 'W. Port TS3 = Port TS4',
        'w. sin eta': 'W. Sin ETA',
        'w. eta en el pasado sin ata': 'W. ETA en el pasado sin ATA',
        'w. con ata, pero no finalizado o arribado': 'W. Con ATA, pero no Finalizado o Arribado',
        'w. etd >= eta': 'W. ETD >= ETA',
        'w. sin pod descarga, finalizado': 'W. Sin POD Descarga, Finalizado',
        'w. pod descarga < ata': 'W. POD Descarga < ATA',
        'w. sin pod descarga estimada': 'W. Sin POD Descarga estimada',
        'w. sin devolución, finalizado': 'W. Sin devolución, Finalizado',
        'w. devuelto vacío < pod descarga': 'W. Devuelto vacío < POD Descarga',
        'w. sin ata ni eta': 'W. Sin ATA ni ETA',
        'w. tr1 sin ata, pero con atd': 'W. TR1 sin ATA, pero con ATD',
        'w. tr2 sin ata, pero con atd': 'W. TR2 sin ATA, pero con ATD',
        'w. tr3 sin ata, pero con atd': 'W. TR3 sin ATA, pero con ATD',
        'w. tr4 sin ata, pero con atd': 'W. TR4 sin ATA, pero con ATD',
        'w. tr1 sin atd, pero tr2 con ata': 'W. TR1 sin ATD, pero TR2 con ATA',
        'w. tr2 sin atd, pero tr3 con ata': 'W. TR2 sin ATD, pero TR3 con ATA',
        'w. tr3 sin atd, pero tr4 con ata': 'W. TR3 sin ATD, pero TR4 con ATA',
        'w. tr1 sin atd, pero arribado': 'W. TR1 sin ATD, pero Arribado',
        'w. tr2 sin atd, pero arribado': 'W. TR2 sin ATD, pero Arribado',
        'w. tr3 sin atd, pero arribado': 'W. TR3 sin ATD, pero Arribado',
        'w. tr4 sin atd, pero arribado': 'W. TR4 sin ATD, pero Arribado',
        # 'w. transbordo final tr1 sin atd, pero arribado': 'W. Transbordo final TR1 sin ATD, pero Arribado',
        # 'w. transbordo final tr2 sin atd, pero arribado': 'W. Transbordo final TR2 sin ATD, pero Arribado',
        # 'w. transbordo final tr3 sin atd, pero arribado': 'W. Transbordo final TR3 sin ATD, pero Arribado',
        # 'w. transbordo final tr4 sin atd, pero arribado': 'W. Transbordo final TR4 sin ATD, pero Arribado',
        'w. inland sin ata, pero con atd': 'W. Inland sin ATA, pero con ATD',
        'w. inland sin descarga, pero con atd': 'W. Inland sin Descarga, pero con ATD',
        'w. inland sin atd, finalizado': 'W. Inland sin ATD, Finalizado',
        'w. inland incompleto, finalizado': 'W. Inland incompleto, Finalizado',
        'w. inland incompleto, no finalizado': 'W. Inland incompleto, No finalizado',
    }

@st.cache_data
def load_data_quality(client="Arauco",new_columns=data_quality_columns) -> pd.DataFrame:



    if client == "Arauco":
        data = pd.read_csv(f"https://klog.metabaseapp.com/public/question/c2c3c38d-e8e6-4482-ab6c-33e3f9317cce.csv")

    data = data.rename(columns=new_columns)
    data = data.drop(columns=['W. Inland incompleto, No finalizado'])

    return data

@st.cache_data
def load_data_quality_historic(date, client="Arauco",new_columns=data_quality_columns) -> pd.DataFrame:
    if client == "Arauco":
        data = load_csv_s3("klog-lake","raw/arauco_snapshots/",f"{date.strftime('%Y%m%d')}-arauco_snapshot.csv")
    data = data.rename(columns=new_columns)
    #data = data.drop(columns=['W. Inland incompleto, No finalizado'])        
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
            if "results" not in d.keys():
                continue
            data += [d]
    results = [i for sublist in [d["results"] for d in data] for i in sublist]
    return pd.DataFrame(results)