import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from kflow import extract
import pandas as pd
import numpy as np
import altair as alt
import datetime
import json

def agrid_options(dataframe, page_size):
    grid_options_builder = GridOptionsBuilder.from_dataframe(dataframe)
    grid_options_builder.configure_pagination(enabled=True, paginationPageSize=page_size, paginationAutoPageSize=False)
    grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
    grid_options_builder.configure_grid_options(domLayout='normal')
    grid_options_builder.configure_selection("single")
    return grid_options_builder.build()

def create_warehouse_engine():
    from sqlalchemy import create_engine
    conn_bd = f"postgresql://{st.secrets['db_username']}:{st.secrets['db_password']}@{st.secrets['db_host']}:{st.secrets['db_port']}/{st.secrets['db_schema']}"
    return create_engine(conn_bd)

def create_prisma_engine():
    from sqlalchemy import create_engine
    conn_bd = f"postgresql://{st.secrets['prisma_username']}:{st.secrets['prisma_password']}@{st.secrets['prisma_host']}:{st.secrets['prisma_port']}/{st.secrets['prisma_database']}"
    return create_engine(conn_bd)

@st.cache_data
def load_data() -> pd.DataFrame:
    warehouse_engine = create_warehouse_engine()

    # All Vecna subscriptions
    query = '''
with subs as (
    select
        *
        ,row_number() over (partition by "subscription_bl" order by "subscription_created_at" asc) as row_number
    from
        "staging"."dev_vecna_subscription"
)
select * from subs where row_number = 1;
    '''
    subscriptions = pd.read_sql_query(query, warehouse_engine)

    # All Vecna events
    query = '''
select
    "vecna_event_id"
    ,"vecna_event_container"
    ,"subscription_id"
    ,"subscription_type"
    ,"subscription_bl"
    ,"subscription_booking"
    ,"subscription_container"
    ,"subscription_carrier_code"
    ,"vecna_event_created_at"
from
    "staging"."dev_vecna_event_consolidated"
where "vecna_event_created_at" >= '2023-03-24'
    '''
    events = pd.read_sql_query(query, warehouse_engine)

    # All containers that have events
    query = '''
select
    *
from
    "staging"."dev_operation_prisma_tracking"
inner join
(
    select distinct
        "vecna_event_container"
    from
        "staging"."dev_vecna_event_consolidated"
) as "containers"
on "staging"."dev_operation_prisma_tracking"."container_number" =  "containers"."vecna_event_container"
where
    "s_created_at" >= '2023-01-01'
    '''
    shipments_cargo = pd.read_sql_query(query, warehouse_engine)

    return (shipments_cargo, events, subscriptions)

@st.cache_data
def load_events_vecna(doctype, doc):
    if doctype == "mbl":
        where = f"where \"subscription_bl\" = '{doc}'"
    elif doctype == "container":
        where = f"where \"subscription_container\" = '{doc}'"
    elif doctype == "booking":
        where = f"where \"subscription_booking\" = '{doc}'"

    warehouse_engine = create_warehouse_engine()
    query = f'''
        select *
        from "staging"."stg_vecna_event_consolidated_2"
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
def load_event_vecna(vecna_event_id, subscription_id, event_created_at, event_container):

    warehouse_engine = create_warehouse_engine()
    query = f'''
        select
            *
        from
            "staging"."dev_vecna_event_consolidated"
        where
            ("vecna_event_id" = '{vecna_event_id}' or "vecna_event_id" is null)
            and "subscription_id" = '{subscription_id}'
            and "vecna_event_created_at" = '{event_created_at}'
            and "vecna_event_container" = '{event_container}'
        '''
    event = pd.read_sql_query(query, warehouse_engine)
    return event


st.set_page_config(layout="wide")

shipments_cargo, events, subscriptions = load_data()

st.write("# ~ Vecna Explorer ~")

st.write("#### Subscriptions @ Vecna")

subscriptions_table_columns = {
    "subscription_created_at":"created_at"
    ,"subscription_id":"id"
    ,"subscription_type":"type"
    ,"subscription_bl":"bl"
    ,"subscription_booking":"booking"
    ,"subscription_container":"container"
    ,"subscription_carrier_code":"carrier"
    ,"response_api_oi_creation":"response_oi"
    ,"response_api_gh_creation":"response_gh"
    }
subscriptions_table = subscriptions[subscriptions_table_columns.keys()].rename(columns=subscriptions_table_columns)
subscriptions_table = subscriptions_table.replace("NOT AVAILABLE","")
selected_subscription = AgGrid(subscriptions_table, agrid_options(subscriptions_table, 15), columns_auto_size_mode=1)

st.write("#### Events @ Vecna")

if selected_subscription["selected_rows"]:

    selected_subscription_id = selected_subscription["selected_rows"][0]["id"]
    events_table_columns = {
        "subscription_id":"subscription_id"
        ,"vecna_event_id":"id"
        ,"vecna_event_created_at": "created_at"
        ,"vecna_event_container":"container"
    }
    events_table = events[events_table_columns.keys()].rename(columns=events_table_columns)
    events_table = events_table.loc[lambda x: x["subscription_id"] == selected_subscription_id]
    selected_event = AgGrid(events_table, agrid_options(events_table, 15), columns_auto_size_mode=1)

    if selected_event["selected_rows"]:
        
        selected_event_id = selected_event["selected_rows"][0]["id"]
        selected_event_subscription_id = selected_event["selected_rows"][0]["subscription_id"]
        selected_event_created_at = selected_event["selected_rows"][0]["created_at"]
        selected_event_container = selected_event["selected_rows"][0]["container"]
        event = load_event_vecna(selected_event_id, selected_event_subscription_id, selected_event_created_at, selected_event_container)
        event_oi = json.loads(event["vecna_event_oi"].values[0])
        event_gh = json.loads(event["vecna_event_gh"].values[0])
        event_vecna = json.loads(event["vecna_event"].values[0])

        exp = st.expander("JSONs", expanded=False)

        with exp:

            col1, col2, col3 = st.columns([1,1,1])
            with col1:
                st.json(event_oi)
            with col2:
                st.json(event_gh)
            with col3:
                st.json(event_vecna)
#

st.write("#### Shipments @ Prisma")

#st.write("All shipments from 2023 considered.")

shipments_cargo_columns = {
    #"subscription_type":"sub"
    "fd_mbl":"mbl"
    ,"container_number":"container"
    ,"sb_booking_number":"booking"
    ,"s_id":"id"
    ,"carrier_final_name":"carrier"
    ,"pol_code":"pol"
    ,"pod_code":"pod"
    ,"company_legal_name":"client"
    #,"vecna_event_id":"events"
    #,"vecna_event_created_at":"last event"
}
shipments_cargo_table = shipments_cargo[shipments_cargo_columns.keys()].rename(columns=shipments_cargo_columns)

filters = st.expander("Filters", expanded=False)

with filters:

    cols_filters = st.columns(3)

    with cols_filters[0]:
        arauco = st.checkbox('Arauco')
    with cols_filters[1]:
        late_events = st.checkbox('Late events')
    with cols_filters[2]:
        carriers = st.multiselect('Carrier', shipments_cargo_table["carrier"].drop_duplicates())

if arauco:
    shipments_cargo_table = shipments_cargo_table.loc[lambda x: x["client"].isin(['CELULOSA ARAUCO','MADERAS ARAUCO S.A'])]

if carriers:
    shipments_cargo_table = shipments_cargo_table.loc[lambda x: x["carrier"].isin(carriers)]

if late_events:
    shipments_cargo_table = shipments_cargo_table.loc[lambda x: (x["last event"] - pd.to_datetime(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:00"), utc=True)) > datetime.timedelta(hours=1)]

selected_shipment = AgGrid(shipments_cargo_table, agrid_options(shipments_cargo_table, 15), columns_auto_size_mode=1)

# Shipment

if not selected_shipment["selected_rows"]:

    st.write("### Selecciona una fila ☝️ para ver eventos")

else:

    # Data

    shipment_id = selected_shipment["selected_rows"][0]["id"]
    container = selected_shipment["selected_rows"][0]["container"]

    print(shipment_id, container)

    shipment = shipments_cargo.loc[lambda x: x["s_id"] == shipment_id].copy()
    date_columns = [
        "sch_initial_etd"
        ,"sch_final_etd"
        ,"sch_atd"
        ,"sch_initial_eta"
        ,"sch_final_eta"
        ,"sch_ata"
        ,"sch_stacking"
        ,"sch_cutoff"
        ]
    for date_column in date_columns:
        shipment[date_column] = shipment[date_column].apply(lambda x: x if pd.isnull(pd.to_datetime(x)) else pd.to_datetime(x).strftime("%y/%m/%d %H:%M"))

    events_prisma = load_events_prisma(container, shipment.iloc[0]["s_created_at"]).sort_values("createdAt", ascending=False)
    print(events_prisma)
    event_prisma_last = events_prisma["body"].iloc[0]
    print(event_prisma_last)

    #doctype = selected_shipment["selected_rows"][0]["sub"]
    #doc = selected_shipment["selected_rows"][0]["mbl"]
    event_vecna_last = event_prisma_last.copy()

    t = pd.DataFrame([
        [
            # Initial Vessel
            shipment['sb_initial_vessel'].unique()[0],
            event_prisma_last["shipment"]["current_vessel"]["name"] if event_prisma_last["shipment"]["current_vessel"] is not None else None,
            event_vecna_last["shipment"]["current_vessel"]["name"] if event_vecna_last["shipment"]["current_vessel"] is not None else None
        ],[
            # Final Vessel
            shipment['sb_final_vessel'].unique()[0],
            event_prisma_last["shipment"]["current_vessel"]["name"] if event_prisma_last["shipment"]["current_vessel"] is not None else None,
            event_vecna_last["shipment"]["current_vessel"]["name"] if event_vecna_last["shipment"]["current_vessel"] is not None else None
        ],[
            # Initial Carrier
            shipment['carrier_initial_name'].unique()[0],
            event_prisma_last["shipment"]["carrier_name"] if event_prisma_last["shipment"]["carrier_name"] is not None else None,
            event_vecna_last["shipment"]["carrier_name"] if event_vecna_last["shipment"]["carrier_name"] is not None else None
        ],[
            # Final Carrier
            shipment['carrier_final_name'].unique()[0],
            event_prisma_last["shipment"]["carrier_name"] if event_prisma_last["shipment"]["carrier_name"] is not None else None,
            event_vecna_last["shipment"]["carrier_name"] if event_vecna_last["shipment"]["carrier_name"] is not None else None,
        ],[
            shipment['sch_initial_etd'].unique()[0],
            event_prisma_last["shipment"]["pol_arrival_planned_initial"] if event_prisma_last["shipment"]["pol_arrival_planned_initial"] is not None else None,
            event_vecna_last["shipment"]["pol_arrival_planned_initial"] if event_vecna_last["shipment"]["pol_arrival_planned_initial"] is not None else None,
        ],[
            shipment['sch_final_etd'].unique()[0],
            event_prisma_last["shipment"]["pol_arrival_planned_last"] if event_prisma_last["shipment"]["pol_arrival_planned_last"] is not None else None,
            event_vecna_last["shipment"]["pol_arrival_planned_last"] if event_vecna_last["shipment"]["pol_arrival_planned_last"] is not None else None,
        ],[
            shipment['sch_atd'].unique()[0],
            event_prisma_last["shipment"]["pol_arrival_actual"] if event_prisma_last["shipment"]["pol_arrival_actual"] is not None else None,
            event_vecna_last["shipment"]["pol_arrival_actual"]
        ],[
            shipment['sch_initial_eta'].unique()[0],
            event_prisma_last["shipment"]["pod_vslarrival_planned_initial"] if event_prisma_last["shipment"]["pod_vslarrival_planned_initial"] is not None else None,
            event_vecna_last["shipment"]["pod_vslarrival_planned_initial"]
        ],[
            shipment['sch_final_eta'].unique()[0],
            event_prisma_last["shipment"]["pod_vslarrival_planned_last"] if event_prisma_last["shipment"]["pod_vslarrival_planned_last"] is not None else None,
            event_vecna_last["shipment"]["pod_vslarrival_planned_last"] if event_vecna_last["shipment"]["pod_vslarrival_planned_initial"] is not None else None,
        ],[
            shipment['sch_ata'].unique()[0],
            event_prisma_last["shipment"]["pod_vslarrival_actual"] if event_prisma_last["shipment"]["pod_vslarrival_actual"] is not None else None,
            event_vecna_last["shipment"]["pod_vslarrival_actual"] if event_vecna_last["shipment"]["pod_vslarrival_actual"] is not None else None,
        ],[
            shipment['sch_stacking'].unique()[0],
            "-",
            "-"
        ],[
            shipment['sch_cutoff'].unique()[0],
            "-",
            "-"
        ]
        ],
        columns=["Prisma","Prisma - OI","Vecna"],
        index=["iCarrier","fCarrier","iVessel","fVessel","iETD","fETD","ATD","iETA","fETA","ATA","Stacking","Cutoff"]
        )
    
    st.write(f"### [Detalles embarque](https://app.klog.co/shipments/{shipment_id})")

    #AgGrid(t, agrid_options(t, 15), columns_auto_size_mode=1)

    st.dataframe(t, width=1000, height=500)