import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from kflow import extract
import pandas as pd
import numpy as np
import altair as alt
import datetime
import json

def create_warehouse_engine():
    from sqlalchemy import create_engine
    conn_bd = f"postgresql://{st.secrets['db_username']}:{st.secrets['db_password']}@{st.secrets['db_host']}:{st.secrets['db_port']}/{st.secrets['db_schema']}"
    return create_engine(conn_bd)

@st.cache_data
def load_data() -> pd.DataFrame:
    warehouse_engine = create_warehouse_engine()
    shipments = pd.read_sql_query('select * from "staging"."stg_operation_prisma_tracking"', warehouse_engine)
    query = '''
select * from "staging"."stg_operation_prisma_tracking"
left join
(
    select
        "subscription_bl"
        ,count("vecna_event_gh") as "number_of_gh_events"
        ,count("vecna_event_oi") as "number_of_oi_events"
        ,max("vecna_event_created_at") as "time_last_event"
    from "staging"."stg_vecna_event_consolidated_2"
    group by "subscription_bl"
) "events_by_bl"
on "staging"."stg_operation_prisma_tracking"."fd_mbl" = "events_by_bl"."subscription_bl"
    '''
    shipments = pd.read_sql_query(query, warehouse_engine)
    return shipments

@st.cache_data
def load_events_from_bl(mbl):
    warehouse_engine = create_warehouse_engine()
    query = f'''
        select *
        from "staging"."stg_vecna_event_consolidated_2"
        where "subscription_bl" = '{mbl}'
        '''
    events = pd.read_sql_query(query, warehouse_engine)
    return events

@st.cache_data
def load_events_from_booking(mbl):
    warehouse_engine = create_warehouse_engine()
    query = f'''
        select *
        from "staging"."stg_vecna_event_consolidated_2"
        where "subscription_booking" = '{mbl}'
        '''
    events = pd.read_sql_query(query, warehouse_engine)
    return events

@st.cache_data
def load_events_from_container(mbl):
    warehouse_engine = create_warehouse_engine()
    query = f'''
        select *
        from "staging"."stg_vecna_event_consolidated_2"
        where "subscription_container" = '{mbl}'
        '''
    events = pd.read_sql_query(query, warehouse_engine)
    return events

st.set_page_config(layout="wide")

stg_clients_retention_month = load_data()
columns = ["fd_mbl","container_number","sb_booking_number","s_id","carrier_name","pol","pod","company_legal_name","number_of_gh_events","number_of_oi_events","time_last_event"]
stg_clients_retention_month_short = stg_clients_retention_month[columns].copy()

st.write("# ~ Vecna Explorer ~")

st.write("### All data")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.write("*Filters*")
    
    arauco = st.checkbox('Arauco')
    if arauco:
        stg_clients_retention_month_short = stg_clients_retention_month_short.loc[lambda x: x["company_legal_name"].isin(['CELULOSA ARAUCO','MADERAS ARAUCO S.A'])]

with col2:

    group = st.selectbox("*Groups*",("none","container_number","fd_mbl","sb_booking_number"))
    def conteo(x): return len(set([y for y in x if y is not None]))
    aggd = {
        "carrier_name":set,
        "pol":set,
        "pod":set,
        "company_legal_name":set,
        "number_of_gh_events":"sum",
        "number_of_oi_events":"sum",
        "time_last_event":"min"
        }
    if group == "container_number":
        aggd["fd_mbl"] = set
        aggd["sb_booking_number"] = [set,conteo]
    elif group == "fd_mbl":
        aggd["container_number"] = [set,conteo]
        aggd["sb_booking_number"] = [set,conteo]
    elif group == "sb_booking_number":
        aggd["container_number"] = [set,conteo]
        aggd["fd_mbl"] = [set,conteo]
    if group != "none":
        stg_clients_retention_month_short = stg_clients_retention_month_short.groupby(group).agg(aggd).reset_index()
        stg_clients_retention_month_short.columns = [' '.join(col).strip() for col in stg_clients_retention_month_short.columns.values]

grid_options_builder = GridOptionsBuilder.from_dataframe(stg_clients_retention_month_short)
grid_options_builder.configure_pagination(enabled=True, paginationPageSize=15, paginationAutoPageSize=False)
grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
grid_options_builder.configure_grid_options(domLayout='normal')
grid_options_builder.configure_selection("single")
grid_options = grid_options_builder.build()
selected_shipment = AgGrid(stg_clients_retention_month_short, grid_options, columns_auto_size_mode=1)

if not selected_shipment["selected_rows"]:

    st.write("### Selecciona una fila ☝️ para ver eventos")

else:

    st.write("### Tracking por MBL")

    with st.spinner("Loading"):

        if group == "fd_mbl":
            mbl = selected_shipment["selected_rows"][0][group]
            events = load_events_from_bl(mbl)
        elif group == "container_number":
            container_number = selected_shipment["selected_rows"][0][group]
            events = load_events_from_container(container_number)
        elif group == "sb_booking_number":
            booking_number = selected_shipment["selected_rows"][0][group]
            events = load_events_from_booking(booking_number)
        else:
            st.info("Not supported")

        if len(events) == 0:

            st.info("No trackings found")

        else:

            subscription_bl = events['subscription_bl'].unique()
            subscription_id = events['subscription_id'].unique()
            subscription_received_at = events['subscription_received_at'].unique()
            subscription_carrier_code = events['subscription_carrier_code'].unique()
            st.markdown(f"""
            * BL: {(subscription_bl[0] if len(subscription_bl) == 1 else 'Error, more than one.')}
            * Carrier: {(subscription_carrier_code[0] if len(subscription_carrier_code) == 1 else 'Error, more than one.')}
            * Subscription id: {(subscription_id[0] if len(subscription_id) == 1 else 'Error, more than one.')}
            * Subscription received at: {(subscription_received_at[0] if len(subscription_received_at) == 1 else 'Error, more than one.')}
            """)
            
            cols = {
                "vecna_event_created_at":"created_at",
                #"subscription_booking":"booking",
                #"subscription_container":"container",
                "vecna_event_gh":"event_gh",
                "vecna_event_oi":"event_oi",
                "vecna_event":"event_vecna",
                "vecna_event_id":"id"
            }
            events = events[cols.keys()].rename(columns=cols).copy().sort_values(by="created_at",ascending=False).reset_index(drop=True)

            st.markdown("### Events")

            cola1, cola2, cola3 = st.columns([1,1,5])

            events = events.reset_index()
            events["unique"] = events.apply(lambda x: str(x["index"]) + ' - ' + datetime.datetime.strftime(pd.to_datetime(x["created_at"]),"%m-%d %H:%M:%S"), axis=1)

            with cola1:
                st.write("Dates of last events")

                event_datetime = st.radio("Dates", events[["unique"]], label_visibility="collapsed")
                event_json_value = (events.loc[lambda x: x["unique"] == event_datetime]["event_gh"].values)[0]

            with cola2:
                st.write("Containers")

                if event_json_value:
                    event_json = json.loads(event_json_value)
                    container = st.radio("Containers", sorted(list(event_json.keys())), label_visibility="collapsed")
                else:
                    st.error('Evento no tiene JSON', icon="🚨")

            with cola3:
                st.write("JSON")

                if event_json_value:
                    st.json(event_json[container])
