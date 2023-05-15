import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import json
import load, components

def agrid_options(dataframe, page_size):
    grid_options_builder = GridOptionsBuilder.from_dataframe(dataframe)
    grid_options_builder.configure_pagination(enabled=True, paginationPageSize=page_size, paginationAutoPageSize=False)
    grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
    grid_options_builder.configure_grid_options(domLayout='normal')
    grid_options_builder.configure_selection("single")
    return grid_options_builder.build()

st.set_page_config(layout="wide")

st.write("# Vecna Explorer")

if 'selected_entrega' not in st.session_state:
    st.session_state.selected_entrega = ""
if 'mbl' not in st.session_state:
    st.session_state.mbl = ''

env = st.selectbox("Ambiente", options=["prod","staging"])

subscriptions = load.load_subscriptions(env)

events = load.load_events(env)

subscriptions = subscriptions.merge(events.groupby("subscription_doc").agg(
    {"subscription_id":"count"
    ,"vecna_event_created_at":"max"
        }), on="subscription_doc", how="left").rename(columns={
    "subscription_id_x":"subscription_id"
    ,"subscription_id_y":"events"
    ,"vecna_event_created_at":"last"
    })

col1, col2 = st.columns([1,1])
with col1:
    # Choose MBL
    if st.session_state.mbl:
        selected_mbl = st.multiselect("MBL", subscriptions["subscription_bl"].unique(), default=[st.session_state.mbl])
    else:
        selected_mbl = st.multiselect("MBL", subscriptions["subscription_bl"].unique())

st.write("#### Subscriptions")

subscriptions_table_columns = {
    "subscription_created_at":"created_at"
    ,"subscription_id":"id"
    ,"subscription_type":"type"
    ,"subscription_doc":"doc"
    ,"subscription_carrier_code":"carrier"
    ,"response_api_oi_creation":"response_oi"
    ,"response_api_gh_creation":"response_gh"
    ,"events":"events"
    ,"last":"last"
    }

subscriptions_table = subscriptions[subscriptions_table_columns.keys()].rename(columns=subscriptions_table_columns)
if selected_mbl:
    subscriptions_table = subscriptions_table.replace("NOT AVAILABLE","")
    subscriptions_table = subscriptions_table.loc[lambda x: x["doc"].isin(selected_mbl)]
selected_subscription = AgGrid(subscriptions_table, agrid_options(subscriptions_table, 15), columns_auto_size_mode=1)

st.write("#### Events")

selected_event = []

if len(selected_subscription["selected_rows"]) == 0:

    st.write("No hay suscripciones seleccionadas")

else:

    events = load.load_events(env)

    selected_subscription_id = selected_subscription["selected_rows"][0]["id"]
    events_table_columns = {
        "subscription_id":"subscription_id"
        ,"subscription_doc":"subscription_doc"
        ,"vecna_event_id":"id"
        ,"vecna_event_created_at": "created_at"
        ,"raw_event_gh":"raw_event_gh"
        ,"raw_event_oi":"raw_event_oi"
    }
    events_table = events[events_table_columns.keys()].rename(columns=events_table_columns)
    events_table = events_table.loc[lambda x: x["subscription_id"] == selected_subscription_id]
    events_table.sort_values(by="created_at", ascending=False, inplace=True)
    selected_event = AgGrid(events_table, agrid_options(events_table, 15), columns_auto_size_mode=1)

if selected_event and len(selected_event["selected_rows"]) != 0:

    selected_event_id = selected_event["selected_rows"][0]["id"]
    selected_event_subscription_id = selected_event["selected_rows"][0]["subscription_id"]
    selected_event_created_at = selected_event["selected_rows"][0]["created_at"]
    selected_event_doc = selected_event["selected_rows"][0]["subscription_doc"]

    #event = load_event_vecna(env, selected_event_id, selected_event_subscription_id, selected_event_created_at, selected_event_container)
    print(selected_event_id)
    event = load.load_event_vecna(env, selected_event_id)
    print(event)
    event_vecna_gh_text = event["vecna_event_gh"].values[0]
    event_vecna_oi_text = event["vecna_event_oi"].values[0]
    event_vecna_text = event["vecna_event"].values[0]

    # Vecna events

    event_vecna_oi = json.loads(event_vecna_oi_text) if event_vecna_oi_text is not None else {}
    event_vecna_gh = json.loads(event_vecna_gh_text) if event_vecna_gh_text is not None else {}
    event_vecna = json.loads(event_vecna_text) if event_vecna_text is not None else {}

    # Raw events

    if event["raw_event_oi"].values[0] and event["raw_event_oi"].values[0] != "subscription":
        event_oi_raw = load.load_event_raw(event["raw_event_oi"].values[0], "oceaninsights/")
    else:
        event_oi_raw = {}
    if event["raw_event_gh"].values[0] and event["raw_event_gh"].values[0] != "subscription":
        event_gh_raw = load.load_event_raw(event["raw_event_gh"].values[0], "ghmaritime/")
    else:
        event_gh_raw = {}

    exp = st.expander("JSONs", expanded=False)

    with exp:

        tab1, tab2, tab3, tab4 = st.tabs(["Vecna","Raw", "Vecna Back", "Dynamo"])

        with tab1:

            col1, col2, col3 = st.columns([1,1,1])
            with col1:
                st.write("Vecna Ocean Insights")
                st.json(event_vecna_oi)
            with col2:
                st.write("Vecna Gatehouse")
                st.json(event_vecna_gh)
            with col3:
                st.write("Vecna")
                st.json(event_vecna)
        
        with tab2:
        
            col1, col2 = st.columns([1,1])
            with col1:
                st.write("Ocean Insights")
                if event_oi_raw == {}:
                    st.warning("File not found")
                else:
                    st.json(event_oi_raw)
            with col2:
                st.write("Gatehouse")
                if event_gh_raw == {}:
                    st.warning("File not found")
                else:
                    st.json(event_gh_raw)

        with tab3:

            event = load.load_event_vecna_back(selected_event_subscription_id, env)
            st.json(event)

        with tab4:

            event = load.load_event_dynamo(selected_event_subscription_id, env)
            st.json(event)
    #

st.write("### Shipment")

if selected_subscription and len(selected_subscription["selected_rows"]) != 0:

    selected_subscription_id = selected_subscription["selected_rows"][0]["id"]
    components.show_shipment_prisma(selected_subscription_id)