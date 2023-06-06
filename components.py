import streamlit as st
import pandas as pd
import load
import json

def show_shipment_prisma(selected_subscription_id = None, rows_to_highlight = []):
    if selected_subscription_id is None:
        st.write("No hay suscripciones seleccionadas")
    else:
        shipment_prisma = load.load_shipments_prisma(selected_subscription_id)
        shipment_prisma = shipment_prisma.iloc[0].reset_index().rename(columns={"index":"Problema"})
        
        def color_coding(row):
            return ['background-color:#ffaaaa'] * len(row) if row["Problema"] in rows_to_highlight else [None] * len(row)
        
        st.dataframe(shipment_prisma.style.apply(color_coding, axis=1), height=2000)

def show_data_sources(event_id:str, subscription_id:str, events_s3:bool = False, vecna_db:bool = False, vecna_dynamo:bool = True, vecna_api:bool = True, event_raw=None):
    # Data sources

    sources = []
    if events_s3:
        sources += ["Source Event (S3)"]
    if vecna_db:
        sources += ["Vecna (DB)"]
    if vecna_dynamo:
        sources += ["Vecna (Dynamo)"]
    if vecna_api:
        sources += ["Vecna (API)"]

    tabs = st.tabs(sources)
    c = 0

    if events_s3:
        with tabs[c]:
            st.write("Evento")
            if event_raw == {}:
                st.warning("File not found")
            else:
                st.json(event_raw)
            st.dataframe(pd.DataFrame(event_raw["events"]))
        c += 1
        
    if vecna_db:
        event = load.load_event_vecna("prod", event_id)
        event_vecna_gh_text = event["vecna_event_gh"].values[0]
        event_vecna_oi_text = event["vecna_event_oi"].values[0]
        event_vecna_text = event["vecna_event"].values[0]
        event_vecna_oi = json.loads(event_vecna_oi_text) if event_vecna_oi_text is not None else {}
        event_vecna_gh = json.loads(event_vecna_gh_text) if event_vecna_gh_text is not None else {}
        event_vecna = json.loads(event_vecna_text) if event_vecna_text is not None else {}
        with tabs[c]:
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
        c += 1

    if vecna_api:
        with tabs[c]:
            event = load.load_event_vecna_back(subscription_id, "prod")
            st.json(event)
        c += 1

    if vecna_dynamo:
        with tabs[c]:
            event = load.load_event_dynamo(subscription_id, "prod")
            st.json(event)
        c += 1