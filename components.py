import streamlit as st
import load

def show_shipment_prisma(selected_subscription_id = None, rows_to_highlight = []):
    if selected_subscription_id is None:
        st.write("No hay suscripciones seleccionadas")
    else:
        shipment_prisma = load.load_shipments_prisma(selected_subscription_id)
        shipment_prisma = shipment_prisma.iloc[0].reset_index().rename(columns={"index":"Problema"})
        
        def color_coding(row):
            return ['background-color:#ffaaaa'] * len(row) if row["Problema"] in rows_to_highlight else [None] * len(row)
        
        st.dataframe(shipment_prisma.style.apply(color_coding, axis=1), height=2000)
