import streamlit as st
from load import load_itinerarios
from tools.tools import setup_ambient

ARAUCO = True

setup_ambient()

st.set_page_config(layout="wide")

st.write("# Itinerarios")

df = load_itinerarios()

st.dataframe(df)