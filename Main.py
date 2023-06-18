import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import json
import load, components
from tools.tools import setup_ambient

ARAUCO = True

setup_ambient()

st.write("# Integraciones de datos KLog.co")

st.write("Bienvenido al centro de integración de dato.")

st.write("En el menú de la izquierda podrá encontrar el reporte de calidad de datos.")