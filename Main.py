import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import json
import load, components

ARAUCO = True


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

delete_page("Main.py","Vecna_explorer")

st.write("# Integraciones de datos KLog.co")

st.write("Bienvenido al centro de integración de datos.")

st.write("En el menú de la izquierda podrá encontrar el reporte de calidad de datos.")