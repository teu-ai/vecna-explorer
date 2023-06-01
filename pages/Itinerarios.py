import streamlit as st
from load import load_itinerarios
from tools.tools import setup_ambient, agrid_options
from st_aggrid import AgGrid
import pandas as pd

ARAUCO = True

setup_ambient()

st.set_page_config(layout="wide")

st.write("# Itinerarios")

df = load_itinerarios()

df.loc[:,"carrier_scac"] = df.loc[:,"carrier"].apply(lambda x: x["scac"])
df.loc[:,"carrier"] = df.loc[:,"carrier"].apply(lambda x: x["short_name"])
df.loc[:,"pol"] = df.loc[:,"pol"].apply(lambda x: x["locode"])
df.loc[:,"pod"] = df.loc[:,"pod"].apply(lambda x: x["locode"])
# Drop alliance column
df = df.drop(columns=["alliance","uuid_p2p","p2p_id","id"])
# Transform timestamp to datetime
df.loc[:,"etd"] = df.loc[:,"etd"].apply(lambda x: pd.to_datetime(x))
df.loc[:,"eta"] = df.loc[:,"eta"].apply(lambda x: pd.to_datetime(x))
df.loc[:,"etd_local"] = df.loc[:,"etd_local"].apply(lambda x: pd.to_datetime(x))
df.loc[:,"eta_local"] = df.loc[:,"eta_local"].apply(lambda x: pd.to_datetime(x))
# 
df.loc[:,"transhipments"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["locode"]+"-"+y["pod"]["locode"]] for y in x])
df.loc[:,"transhipments_name"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["name"]+"-"+y["pod"]["name"]] for y in x])
df.loc[:,"vessel"] = df.loc[:,"legs"].apply(lambda x: [y["vessel"]["shipname"] for y in x])
# Drop legs column
df = df.drop(columns=["legs"])

def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

csv = convert_df(df)

st.download_button(
   "Descargar en CSV",
   csv,
   "itinerarios.csv",
   "text/csv",
   key='download-csv'
)

columns = ['carrier', 'carrier_scac', 'pol', 'pod', 'etd', 'etd_local', 'eta', 'eta_local',
   'cyclosing', 'transshipment_count', 'transhipments', 'transhipments_name', 'vessel', 'transit_time']

AgGrid(df[columns], agrid_options(df[columns], 20), fit_columns_on_grid_load=True)
