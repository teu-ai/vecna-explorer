import streamlit as st
from load import load_itinerarios
from tools.tools import setup_ambient, agrid_options
from st_aggrid import AgGrid
import pandas as pd
import duckdb

ARAUCO = True

setup_ambient()

st.set_page_config(layout="wide")

@st.cache_data
def load_itinerarios(month):
   con = duckdb.connect('itineraries.db')
   df = con.execute(f"""
                     SELECT
                        *
                     FROM
                        itineraries
                     WHERE
                        date_part('year', (replace(left(etd,position('+' in etd)-1),'T',' ') || ':00')::timestamp) = 2023 and
                        date_part('month',(replace(left(etd,position('+' in etd)-1),'T',' ') || ':00')::timestamp) = {month}
                    """).fetchdf()

   df.loc[:,"carrier_scac"] = df.loc[:,"carrier"].apply(lambda x: x["scac"])
   df.loc[:,"carrier"] = df.loc[:,"carrier"].apply(lambda x: x["short_name"])
   df.loc[:,"pol_name"] = df.loc[:,"pol"].apply(lambda x: x["name"])
   df.loc[:,"pol"] = df.loc[:,"pol"].apply(lambda x: x["locode"])
   df.loc[:,"pod_name"] = df.loc[:,"pod"].apply(lambda x: x["name"])
   df.loc[:,"pod"] = df.loc[:,"pod"].apply(lambda x: x["locode"])
   
   # Drop alliance column
   df = df.drop(columns=["alliance","uuid_p2p","p2p_id","id"])
   
   # Transform timestamp to datetime
   df["etd"] = df["etd"].apply(lambda x: pd.to_datetime(x))
   df["eta"] = df["eta"].apply(lambda x: pd.to_datetime(x))
   df["etd_local"] = df["etd_local"].apply(lambda x: pd.to_datetime(x))
   df["eta_local"] = df["eta_local"].apply(lambda x: pd.to_datetime(x))
   
   # Transhipments
   df.loc[:,"transhipments"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["locode"]+"-"+y["pod"]["locode"]] for y in x])
   df.loc[:,"transhipments_name"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["name"]+"-"+y["pod"]["name"]] for y in x])
   for i in range(5):
      df["transhipments_name_"+str(i+1)] = df["legs"].apply(lambda x: x[i]["pod"]["name"] if len(x)>i+1 else None)
   df["transhipments_name_1"] = df["transhipments_name_1"].apply(lambda x: x if x is not None else "DIRECT")
   df.loc[:,"vessel"] = df.loc[:,"legs"].apply(lambda x: [y["vessel"]["shipname"] for y in x])

   # Services
   df["service"] = df.apply(lambda y: [x["service_name"] for x in y["legs"]], axis=1)

   # Drop legs column
   df = df.drop(columns=["legs"])

   # Remove timezone of all datetime columns in df
   df["etd"] = df["etd"].apply(lambda x: x.replace(tzinfo=None))
   df["eta"] = df["eta"].apply(lambda x: x.replace(tzinfo=None))
   df["etd_local"] = df["etd_local"].apply(lambda x: x.replace(tzinfo=None))
   df["eta_local"] = df["eta_local"].apply(lambda x: x.replace(tzinfo=None))

   #df["a"] = df.apply(lambda x: (x["pod_name"] == x['transhipments_name_1'])|(x["pod_name"] == x['transhipments_name_2']),axis=1)

   # Leave only the following ports: Coronel – Lirquén – San Vicente – San Antonio – Valparaíso
   df = df[df["pol_name"].isin(["Coronel","Lirquén","San Vicente","Talcahuano","Talcahuano (San Vicente)","San Antonio","Valparaiso"])].copy()
   df = df[df["carrier_scac"].isin(["CMDU","COSU","EGLV","EVRG","HLCU","SUDU","MSCU","MAEU","ONEY","ZIMU"])].copy()

   return df

def convert_df_to_csv(df, columns):
   df2 = df[columns].copy()
   return df2.to_csv(index=False,sep=";",quotechar='"').encode('utf-8')

st.write("# Itinerarios")

col1, _ = st.columns([1,6])
with col1:
   mes = st.selectbox("Mes",[6,7],format_func=lambda x: "Junio" if x==6 else "Julio")

itinerarios = load_itinerarios(mes)

tabs = st.tabs(["Tiempos de tránsito","Tiempos a destino","Itinerarios"])

with tabs[2]:

   columns = [
      'carrier'
      ,'pol'
      ,'pol_name'
      ,'pod'
      ,'pod_name'
      ,'eta'
      ,'etd'
      ,'transshipment_count'
      ,'transit_time'
      ,'transhipments_name_1'
      ,'transhipments_name_2'
      ,'vessel'
      ,'service'
      ]

   # Download in CSV
   csv = convert_df_to_csv(itinerarios, columns)
   st.download_button(
      label="Descargar en CSV",
      data=csv,
      file_name="itinerarios.csv",
      mime="text/csv",
      key='download-csv-2'
      )

   AgGrid(itinerarios[columns], agrid_options(itinerarios[columns], 20))

with tabs[0]:

   itinerarios["service_first"] = itinerarios["service"].apply(lambda x: x[0])
   itinerarios["transhipments_name_1"].fillna("-", inplace=True)
   itinerarios["transhipments_name_2"].fillna("-", inplace=True)

   promedios = itinerarios.groupby(by=["pol","pod","carrier","transhipments_name_1","transhipments_name_2","service_first"]).agg({'transit_time':["mean","std","count"]}).reset_index()
   # Round transit_time to 2 decimals
   promedios["transit_time"] = promedios["transit_time"].apply(lambda x: round(x,1))
   # Flatten columns
   promedios.columns = ['_'.join(col).strip() for col in promedios.columns.values]

   promedios.rename(columns={
      "pol_":"POL",
      "pod_":"POD",
      "carrier_":"Naviera",
      "transhipments_name_1_":"Trasbordo 1",
      "transhipments_name_2_":"Trasbordo 2",
      "service_first_":"Servicio",
      "transit_time_mean":"Promedio tránsito",
      "transit_time_std":"Varianza tránsito",
      "transit_time_count":"Número de viajes"}, inplace=True)
   
   promedios_csv = convert_df_to_csv(promedios, promedios.columns)
   st.download_button(
      label="Descargar en CSV",
      data=promedios_csv,
      file_name="rutas.csv",
      mime="text/csv",
      key='download-csv'
      )

   AgGrid(promedios, agrid_options(promedios, 20))

with tabs[1]:

   destinos = itinerarios.groupby(["pod","pod_name"]).agg({'transit_time':["mean","std","count"]}).reset_index()
   destinos.columns = ['_'.join(col).strip() for col in destinos.columns.values]
   destinos["transit_time_mean"] = destinos["transit_time_mean"].apply(lambda x: round(x,1))
   destinos["transit_time_std"] = destinos["transit_time_std"].apply(lambda x: round(x,1))
   destinos.rename(columns={
      "pod_":"POD",
      "pod_name_":"Nombre POD",
      "transit_time_mean":"Promedio tránsito",
      "transit_time_std":"Varianza tránsito",
      "transit_time_count":"Número de viajes"
   }, inplace=True)

   destinos_csv = convert_df_to_csv(destinos, ["POD","Nombre POD","Promedio tránsito","Varianza tránsito","Número de viajes"])
   st.download_button(
      label="Descargar en CSV",
      data=destinos_csv,
      file_name="destinos.csv",
      mime="text/csv"
      )

   AgGrid(destinos, agrid_options(destinos, 20))