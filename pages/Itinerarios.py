import streamlit as st
from load import load_itinerarios
from tools.tools import setup_ambient, agrid_options
from st_aggrid import AgGrid
import pandas as pd
import duckdb

MONTHS = {6:"Junio",7:"Julio",8:"Agosto"}

setup_ambient(ambient="Arauco")

st.set_page_config(layout="wide")

@st.cache_data
def load_itinerarios(month: int, year:int=2023):
   """Load itineraries from database and transform it to a dataframe

   Args:
      month (int): Month to filter itineraries
      year (int): Year to filter itineraries
   """

   # Connect to local database itineraries.db
   con = duckdb.connect('itineraries.db')

   # Query
   query = f"""--sql
SELECT
   *
FROM
   itineraries
WHERE
   -- We convert the timestamp to a string and then we remove the timezone
   date_part('year', (replace(left(etd,position('+' in etd)-1),'T',' ') || ':00')::timestamp) = 2023 and
   date_part('month',(replace(left(etd,position('+' in etd)-1),'T',' ') || ':00')::timestamp) = {month};
"""

   # Get dataframe from database and then close connection
   df = con.execute(query).fetchdf()
   con.close()

   # Transform JSON data in specific columns to individual columns
   df.loc[:,"carrier_scac"] = df.loc[:,"carrier"].apply(lambda x: x["scac"])
   df.loc[:,"carrier"] = df.loc[:,"carrier"].apply(lambda x: x["short_name"])
   df.loc[:,"pol_name"] = df.loc[:,"pol"].apply(lambda x: x["name"])
   df.loc[:,"pol"] = df.loc[:,"pol"].apply(lambda x: x["locode"])
   df.loc[:,"pod_name"] = df.loc[:,"pod"].apply(lambda x: x["name"])
   df.loc[:,"pod"] = df.loc[:,"pod"].apply(lambda x: x["locode"])
   
   # Drop alliance column, not necesary
   df = df.drop(columns=["alliance","uuid_p2p","p2p_id","id"])
   
   # Transform timestamp to datetime
   df["etd"] = df["etd"].apply(lambda x: pd.to_datetime(x))
   df["eta"] = df["eta"].apply(lambda x: pd.to_datetime(x))
   df["etd_local"] = df["etd_local"].apply(lambda x: pd.to_datetime(x))
   df["eta_local"] = df["eta_local"].apply(lambda x: pd.to_datetime(x))
   
   # Transhipments: first create column with all of thems
   df.loc[:,"transhipments"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["locode"]+"-"+y["pod"]["locode"]] for y in x])
   df.loc[:,"transhipments_name"] = df.loc[:,"legs"].apply(lambda x: [[y["pol"]["name"]+"-"+y["pod"]["name"]] for y in x])
   # Transhipments: then create columns for each transhipment, iterating over list
   for i in range(5):
      df["transhipments_name_"+str(i+1)] = df["legs"].apply(lambda x: x[i]["pod"]["name"] if len(x)>i+1 else None)
   # If there are no transhipments, then set "DIRECT" as first transhipment. This was Arauco's request.
   df["transhipments_name_1"] = df["transhipments_name_1"].apply(lambda x: x if x is not None else "DIRECT")
   df.loc[:,"vessel"] = df.loc[:,"legs"].apply(lambda x: [y["vessel"]["shipname"] for y in x])

   # List of services
   df["service"] = df.apply(lambda y: [x["service_name"] for x in y["legs"]], axis=1)
   if month == 8:
      # filter services from Agosto
      df["service"] = df.apply(lambda x: select_service(x), axis=1)
   #
   # delete rows with no services
   df = df[~pd.isnull(df["service"])]

   # # for testing
   # print(df.loc[lambda x: x["carrier_scac"] == "CMDU",["service"]].values)
   #
   # services_test = []
   # # scac = "CMDU"
   # # scac = "COSU"
   # # scac = "EVRG"
   # # scac = "SUDU"
   # # scac = "HLCU"
   # # scac = "MAEU"
   # # scac = "MSCU"
   # scac = "ONEY"
   # for i, r in df.iterrows():
   #    if r["carrier_scac"] == scac:
   #       services_test.extend(r["service"])
   # services_test = list(set(services_test))

   # Drop legs column, we don't need it anymore
   df = df.drop(columns=["legs"])

   # Remove timezone of all datetime columns in df
   datetime_columns = ["etd", "eta", "etd_local", "eta_local"]
   for datetime_column in datetime_columns:
      df[datetime_column] = df[datetime_column].apply(lambda x: x.replace(tzinfo=None))

   # Leave only the following ports: Coronel, Lirquén, San Vicente, San Antonio, Valparaíso. Arauco's request.
   # We do this as some initial data contained more ports. Current data doesn't as we filter at the API level.
   df = df[df["pol_name"].isin(["Coronel","Lirquén","San Vicente","Talcahuano","Talcahuano (San Vicente)","San Antonio","Valparaiso"])].copy()
   # Leave only these carriers: CMDU, COSU, EGLV, EVRG, HLCU, SUDU, MSCU, MAEU, ONEY, ZIMU, Arauco's request.
   df = df[df["carrier_scac"].isin(["CMDU","COSU","EGLV","EVRG","HLCU","SUDU","MSCU","MAEU","ONEY","ZIMU"])].copy()

   # Create column with first service, to compute average times over it
   df["service_first"] = df["service"].apply(lambda x: x[0])

   # To display it better on DataFrames
   df["transhipments_name_1"].fillna("-", inplace=True)
   df["transhipments_name_2"].fillna("-", inplace=True)
   df["transhipments_name_3"].fillna("-", inplace=True)
   df["transhipments_name_4"].fillna("-", inplace=True)

   return df

def select_service(x):

   servicios_in = {
      "CMDU": [
         "AMERICAS XL SERVICE",
         "ASIA CENTRAL SOUTH AMERICA 2",
         "ASIA CENTRAL SOUTH AMERICA SERVICE 3",
         "EUROSAL XL SERVICE",
      ],
      "COSU": [
         "ASIA - MIDDLE AND SOUTH AMERICA WEST COAST WEEKLY SERVICE 3",
         "ASIA - SOUTH AMERICA WEST COAST SERVICE",
      ],
      "EVRG": [
         "ASIA - SOUTH AMERICA WEST COAST SERVICE",
         "ASIA - SOUTH AMERICA WEST COAST SERVICE 3",
      ],
      "EGLV": [
         "ASIA - SOUTH AMERICA WEST COAST SERVICE",
         "ASIA - SOUTH AMERICA WEST COAST SERVICE 3",
      ],
      "SUDU": [
         "ATACAMA SERVICE"
      ],
      "HLCU": [
         "CONOSUR SERVICE - LOOP1",
         "CONOSUR SERVICE - LOOP2",
         "SOUTH AMERICA - ASIA - LOOP 2",
         "SOUTH AMERICA - ASIA SERVICE - LOOP 1",
         "WEST COAST SOUTH AMERICA FEEDER SERVICE 3",
      ],
      "MAEU": [
         "ATACAMA SERVICE",
         "WEST COAST LATIN AMERICA-NORTH EUROPE EXPRESS SERVICE",
      ],
      "MSCU": [
         "ANDES EXPRESS SERVICE",
         "ASIA - LATIN AMERICA - INCA SERVICE",
         "NORTH EUROPE WEST COAST - UNITED STATES OF AMERICA - SOUTH AMERICA WEST COAST SERVICE",
      ],
      "ONEY": [
         "ASIA LATIN AMERICA EXPRESS SERVICE 1 - ALX1",
         "ASIA LATIN AMERICA EXPRESS SERVICE 2 - ALX2",
      ],
   }

   carrier_scac = x["carrier_scac"]
   if carrier_scac in servicios_in:
      s = [c for c in x["service"] if c in servicios_in[carrier_scac]]
      if len(s) > 0:
         return s
      else:
         return None
   else:
      return x["service"]

def convert_df_to_csv(df: pd.DataFrame, columns: list[str]):
   """Convert dataframe to CSV

   Args:
      df (pandas.DataFrame): Dataframe to convert
      columns (list): List of columns to include in CSV
   """
   df2 = df[columns].copy()

   return df2.to_csv(index=False,sep=";",quotechar='"').encode('utf-8')

def ui_download_as_csv_button(df:pd.DataFrame, filename:str="tmp.csv"):
   """Download button for CSV

   Args:
      df (pandas.DataFrame): Dataframe to download
      filename (str, optional): Filename to download. Defaults to "tmp.csv".
   """
   csv = convert_df_to_csv(df, df.columns)
   return st.download_button(label="Descargar en CSV", data=csv, file_name=filename, mime="text/csv")



# UI: Title
st.write("# Itinerarios")

# UI: Month selector
col1, _ = st.columns([1,6])
with col1:
   mes = st.selectbox("Mes",MONTHS.keys(),format_func=lambda x: MONTHS[x])
# Get data based on month selected
itinerarios = load_itinerarios(mes)

# UI: Tabs
tabs = st.tabs(["Tiempos de tránsito","Tiempos a destino","Itinerarios"])

with tabs[0]:
   # Tiempos de tránsito

   # Group by pol, pod, carrier, transhipments_name_1, transhipments_name_2, service_first and compute mean, std and number of trips from transit time.
   # deepcopy
   itinerarios["transhipments"] = itinerarios["transhipments"].apply(lambda x: str(x))
   itinerarios["service"] = itinerarios["service"].apply(lambda x: str(x))
   promedios = itinerarios.groupby(by=["pol","pod","carrier","transhipments","transhipments_name_1","transhipments_name_2","service_first","service"]).agg({'transit_time':["mean","std","count"]}).reset_index()
   # Round transit_time to 2 decimals
   promedios["transit_time"] = promedios["transit_time"].apply(lambda x: round(x,1))
   # Flatten multiindex columns
   promedios.columns = ['_'.join(col).strip() for col in promedios.columns.values]

   # Names to show in table
   promedios.rename(columns={
      "pol_":"POL",
      "pod_":"POD",
      "carrier_":"Naviera",
      "transhipments_":"Trasbordos",
      "transhipments_name_1_":"Trasbordo 1",
      "transhipments_name_2_":"Trasbordo 2",
      "service_first_":"Servicio",
      "service_":"Servicio o",
      "transit_time_mean":"Promedio tránsito",
      "transit_time_std":"Varianza tránsito",
      "transit_time_count":"Número de viajes"}, inplace=True)
   
   # Button to download in CSV
   ui_download_as_csv_button(promedios, filename="promedios.csv")

   # UI: Show table
   AgGrid(promedios, agrid_options(promedios, 20))

with tabs[1]:
   # Tiempos a destino

   # Group by pod and compute mean, std and number of trips from transit time.
   # This is, the average transit time to each destination.
   destinos = itinerarios.groupby(["pod","pod_name"]).agg({'transit_time':["mean","std","count"]}).reset_index()
   # Flatten multiindex columns
   destinos.columns = ['_'.join(col).strip() for col in destinos.columns.values]
   # Round transit_time to 1 decimal
   destinos["transit_time_mean"] = destinos["transit_time_mean"].apply(lambda x: round(x,1))
   destinos["transit_time_std"] = destinos["transit_time_std"].apply(lambda x: round(x,1))

   # Names to show in table
   destinos.rename(columns={
      "pod_":"POD",
      "pod_name_":"Nombre POD",
      "transit_time_mean":"Promedio tránsito",
      "transit_time_std":"Varianza tránsito",
      "transit_time_count":"Número de viajes"
   }, inplace=True)

   # Button to download in CSV
   destinos_csv = destinos[["POD","Nombre POD","Promedio tránsito","Varianza tránsito","Número de viajes"]].copy()
   ui_download_as_csv_button(destinos_csv, filename="destinos.csv")

   # UI: Show table
   AgGrid(destinos, agrid_options(destinos, 20))

with tabs[2]:
   # Itinerarios

   # Columns to rename and show
   columns_dict = {
      'carrier':'Naviera'
      ,'pol':'POL'
      ,'pol_name':'Nombre POL'
      ,'pod':'POD'
      ,'pod_name':'Nombre POD'
      ,'eta':'ETA'
      ,'etd':'ETD'
      ,'transshipment_count':'Número de trasbordos'
      ,'transit_time':'Tiempo de tránsito'
      ,'transhipments_name_1':'Trasbordo 1'
      ,'transhipments_name_2':'Trasbordo 2'
      ,'vessel':'Nave'
      ,'service':'Servicio'
      }
   
   # Create table to show
   itinerarios_table = itinerarios.copy()
   itinerarios_table = itinerarios_table.rename(columns=columns_dict)[columns_dict.values()]

   # Download in CSV
   ui_download_as_csv_button(itinerarios_table, filename="itinerarios.csv")
   
   # UI: Show table
   AgGrid(itinerarios_table, agrid_options(itinerarios_table, 20))