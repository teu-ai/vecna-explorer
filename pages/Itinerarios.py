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
for i in range(len(df["legs"])):
   df.loc[:,"transhipments_name_"+str(i)] = df.loc[:,"legs"].apply(lambda x: x[i]["pod"]["name"] if len(x)>i else None)
df.loc[:,"vessel"] = df.loc[:,"legs"].apply(lambda x: [y["vessel"]["shipname"] for y in x])
# Drop legs column
df = df.drop(columns=["legs"])

def convert_df_to_csv(df):
   return df.to_csv(index=False).encode('utf-8')

def convert_df_to_excel(df):
   from io import BytesIO
   from pyxlsb import open_workbook as open_xlsb
   import streamlit as st
   output = BytesIO()
   writer = pd.ExcelWriter(output, engine='xlsxwriter')
   df.to_excel(writer, index=False, sheet_name='Sheet1')
   workbook = writer.book
   worksheet = writer.sheets['Sheet1']
   format1 = workbook.add_format({'num_format': '0.00'}) 
   worksheet.set_column('A:A', None, format1)  
   writer.save()
   processed_data = output.getvalue()
   return processed_data

# Remove timezone of all datetime columns in df
print(df.columns)
df.loc[:,"etd"] = df.loc[:,"etd"].apply(lambda x: x.replace(tzinfo=None))
df.loc[:,"eta"] = df.loc[:,"eta"].apply(lambda x: x.replace(tzinfo=None))
df.loc[:,"etd_local"] = df.loc[:,"etd_local"].apply(lambda x: x.replace(tzinfo=None))
df.loc[:,"eta_local"] = df.loc[:,"eta_local"].apply(lambda x: x.replace(tzinfo=None))

csv = convert_df_to_csv(df)
xlsx = convert_df_to_excel(df)

_, col1, col2 = st.columns([4,1,1])

with col1:
   st.download_button(
      label="Descargar en CSV",
      data=csv,
      file_name="itinerarios.csv",
      mime="text/csv",
      key='download-csv'
      )

with col2:
   st.download_button(
      label='Descargar en Excel',
      data=xlsx ,
      file_name='itinerarios.xlsx'
      )

columns = [
   'carrier'
   ,'pol'
   ,'pod'
   ,'eta'
   ,'etd'
   ,'transshipment_count'
   ,'transit_time'
   #,'cyclosing'
   ,'transhipments_name_1'
   ,'transhipments_name_2'
   ,'transhipments_name_3'
   ,'transhipments_name_4'
   ,'vessel'
   ]

AgGrid(df[columns], agrid_options(df[columns], 20))