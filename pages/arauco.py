import streamlit as st
import pandas as pd
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from tools.tools import list_files_s3, load_csv_s3
import altair as alt

# Config

def agrid_options(dataframe, page_size):
    grid_options_builder = GridOptionsBuilder.from_dataframe(dataframe)
    grid_options_builder.configure_pagination(enabled=True, paginationPageSize=page_size, paginationAutoPageSize=False)
    grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
    grid_options_builder.configure_grid_options(domLayout='normal')
    grid_options_builder.configure_selection("single")
    #grid_options_builder.configure_column("Entrega",
    #                    headerName="Entrega",
    #                    cellRenderer=JsCode('''function(params) {return '<a href="https://www.google.com">${params.value}</a>'}'''))
    return grid_options_builder.build()

# Data

@st.cache_data
def load_current_data() -> pd.DataFrame:
    data = pd.read_csv(f"https://klog.metabaseapp.com/public/question/c2c3c38d-e8e6-4482-ab6c-33e3f9317cce.csv")
    return data

@st.cache_data
def load_historic_data(datetime) -> pd.DataFrame:
    data = load_csv_s3("klog-lake","raw/arauco_snapshots/",f"{datetime.strftime('%Y%m%d')}-arauco_snapshot.csv")
    return data

# Plots

def plot_errors_per_envio(data):
    source = pd.DataFrame(data).T.reset_index().rename(columns={"index":"Envío de datos"})
    source = source.reset_index().rename(columns={"index":"N"})
    source["Fecha"] = [datetime(2023,3,31),datetime(2023,4,7),datetime(2023,4,14),datetime(2023,4,21),datetime(2023,4,28)]
    
    print(source)
    plot = alt.Chart(source).mark_point().encode(
        x=alt.X("Fecha",title="Envío de datos"),
        y=alt.Y("percent",title="Porcentaje de entregas con comentarios")
    )
    return plot

# App

st.set_page_config(layout="wide")

st.write("# Calidad de datos Arauco")

col1, col2 = st.columns([1,1])

with col1:
    # Choose between current data or historic
    historic_data = list_files_s3("klog-lake","raw/arauco_snapshots/")
    historic_data_choice = [datetime.strptime(f.split("/")[-1].split("-")[0],"%Y%m%d") for f in historic_data[1:]]
    data_source = st.selectbox("Fuente de datos", ["Actual"]+historic_data_choice)
    if data_source == "Actual":
        data_quality_wide = load_current_data()
    else:
        data_quality_wide = load_historic_data(data_source)

with col2:
    # Create selectbox with Envío de datos
    envios_de_datos = ["Todos"] + data_quality_wide[["Envío de datos"]].drop_duplicates()["Envío de datos"].dropna().tolist()
    selected_envio_de_datos = st.selectbox("Envío de datos", envios_de_datos)
    
# Get all columns that start with W., as they represent the problems.
problem_columns = [col for col in data_quality_wide.columns if col.startswith("W.")]

# Choose which problems to show
problems_selected = st.multiselect("Problemas", problem_columns)

# Filtered
data_quality_wide_filtered = data_quality_wide.copy()

# Filter the data based on the Envío de datos selected
if selected_envio_de_datos != "Todos":
    data_quality_wide_filtered = data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == selected_envio_de_datos]

# Filter the data based on the problems selected.
if problems_selected:
    for problem in problems_selected:
        data_quality_wide_filtered = data_quality_wide_filtered.loc[lambda x: x[problem] == 1]

documentation = {"W. ETD en el pasado sin ATD": "La fecha estimada de salida (ETD) es anterior a la fecha actual, y todavía no hay ATD.",
                 "W. Con ATA, pero no Finalizado o Arribado": "El estado del embarque no es coherente con el hecho de que exista una fecha de arribo (ATA)."}

tab1, tab2, tab3 = st.tabs(["Resumen", "Evolucion", "Entregas"])

with tab1:

    # Show the total number of rows with at least one problem.
    entregas_total = data_quality_wide.count()[0]
    # Get how many rows have at least one problem.
    total_problems = data_quality_wide_filtered[problem_columns].any(axis=1)
    st.write(f"**Entregas**: {entregas_total}")
    st.write(f"**Entregas con comentarios**: {total_problems.sum()} ({round((total_problems.sum()*1.0)/entregas_total*1000.0)/10.0}%)")

    # Get how many times each problem appears.
    problem_counts = data_quality_wide_filtered[problem_columns].sum()

    # Transform the counts into a dataframe.
    if problems_selected:
        problem_counts = pd.DataFrame(problem_counts[problems_selected], columns=["Entregas"])
    else:
        problem_counts = pd.DataFrame(problem_counts, columns=["Entregas"])

    for envio_de_datos in envios_de_datos:
        if envio_de_datos == "Todos":
            continue
        problem_counts[envio_de_datos] = data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == envio_de_datos][problem_columns].sum()

    problem_counts = problem_counts.reset_index().rename(columns={"index":"Problema"})

    AgGrid(problem_counts, agrid_options(problem_counts, 60), columns_auto_size_mode=1)
    #st.table(problem_counts)

with tab2:

    # Compuute percent of problems for all envios de datos
    data_per_envio = {}
    for envio_de_datos in envios_de_datos:
        if envio_de_datos == "Todos":
            continue
        data_per_envio[envio_de_datos] = {}
        data_per_envio[envio_de_datos]["total"] = data_quality_wide.loc[lambda x: x["Envío de datos"] == envio_de_datos].count()[0]
        if problems_selected:
            ps = problems_selected
        else:
            ps = problem_columns
        data_per_envio[envio_de_datos]["errors"] = data_quality_wide.loc[lambda x: x["Envío de datos"] == envio_de_datos][ps].any(axis=1).sum()
        data_per_envio[envio_de_datos]["percent"] = data_per_envio[envio_de_datos]["errors"]/data_per_envio[envio_de_datos]["total"]

    st.altair_chart(plot_errors_per_envio(data_per_envio))

with tab3:

    columns_default = ["Entrega","Estado","MBL","Contenedor","Cliente"]
    columns = st.multiselect("Columnas", data_quality_wide_filtered.columns, default=columns_default)

    if problems_selected:
        if len(problems_selected) == 1 and problems_selected[0] in ["W. Gran error ETA TR1 - ATA TR1"]:
            columns.append("TR1 ETA")
            columns.append("TR1 ATA")

    data_quality_main = data_quality_wide_filtered[columns]
    selected_problem_rows = AgGrid(data_quality_main, agrid_options(data_quality_main, 30), columns_auto_size_mode=1, allow_unsafe_jscode=1, allow_unsafe_html=1)