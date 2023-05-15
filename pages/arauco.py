import streamlit as st
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from tools.tools import list_files_s3, load_csv_s3, agrid_options
import altair as alt
import components

# Data

@st.cache_data
def load_data_quality() -> pd.DataFrame:
    data = pd.read_csv(f"https://klog.metabaseapp.com/public/question/c2c3c38d-e8e6-4482-ab6c-33e3f9317cce.csv")
    return data

@st.cache_data
def load_data_quality_historic(datetime) -> pd.DataFrame:
    data = load_csv_s3("klog-lake","raw/arauco_snapshots/",f"{datetime.strftime('%Y%m%d')}-arauco_snapshot.csv")
    return data

# Plots

def plot_errors_per_envio(data):
    source = pd.DataFrame(data).T.reset_index().rename(columns={"index":"Envío de datos"})
    source = source.reset_index().rename(columns={"index":"N"})
    source["Fecha"] = [datetime(2023,3,31),datetime(2023,4,7),datetime(2023,4,14),datetime(2023,4,21),datetime(2023,4,28),datetime(2023,5,5)]
    plot = alt.Chart(source).mark_point().encode(
        x=alt.X("Fecha",title="Envío de datos"),
        y=alt.Y("percent",title="Porcentaje de entregas con comentarios")
    )
    return plot

# App

st.set_page_config(layout="wide")

if 'problems_selected_in_table' not in st.session_state:
    st.session_state.problems_selected_in_table = []

st.write("# Calidad de datos Arauco")

# Filters

col1_a, col2_a = st.columns([1,1])

with col1_a:
    # Choose between current data or historic
    historic_data = list_files_s3("klog-lake","raw/arauco_snapshots/")
    historic_data_choice = [datetime.strptime(f.split("/")[-1].split("-")[0],"%Y%m%d") for f in historic_data[1:]]
    data_source = st.selectbox("Fuente de datos", ["Actual"]+historic_data_choice)
    if data_source == "Actual":
        data_quality_wide = load_data_quality()
    else:
        data_quality_wide = load_data_quality_historic(data_source)

with col2_a:
    # Create selectbox with Envío de datos
    envios_de_datos = ["Todos"] + data_quality_wide[["Envío de datos"]].drop_duplicates()["Envío de datos"].dropna().tolist()
    selected_envio_de_datos = st.selectbox("Envío de datos", envios_de_datos)

col1_b, col2_b = st.columns([1,1])

with col1_b:

    # Select entregas to show
    entregas_selected = st.multiselect("Entregas", data_quality_wide["Entrega"].drop_duplicates().tolist())    

with col2_b:

    # Get all columns that start with W., as they represent the problems.
    problem_columns = [col for col in data_quality_wide.columns if col.startswith("W.")]

    # Choose which problems to show
    problems_selected = st.multiselect("Problemas", problem_columns, default=st.session_state.problems_selected_in_table)

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
    entregas_total_filtered = data_quality_wide_filtered.count()[0]

    # Print the number of Entregas
    st.write(f"**Entregas**: {entregas_total} / {entregas_total_filtered}")
    
    # Print the number of Entregas with problems
    entregas_with_problems = data_quality_wide_filtered[problem_columns].any(axis=1).sum()
    st.write(f"**Entregas con comentarios**: {entregas_with_problems} ({round(entregas_with_problems*1.0/entregas_total*1000.0)/10.0}%)")

    # Get how many times each problem appears.
    problem_counts = data_quality_wide_filtered[problem_columns].sum()

    # Transform the counts into a dataframe.
    if problems_selected:
        problem_counts = pd.DataFrame(problem_counts[problems_selected], columns=["Entregas"])
    else:
        problem_counts = pd.DataFrame(problem_counts, columns=["Entregas"])

    # Compute sums for each problem of each envío de datos.
    for envio_de_datos in envios_de_datos:
        if envio_de_datos == "Todos":
            continue
        problem_counts[envio_de_datos] = data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == envio_de_datos][problem_columns].sum()

    problem_counts = problem_counts.reset_index().rename(columns={"index":"Problema"})

    problems_selected_in_table = AgGrid(problem_counts, agrid_options(problem_counts, 60), fit_columns_on_grid_load=True)
    if problems_selected_in_table:
        st.session_state.problems_selected_in_table = [problem_selected["Problema"] for problem_selected in problems_selected_in_table["selected_rows"]]

with tab2:

    # Compute percent of problems for all envios de datos
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

    # Select columns based on the problem selected
    data_quality_columns_from_problem = []
    if problems_selected:
        if len(problems_selected) == 1:
            if problems_selected[0] in ["W. Gran error ETA TR1 - ATA TR1"]:
                data_quality_columns_from_problem.append("TR1 ETA")
                data_quality_columns_from_problem.append("TR1 ATA")
            elif problems_selected[0] in ["W. Port TS1 = Port TS2"]:
                data_quality_columns_from_problem.append("TR1 Puerto")
                data_quality_columns_from_problem.append("TR2 Puerto")

    # Filter columns to show
    data_quality_columns_default = ["subscriptionId","Entrega","Estado","MBL","Contenedor","Cliente"] + data_quality_columns_from_problem
    data_quality_columns = st.multiselect("Columnas", data_quality_wide_filtered.columns, default=data_quality_columns_default)

    # Show the data
    data_quality_main = data_quality_wide_filtered[data_quality_columns]

    selected_entregas = AgGrid(data_quality_main, agrid_options(data_quality_main, 30), columns_auto_size_mode=1, allow_unsafe_jscode=1, allow_unsafe_html=1)

    if selected_entregas and len(selected_entregas["selected_rows"])>0:
        selected_entrega = selected_entregas["selected_rows"][0]["Entrega"]
        selected_mbl = selected_entregas["selected_rows"][0]["MBL"]
        selected_subscription_id = selected_entregas["selected_rows"][0]["subscriptionId"]
        st.session_state.selected_entrega = selected_entrega
        st.session_state.mbl = selected_mbl
        st.session_state.selected_subscription_id = selected_subscription_id

        components.show_shipment_prisma(selected_subscription_id, rows_to_highlight=["TR1 Puerto","TR2 Puerto"])