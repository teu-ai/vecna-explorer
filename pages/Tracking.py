import streamlit as st
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from tools.tools import list_files_s3, load_csv_s3, agrid_options, agrid_options_raw, setup_ambient
import altair as alt
import components
import load

# Comment: change line below to submit changes to github
# Commited on 201306301000

ARAUCO = True
AMBIENT = st.secrets['ambient']
ENVIOS = [
    'Envío 1',
    'Envío 2',
    'Envío 3',
    'Envío 4',
    'Envío 5',
    'Envío 6',
    'Envío 7',
    'Envío 8',
    'Envío 9',
    'Envío 10',
    'Envío 11',
    'Envío 12',
    'Envío 13',
    'Envío 14',
    'Envío 15',
    'Envío 16',
    'Envío 17',
    'Envío 18',
    'Envío 19',
    'Envío 47',
    'Envío CONT',
    'Envío 50',
    'Envío 51',
    'Envío 52',
    'Envío 53',
    'Envío 54',
    'Envío 55',
    'Envío 56',
    'Envío 57',
    'Envío 1_ER',
    'Envío 2_ER',
    'Envío 3_ER',
    'Envío 4_ER',
    'Envío 5_ER',
    'Envío 6_ER',
]
PROBLEMS_TO_IGNORE = [
    "W. Sin BL",
    "W. Sin contenedor",
    "W. Iniciando",
    "W. No tiene suscripción",
    "W. ATD e Iniciando",
    "W. Sin ATA ni ETA"
]
PROBLEMS_TRANSSHIPMENT_COMPLETENESS = [
    "W. TR1 sin ATA, pero con ATD",
    "W. TR2 sin ATA, pero con ATD",
    "W. TR3 sin ATA, pero con ATD",
    "W. TR4 sin ATA, pero con ATD",
    "W. TR1 sin ATD, pero TR2 con ATA",
    "W. TR2 sin ATD, pero TR3 con ATA",
    "W. TR3 sin ATD, pero TR4 con ATA",
    "W. TR1 sin ATD, pero Arribado",
    "W. TR2 sin ATD, pero Arribado",
    "W. TR3 sin ATD, pero Arribado",
    "W. TR4 sin ATD, pero Arribado",
]
PROBLEMS_INLAND_COMPLETENESS = [
    "W. Inland sin ATA, pero con ATD",
    "W. Inland sin Descarga, pero con ATD",
    "W. Inland sin ATD, Finalizado",
    "W. Inland incompleto, Finalizado"
    # "W. Inland incompleto, No finalizado"
]

setup_ambient()

# Plots

def plot_errors_per_envio(data):
    source = pd.DataFrame(data).T.reset_index().rename(columns={"index":"Envío de datos"})
    source = source.reset_index().rename(columns={"index":"N"})
    source["Fecha"] = [
        datetime(2023,3,31),
        datetime(2023,4,7),
        datetime(2023,4,14),
        datetime(2023,4,21),
        datetime(2023,4,28),
        datetime(2023,5,5),
        datetime(2023,5,19),
        datetime(2023,5,19),
        datetime(2023,5,26),
        datetime(2023,6,1),
        datetime(2023,6,12),
        datetime(2023,6,19),
        datetime(2023,6,30),
        datetime(2023,6,30),
        datetime(2023,7,12),
        datetime(2023,7,18),
        datetime(2023,7,24),
        datetime(2023,7,28),
        datetime(2023,8,4),
        datetime(2023,11,20),
        datetime(2024,1,5),
        datetime(2024,4,5),
        datetime(2024,4,12),
        datetime(2024,4,23),
        datetime(2024,4,29),
        datetime(2024,5,6),
        datetime(2024,5,8),
        datetime(2024,5,10),
        datetime(2024,5,20),
        datetime(2024,7,26),
        datetime(2024,8,1),
        datetime(2024,8,12),
        datetime(2024,8,20),
        datetime(2024,8,27),
        datetime(2024,8,30),
    ]
    plot = alt.Chart(source).mark_point().encode(
        x=alt.X("Fecha",title="Envío de datos"),
        y=alt.Y("percent",title="Porcentaje de entregas con comentarios")
    )
    return plot

# App

st.set_page_config(layout="wide", page_title = "Calidad de datos")

st.write("# Calidad de datos Arauco")

# Filters

col1_a, col2_a = st.columns([1,2])

# Choose between current data / historic
with col1_a:
    historic_data = list_files_s3("klog-lake","raw/arauco_snapshots/")
    historic_data_choice = [datetime.strptime(f.split("/")[-1].split("-")[0],"%Y%m%d") for f in historic_data[1:]]
    data_source = st.selectbox(
        "Fuente de datos",
        ["Actual"]+historic_data_choice,
        help="Fuente actual corresponde a datos en tiempo real; en los otros casos se trata de un snapshot de la base de datos en el pasado.")
    
    if data_source == "Actual":
        data_quality_wide = load.load_data_quality(client="Arauco")
    else:
        data_quality_wide = load.load_data_quality_historic(data_source, client="Arauco")

    data_quality_wide = data_quality_wide.loc[lambda x: x["Envío de datos"].apply(lambda y: y in ENVIOS)]


# Create selectbox with Envío de datos
envios_de_datos_default = [f"Envío {i}" for i in range(50, 58)] + ['Envío 1_ER', 'Envío 2_ER', 'Envío 3_ER', 'Envío 4_ER', 'Envío 5_ER', 'Envío 6_ER']
with col2_a:
    # envios_de_datos = data_quality_wide[["Envío de datos"]].drop_duplicates()["Envío de datos"].dropna().tolist()    
    envios_de_datos = ENVIOS
    selected_envios_de_datos = st.multiselect(
        "Envíos de datos",
        envios_de_datos,
        default=envios_de_datos_default,
        help="Un Envío de dato corresponde a un conjunto de datos que se envía a KLog.co desde Arauco.")

# Select entregas to show
entregas_selected = st.multiselect("Entregas", data_quality_wide["Entrega"].drop_duplicates().tolist(), help="La Entrega corresponde al registro interno de Arauco para un embarque.")

# Select warnings to show or ignore
col1_c, col2_c = st.columns([1,1])

# Compute and filter out errors
# considerar_entregas_con_errores = st.checkbox("Considerar entregas con errores", value=True, help="Si se desactiva, se considerarán sólo las entregas que no tengan errores.")
sin_entregas_con_errores = st.checkbox("Sin entregas con errores de suscripción", value=False, help="Si se activa, se omiten entregas con contenedores no reportados por navieras, etc.")

sin_msc = st.checkbox("Sin MSC", value=False, help="Sin MSC.")
if sin_msc:
    data_quality_wide = data_quality_wide.loc[lambda x: x["Naviera"] != 'MSC']

cols_sin_tsp_completeness = [x for x in data_quality_wide.columns if x not in PROBLEMS_TRANSSHIPMENT_COMPLETENESS]
sin_tsp_completeness = st.checkbox("Sin errores de completitud en transbordos", value=True, help="Errores de completitud en transbordos.")
if sin_tsp_completeness:
    data_quality_wide = data_quality_wide[cols_sin_tsp_completeness]

cols_sin_inland_completeness = [x for x in data_quality_wide.columns if x not in PROBLEMS_INLAND_COMPLETENESS]
sin_inland_completeness = st.checkbox("Sin errores de completitud en inland", value=True, help="Errores de completitud en inland.")
if sin_inland_completeness:
    data_quality_wide = data_quality_wide[cols_sin_inland_completeness]

# Not subscribed
data_quality_wide_not_subscribed = data_quality_wide.loc[lambda x: x["W. No tiene suscripción"] == 1]
entregas_not_subscribed = data_quality_wide_not_subscribed["Entrega"].unique().tolist()
if sin_entregas_con_errores:
    data_quality_wide = data_quality_wide.loc[lambda x: x["Entrega"].apply(lambda y: y not in entregas_not_subscribed)]

# Container not in BL
containers_by_subscription = load.load_containers_by_subscription("prod")
containers_by_subscription = containers_by_subscription.groupby('subscription_id')["vecna_event_container"].apply(list).to_dict()
containers_not_in_subscriptions = data_quality_wide.loc[lambda x: x.apply(lambda y: y["Contenedor"] not in containers_by_subscription.get(str(y["subscriptionId"]),[]),axis=1)].copy()
if sin_entregas_con_errores:
    c = containers_not_in_subscriptions["Contenedor"].unique().tolist()
    data_quality_wide = data_quality_wide.loc[lambda x: x["Contenedor"].apply(lambda y: y not in c)]

# For testing
# data_quality_wide.loc[lambda x: x["MBL"] == "MEDUD9458693", ["MBL","Contenedor","Envío de datos","subscriptionId"]]

# Global ignore of warnings
columns = [x for x in data_quality_wide.columns if x not in PROBLEMS_TO_IGNORE]
data_quality_wide = data_quality_wide[columns]

# Get all columns that start with W., as they represent the problems.
problem_columns_all = [col for col in data_quality_wide.columns if col.startswith("W.")]

with col1_c:

    # Choose which problems to show
    problems_selected = st.multiselect(
        "Comentarios",
        problem_columns_all,
        help="Un Comentario es una observación sobre coherencia y completitud de los datos.")

with col2_c:

    # Comentarios a ignorar
    problems_ignore_selected = st.multiselect(
        "Comentarios a ignorar",
        problem_columns_all,
        #default=["W. Sin POD Descarga estimada"],
        help="Un Comentario es una observación sobre coherencia y completitud de los datos.")

if problems_selected:
    problem_columns = problems_selected
problem_columns = [col for col in problem_columns_all if col not in problems_ignore_selected]

# Filtered

problem_columns_categories_map = {
    "1. Zarpe POL":[
        "W. Sin ETD",
        "W. ETD en el pasado sin ATD",
        "W. Sin ATD y ya zarpó",
        "W. ATD >= ETA"],
    "2. Trasbordo":[
        "W. TR1 sin ETA",
        "W. Gran error ETA TR1 - ATA TR1",
        "W. ETA TR1 = ETD Total",
        "W. ETA TR1 < ETD Total",
        "W. ETA TR1 = ETA Total",
        "W. ETA TR1 > ETA Total",
        "W. TR2 sin ETA",
        "W. Gran error ETA TR2 - ATA TR2",
        "W. ETA TR2 = ETD Total",
        "W. ETA TR2 < ETD Total",
        "W. ETA TR2 = ETA Total",
        "W. ETA TR2 > ETA Total",
        "W. TR3 sin ETA",
        "W. Gran error ETA TR3 - ATA TR3",
        "W. ETA TR3 = ETD Total",
        "W. ETA TR3 < ETD Total",
        "W. ETA TR3 = ETA Total",
        "W. ETA TR3 > ETA Total",
        "W. TR4 sin ETA",
        "W. Gran error ETA TR4 - ATA TR4",
        "W. ETA TR4 = ETD Total",
        "W. ETA TR4 < ETD Total",
        "W. ETA TR4 = ETA Total",
        "W. ETA TR4 > ETA Total",
        "W. Atasco Transbordo",
        "W. TS1 = TS2",
        "W. TS2 = TS3",
        "W. TS3 = TS4",
        "W. TS2 < TS1",
        "W. TS3 < TS2",
        "W. TS4 < TS3",
        "W. Port TS1 = Port TS2",
        "W. Port TS2 = Port TS3",
        "W. Port TS3 = Port TS4"
    ],
    "2.1 Trasbordo Completitud":[
        "W. TR1 sin ATA, pero con ATD",
        "W. TR2 sin ATA, pero con ATD",
        "W. TR3 sin ATA, pero con ATD",
        "W. TR4 sin ATA, pero con ATD"
    ],
    "2.2 Trasbordo Completitud":[
        "W. TR1 sin ATD, pero TR2 con ATA",
        "W. TR2 sin ATD, pero TR3 con ATA",
        "W. TR3 sin ATD, pero TR4 con ATA"
    ],
    "2.3 Trasbordo Completitud":[
        "W. TR1 sin ATD, pero Arribado",
        "W. TR2 sin ATD, pero Arribado",
        "W. TR3 sin ATD, pero Arribado",
        "W. TR4 sin ATD, pero Arribado"
    ],
    # "2.3 Trasbordo Completitud":[
    #     "W. Transbordo final TR1 sin ATD, pero Arribado",
    #     "W. Transbordo final TR2 sin ATD, pero Arribado",
    #     "W. Transbordo final TR3 sin ATD, pero Arribado",
    #     "W. Transbordo final TR4 sin ATD, pero Arribado"
    # ],
    "3. Fecha de llegada POD":[
        "W. Sin ETA",
        "W. ETD >= ETA",
        "W. ETA en el pasado sin ATA",
        "W. Con ATA, pero no Finalizado o Arribado"
    ],
    "4. Descarga POD":[
        "W. Sin POD Descarga, Finalizado",
        "W. Sin POD Descarga estimada",
        "W. POD Descarga < ATA"
    ],
    # "5. Out of gate POD":[

    # ],
    "5. Completitud Inland":[
        "W. Inland sin ATA, pero con ATD",
        "W. Inland sin Descarga, pero con ATD",
        "W. Inland sin ATD, Finalizado",
        "W. Inland incompleto, Finalizado",
        # "W. Inland incompleto, No finalizado"
    ],
    "6. Empty return":[
        "W. Sin devolución, Finalizado",
        "W. Devuelto vacío < POD Descarga"
    ],
    "Otros comentarios":[
        "W. Sin POL",
        "W. Sin POD",
        "W. POL = POD",
        "W. Sin nave",
        "W. Sin viaje",
        "W. Sin naviera",
    ],
    "Total":problem_columns
}

if sin_tsp_completeness:
    del(problem_columns_categories_map["2.1 Trasbordo Completitud"])
    del(problem_columns_categories_map["2.2 Trasbordo Completitud"])
    del(problem_columns_categories_map["2.3 Trasbordo Completitud"])

if sin_inland_completeness:
    del(problem_columns_categories_map["5. Completitud Inland"])

problem_columns_categories = problem_columns_categories_map.keys()
problem_columns_categories_list = {v:k for k in problem_columns_categories_map for v in problem_columns_categories_map[k] if k != "Total"}

for k, v in problem_columns_categories_map.items():
    data_quality_wide[k] = data_quality_wide[v].sum(axis=1)

data_quality_wide_filtered = data_quality_wide.copy()

# Filter the data based on the Envío de datos selected
if selected_envios_de_datos:
    data_quality_wide_filtered = data_quality_wide_filtered.loc[lambda x: x.apply(lambda y: y["Envío de datos"] in selected_envios_de_datos, axis=1)]

# Filter the data based on the problems selected.
if problems_selected:
    for problem in problems_selected:
        data_quality_wide_filtered = data_quality_wide_filtered.loc[lambda x: x[problem] == 1]

# Filter the data based on the entrega selected.
if entregas_selected:
    data_quality_wide_filtered = data_quality_wide_filtered.loc[lambda x: x["Entrega"].apply(lambda y: y in entregas_selected)]

for k, v in problem_columns_categories_map.items():
    data_quality_wide_filtered[k] = data_quality_wide_filtered[v].sum(axis=1)

documentation = {"W. ETD en el pasado sin ATD": "La fecha estimada de salida (ETD) es anterior a la fecha actual, y todavía no hay ATD.",
                 "W. Con ATA, pero no Finalizado o Arribado": "El estado del embarque no es coherente con el hecho de que exista una fecha de arribo (ATA)."}

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



# Main

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Resumen", "Errores", "Detalle", "Análisis", "Contenedores"])

with tab1:

    # Summary table
    #def summary_table():
    entregas_total = data_quality_wide.count()[0]
    entregas_total_filtered = data_quality_wide_filtered.count()[0]

    entregas_with_problems = data_quality_wide[problem_columns].any(axis=1).sum()
    entregas_with_problems_filtered = data_quality_wide_filtered[problem_columns].any(axis=1).sum()

    problems_total = data_quality_wide[problem_columns].sum().sum()
    problems_total_filtered = data_quality_wide_filtered[problem_columns].sum().sum()

    df = pd.DataFrame([
            [entregas_total, entregas_total_filtered],
            [entregas_with_problems, entregas_with_problems_filtered],
            [round(entregas_with_problems*1.0/entregas_total*1000.0)/10.0 if entregas_total != 0 else 0,
                round(entregas_with_problems_filtered*1.0/entregas_total_filtered*1000.0)/10.0 if entregas_total_filtered != 0 else 0],
        ],columns=["Total","Filtradas"],
        index=["Contenedores","Contenedores con comentarios","% Contenedores con comentarios"])

    st.write("**Tabla resumen**")

    st.dataframe(df)
    
    #summary_table()

    # Get how many times each problem appears.
    problem_counts = data_quality_wide_filtered[problem_columns].sum()
    problem_categories_counts = data_quality_wide_filtered[problem_columns_categories].sum()

    entregas_with_problems_counts = data_quality_wide_filtered[problem_columns].sum()
    entregas_with_problems_categories_counts = (data_quality_wide_filtered[problem_columns_categories] >= 1).sum()
    
    # Transform the counts into a dataframe.
    if problems_selected:
        problem_counts = pd.DataFrame(problem_counts[problems_selected], columns=["Contenedores"])
    else:
        problem_counts = pd.DataFrame(problem_counts, columns=["Contenedores"])

    problem_categories_counts = pd.DataFrame(problem_categories_counts, columns=["Comentarios"])
    entregas_with_problems_categories_counts = pd.DataFrame(entregas_with_problems_categories_counts, columns=["Contenedores"])

    # Compute sums for each problem of each envío de datos.
    for envio_de_datos in selected_envios_de_datos:
        if envio_de_datos == "Todos":
            continue
        problem_counts[envio_de_datos] = data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == envio_de_datos][problem_columns].sum()
        problem_categories_counts[envio_de_datos] = data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == envio_de_datos][problem_columns_categories].sum()
        entregas_with_problems_categories_counts[envio_de_datos] = (data_quality_wide_filtered.loc[lambda x: x["Envío de datos"] == envio_de_datos][problem_columns_categories] >= 1).sum()

    problem_counts = problem_counts.reset_index().rename(columns={"index":"Comentario"})
    problem_categories_counts = problem_categories_counts.reset_index().rename(columns={"index":"Categoria"})
    entregas_with_problems_categories_counts = entregas_with_problems_categories_counts.reset_index().rename(columns={"index":"Categoria"})

    tab11, tab21 = st.tabs(["Contenedores", "Comentarios"])

    with tab11:

        st.write("**Cantidad de contenedores con al menos un comentario, por etapa**")

        AgGrid(entregas_with_problems_categories_counts, agrid_options(entregas_with_problems_categories_counts, 60), fit_columns_on_grid_load=True)

        st.write("**Porcentaje de contenedores con al menos un comentario, por etapa**")

        for column in entregas_with_problems_categories_counts.columns:
            if column == "Categoria":
                continue
            if column == "Contenedores":
                entregas_with_problems_categories_counts[column] = entregas_with_problems_categories_counts[column]/(entregas_total_filtered*1.0)
            else:
                entregas_with_problems_categories_counts[column] = entregas_with_problems_categories_counts[column]/(data_per_envio[column]["total"]*1.0)

        grid_options_builder = agrid_options_raw(entregas_with_problems_categories_counts, 60)
        for column in entregas_with_problems_categories_counts.columns:
            if column == "Categoria":
                continue
            grid_options_builder.configure_column(column, valueGetter=f"(data['{column}']*100).toFixed(1) + '%'")
        AgGrid(entregas_with_problems_categories_counts, grid_options_builder.build(), fit_columns_on_grid_load=True)

    with tab21:

        st.write("**Cantidad de comentarios, por etapa**")

        AgGrid(problem_categories_counts, agrid_options(problem_categories_counts, 60), fit_columns_on_grid_load=True)

        st.write("**Porcentaje de comentarios por etapa sobre total de comentarios**")

        for column in problem_categories_counts.columns:
            if column == "Categoria":
                continue
            if column == "Comentarios":
                problem_categories_counts[column] = problem_categories_counts[column]/(problem_categories_counts[column].iloc[-1]*1.0)
            else:
                problem_categories_counts[column] = problem_categories_counts[column]/(problem_categories_counts[column].iloc[-1]*1.0)

        grid_options_builder = agrid_options_raw(problem_categories_counts, 60)
        for column in problem_categories_counts.columns:
            if column == "Categoria":
                continue
            grid_options_builder.configure_column(column, valueGetter=f"(data['{column}']*100).toFixed(1) + '%'")
        AgGrid(problem_categories_counts, grid_options_builder.build(), fit_columns_on_grid_load=True)

with tab2:

    st.write("A continuación se muestran las entregas con errores críticos, es decir, aquellos que no permiten obtener información de las entregas.")

    st.write("**Subscription status = 0**")

    AgGrid(data_quality_wide_not_subscribed[["Entrega","MBL","Contenedor","Envío de datos"]], agrid_options(data_quality_wide_not_subscribed[["Entrega","MBL","Contenedor","Envío de datos"]], 60))

    st.write("**Contenedor en prisma no presente en eventos**")

    AgGrid(containers_not_in_subscriptions[["Entrega","MBL","Contenedor","Envío de datos"]], agrid_options(containers_not_in_subscriptions[["Entrega","MBL","Contenedor","Envío de datos"]], 60))

with tab3:

    st.write("**Top comentarios**")

    problem_counts["Etapa"] = problem_counts["Comentario"].apply(lambda x: problem_columns_categories_list[x] if x in problem_columns_categories_list.keys() else "ETC")
    problem_counts = problem_counts[["Comentario","Etapa"]+[x for x in problem_counts.columns if x not in ["Comentario","Etapa"]]]
    top_5_problems = problem_counts.sort_values(by="Contenedores", ascending=False).head(5)

    AgGrid(top_5_problems, agrid_options(top_5_problems, 5), fit_columns_on_grid_load=True)

    st.write("Top comentarios por envío en %")
    # Totals
    total_envios_df = data_quality_wide_filtered.groupby('Envío de datos').size().to_frame().reset_index()
    total_envios_df.columns = ["Envío de datos", "Total"]
    totals_indexed = total_envios_df.set_index("Envío de datos")["Total"]

    envios = total_envios_df["Envío de datos"].tolist()
    grouped_sums = top_5_problems.groupby('Comentario')[envios].sum()

    envios_df = grouped_sums.div(totals_indexed, axis='columns').multiply(100).round(1).transpose().reset_index()
    envios_df.rename(columns={'index': 'Envío'}, inplace=True)
    envios_df = envios_df[['Envío'] + top_5_problems['Comentario'].tolist()]
    AgGrid(envios_df, agrid_options(envios_df, 10), fit_columns_on_grid_load=True)

    st.write("**Detalle por comentario y entrega**")

    problem_counts["Etapa"] = problem_counts["Comentario"].apply(lambda x: problem_columns_categories_list[x] if x in problem_columns_categories_list.keys() else "ETC")
    problem_counts = problem_counts[["Comentario","Etapa"]+[x for x in problem_counts.columns if x not in ["Comentario","Etapa"]]]

    AgGrid(problem_counts, agrid_options(problem_counts, 60), fit_columns_on_grid_load=True)

    st.write("**Detalle por entrega comentarios**")

    # Drop columns from data_quality_wide_filtered where all values are 0
    data_quality_wide_filtered_details = data_quality_wide_filtered.loc[:, (data_quality_wide_filtered != 0).any(axis=0)]
    # Select entregas that have at least one comment

    if len(data_quality_wide_filtered_details) == 0:
        st.write("Entregas filtradas no tienen comentarios")
    else:
        w_c = [x for x in data_quality_wide_filtered_details.columns.tolist() if x[0] == 'W']
        c = list(["Entrega"] + w_c)
        data_quality_wide_filtered_details = data_quality_wide_filtered_details[c]
        # Add column with total of row
        data_quality_wide_filtered_details["Total"] = data_quality_wide_filtered_details[w_c].sum(axis=1)
        data_quality_wide_filtered_details = data_quality_wide_filtered_details.loc[lambda x: x["Total"]>0]
        AgGrid(data_quality_wide_filtered_details, agrid_options(data_quality_wide_filtered_details, 20), fit_columns_on_grid_load=True)
    

with tab4:

    st.altair_chart(plot_errors_per_envio(data_per_envio))

with tab5:

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
    data_quality_columns_out = [
        "Shipment_id","Fecha_Creacion_Embarque","subscriptionId","ETD Inicial (Sch)","ETD Final (Sch)","ETD (Sch)","ATD (Sch)","TR1 ATA (M)",
        "ETD Inicial Date (Sch)","ETD Final Date (Sch)",
        "TR1 ATD (M)","TR2 ATA (M)","TR2 ATD (M)","TR3 ATA (M)","TR3 ATD (M)","TR4 ATA (M)","TR4 ATD (M)","ETA (Sch)","ETA Inicial Date (Sch)",
        "ETA Inicial (Sch)","ETA Final Date (Sch)","ETA Final (Sch)","ATA (Sch)"] + problem_columns
    data_quality_columns = [x for x in data_quality_wide_filtered.columns if x not in data_quality_columns_out]
    data_quality_columns_default = ["Entrega","Estado","MBL","Contenedor","Cliente"] + data_quality_columns_from_problem

    c1, c2 = st.columns([1,3])
    with c1:
        all_errors = st.checkbox("Mostrar todos los errores", value=False)
        #if all_errors:
        #    data_quality_columns_default += problem_columns
    with c2:
        data_quality_columns_selected = st.multiselect("Columnas", data_quality_columns, default=data_quality_columns_default)

    # Show the data
    data_quality_wide_filtered["Total W"] = data_quality_wide_filtered[problem_columns].sum(axis=1)
    data_quality_main = data_quality_wide_filtered[data_quality_columns_selected+["Total W"]]
    # Sum over all columns that start with W

    selected_entregas = AgGrid(data_quality_main, agrid_options(data_quality_main, 15), columns_auto_size_mode=1, allow_unsafe_jscode=1, allow_unsafe_html=1)

    if selected_entregas and len(selected_entregas["selected_rows"])>0:
        selected_entrega = selected_entregas["selected_rows"][0]["Entrega"]
        selected_mbl = selected_entregas["selected_rows"][0]["MBL"]
        selected_container = selected_entregas["selected_rows"][0]["Contenedor"]
        selected_subscription_id = str(data_quality_wide_filtered.loc[lambda x: x["MBL"] == selected_mbl, "subscriptionId"].values[0])
        st.session_state.selected_entrega = selected_entrega
        st.session_state.mbl = selected_mbl
        st.session_state.selected_subscription_id = selected_subscription_id

        if ARAUCO:
        
            events_rows = load.load_events_vecna("mbl",selected_mbl,"prod")
            
            # Vecna S3
            if events_rows["raw_event_oi"].values[0] and events_rows["raw_event_oi"].values[0] != "subscription":
                event_raw = load.load_event_raw(events_rows["raw_event_oi"].values[0], "oceaninsights/")
            else:
                event_raw = {}
            if events_rows["raw_event_gh"].values[0] and events_rows["raw_event_gh"].values[0] != "subscription":
                event_raw = load.load_event_raw(events_rows["raw_event_gh"].values[0], "ghmaritime/")
            else:
                event_raw = {}
            
            if AMBIENT == "dev":
                components.show_data_sources(selected_entregas, selected_subscription_id, vecna_dynamo=False, events_s3=True, event_raw=event_raw)
            elif AMBIENT == "prod":
                components.show_data_sources(selected_entregas, selected_subscription_id, vecna_dynamo=False, events_s3=False, event_raw=event_raw)

        if not ARAUCO:

            components.show_shipment_prisma(selected_subscription_id, rows_to_highlight=["TR1 Puerto","TR2 Puerto"])
