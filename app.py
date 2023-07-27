# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.
#uninstall la version actual de flask
#install dash
#instal pandas
#install pyodbc
#install sqlalchemy
#install plotly
#install python-dotenv
#install matplotlib
#install numpy

from dash import Dash, html, dash_table, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import sqlalchemy as db
import urllib
from sqlalchemy import create_engine
import pyodbc
import datetime as dt
####componentes para hacer la conexió con azure sql
import plotly.graph_objects as go
import numpy as np
import os #requests, os, uuid, json
from dotenv import load_dotenv
import urllib
from sqlalchemy import create_engine
load_dotenv()
driver = os.environ['DRIVER']
server = os.environ['SERVER']
database = os.environ['DATABASE']
user = os.environ['USER']
password = os.environ['PASSWORD']

conn = f"""Driver={driver};Server=tcp:{server},1433;Database={database};
Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""

params = urllib.parse.quote_plus(conn)
conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
engine = create_engine(conn_str, echo=True)

with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM dbo.vista_ejecucion_dashboard();", connection)
        df_desc_tipo_ejecucion = pd.read_sql("SELECT desc_tipo_ejecucion from tipo_ejecucion;", connection)
        df_desc_subproyecto = pd.read_sql("SELECT desc_subproyecto from subproyecto;", connection)
        df_desc_fase = pd.read_sql("SELECT desc_fase from fase;", connection)
        df_fecha_ejecucion = pd.read_sql("""SELECT datetrunc(day,getdate()-4), datetrunc(day,getdate()-3),datetrunc(day,getdate()-2),datetrunc(day,getdate()-1),
datetrunc(day,getdate())""" , connection)#"SELECT CONVERT(varchar, ejecucion_estatus.fecha_ejecucion, 126) as fecha_ejecucion from ejecucion_estatus;"
        df_fecha_ejecucion = df_fecha_ejecucion.dropna(how='all')

# Initialize the app - incorporate css
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.layout=html.Div([
     html.Div(className='row', children='Dashboard Nebula tiempos de ejecución',
             style={'textAlign': 'left', 'color': '#D01E76', 'fontSize': 30})
        ,
    html.Div([
            html.Header("Selecciona la fecha que deseas revisar"),

            dcc.Dropdown(
                df_fecha_ejecucion.T[0].unique(),
                df_fecha_ejecucion.T[0].iloc[-1],
                id='fecha_ejecucion'
            ),
    ]),
html.Div(
   [ html.Div(id = 'indicador_tiempo_promedio_ejecucion', style={'width': '33.20%', 'float': 'left', 'display': 'inline-block', "border":"1px solid lightgray" }
        ), 
        html.Div(id='indicador_total_subproyectos', style={'width': '33.27%', 'float': 'center', 'display': 'inline-block',"border":"1px solid lightgray"}
        ),
        html.Div(id = 'indicador_total_ejecuciones',style={'width': '33.20%', 'float': 'right', 'display': 'inline-block',"border":"1px solid lightgray"} 
        )] ),
html.Div( [html.Div([
        html.H6('Tiempo total de ejecución por subproyecto' ,style={'color': '#3C1053'}),
        dcc.Graph(id='tiempo_total_ejecucion'),
    ], style={'display': 'inline-block', 'width': '49%'}),
    html.Div([
        html.H6('Tiempo de ejecución por fase' ,style={'color': '#3C1053'}),
        dcc.Graph(id='serie_tiempo_ejecucion_por_fase'),
    ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'})]),
    html.Div([
            html.Header("Selecciona el tipo de carga y subproyecto"),
            dcc.Dropdown(
                df_desc_subproyecto['desc_subproyecto'].unique(),
                'Nebula',
                id='desc_subproyecto'
            ),
              dcc.Dropdown(
                df_desc_tipo_ejecucion['desc_tipo_ejecucion'].unique(),
                'Completa',
                id='tipo_ejecucion'
            ),
html.Div([
        html.H6('Tiempo de ejecución por archivo',style={'color': '#3C1053'}),
        dcc.Graph(id='serie_tiempo_ejecucion_archivo'),
    ], style={'display': 'inline-block', 'width': '49%'}),
         html.Div([ html.H6('Tabla de detalles',style={'color': '#3C1053'}),html.Div(id='tabla_vista_ejecucion') ,],
             style={'width': '49%', 'float': 'right', 'display': 'inline-block'}
             ), 
    ]),
   
    ])
#actualización gráfica tiempo total  de ejecución por subproyecto
@callback(
    Output('tiempo_total_ejecucion', 'figure'),
    Input('fecha_ejecucion', 'value'))
def update_graph(fecha_ejecucion):
    with engine.connect() as connection:
        query = f"""select * from dbo.vista_ejecucion_dashboard() as tabla where tabla.fecha_ejecucion = '{fecha_ejecucion}'"""
        df = pd.read_sql(query, connection)
    df1=df
    df3 = df1.drop_duplicates()
    df3 = df3[['id_ejecucion','id_status_subproyecto','tiempo_total_ejecucion','desc_subproyecto','desc_tipo_ejecucion']]
    df3['Subproyecto-Carga:Estatus'] = [str(df3['desc_subproyecto'].iloc[i])+'-'+str(df3['desc_tipo_ejecucion'].iloc[i]) + ':'+str(df3['id_status_subproyecto'].iloc[i]) for i in range(0, len(df3.index))]    
    df3 = df3.drop_duplicates()
    figure=px.bar(df3, x='desc_subproyecto', y='tiempo_total_ejecucion' ,labels={'desc_subproyecto':'subproyectos','tiempo_total_ejecucion':'tiempo total de ejecucion (hrs)'},color = 'Subproyecto-Carga:Estatus',barmode = 'group')
    return figure

#actualización de la gráfica tiempo por fase de cada subproyecto
@callback(
    Output('serie_tiempo_ejecucion_por_fase', 'figure'),
    Input('fecha_ejecucion', 'value'))
def create_time_series(fecha_ejecucion):
    with engine.connect() as connection:
        df = pd.read_sql(f"SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha_ejecucion}';", connection)
    df3 = df
    df3['Subproyecto-Carga'] = [str(df3['desc_subproyecto'].iloc[i])+'-' +':'+str(df3['desc_tipo_ejecucion'].iloc[i]) for i in range(0, len(df3.index))]

    figure2 = px.line(df3, x="desc_fase", y="tiempo_total_ejecucion_fase", labels={"tiempo_total_ejecucion_fase":"tiempo total de ejecucion por fase (min)"},color='Subproyecto-Carga',markers=True)
    return figure2
#actualización de la tabla por subproyecto y tipo de carga
@callback(
    Output('tabla_vista_ejecucion', 'children'),
    Input('fecha_ejecucion', 'value'),
    Input('desc_subproyecto', 'value'),
    Input('tipo_ejecucion', 'value'))
def update_table(fecha,desc_subproyecto,tipo_carga):
    with engine.connect() as connection:
        df = pd.read_sql(f"""SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha}' 
                         and desc_tipo_ejecucion = '{tipo_carga}' and desc_subproyecto = '{desc_subproyecto}'""", connection)

    df3=df[['id_ejecucion_estatus',
            'desc_tipo_ejecucion',
            'desc_subproyecto',
            'desc_fase',
            'id_status_detalle',
            'desc_detalle_fase',
            'filas_leidas',
            'filas_copiadas',
            'fecha_ejecucion',
            'hora_inicio',
            'hora_fin',
            'tiempo_ejecucion_archivo']]
    return dash_table.DataTable(data=df3.to_dict('records'),  
                                    style_data={
                                    'whidth': 'auto',
                                    'height': 'auto',
                                },
                                style_table={'overflowX': 'auto'},
                                style_header={
                                    'backgroundColor': '#3C1053',
                                    'color': 'white',
                                    'fontWeight': 'bold'
                                }, page_size=10)

#actualizar gráfica de tiempo de ejcución por archivo para una carga y subproyectos dados
@callback(
    Output('serie_tiempo_ejecucion_archivo', 'figure'),
    Input('fecha_ejecucion', 'value'), 
    Input('desc_subproyecto', 'value'),
    Input('tipo_ejecucion', 'value'))
def create_time_series_archivo(fecha_ejecucion,desc_subproyecto,tipo_ejecucion):
    with engine.connect() as connection:
        df = pd.read_sql(f"""SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha_ejecucion}'
                         and desc_subproyecto = '{desc_subproyecto}' and desc_tipo_ejecucion = '{tipo_ejecucion}';""", connection)
    df3 = df
    figure3 = px.line(df3, x="id_ejecucion_estatus", y="tiempo_ejecucion_archivo",markers=True)
    return figure3

#actualizar tarjeta de indicador del número total de subproyectos
@callback(
    Output('indicador_total_subproyectos', 'children'),
    Input('fecha_ejecucion', 'value'),)
def update_indicador_subproyectos(fecha):
    with engine.connect() as connection:
        df = pd.read_sql(f"""SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha}' 
                     """, connection)
    numero_subproyectos = len(df['desc_subproyecto'].value_counts())
    return html.Div([
            html.H6(children='Total de subproyectos',
                    style={
                        'textAlign': 'center',
                        'color': '#3C1053'}
                    ),

            html.P(f"{numero_subproyectos}",
                   style={
                       'textAlign': 'center',
                       'color': '#3C1053',
                       'fontSize': 20}
                   ),])

#actualizar tarjeta de indicador tiempo promedio por ejecución
@callback(
    Output('indicador_tiempo_promedio_ejecucion', 'children'),
    Input('fecha_ejecucion', 'value'),)
def update_indicador_tiempo_promedio_ejecucion(fecha):
    with engine.connect() as connection:
        df = pd.read_sql(f"""SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha}' 
                     """, connection)
    if np.isnan(df['tiempo_total_ejecucion'].mean()):
         tiempo_promedio_ejecucion =  '-'
    else:
        tiempo_promedio_ejecucion = df['tiempo_total_ejecucion'].mean()

    return html.Div([
            html.H6(children='Tiempo promedio de ejecución (hrs)',
                    style={
                        'textAlign': 'center',
                        'color': '#3C1053'}
                    ),

            html.P(f"{tiempo_promedio_ejecucion}",
                   style={
                       'textAlign': 'center',
                       'color': '#3C1053',
                       'fontSize': 20}
                   ),])
#actualizar tarjeta de indicador del número total de ejecuciones
@callback(
    Output('indicador_total_ejecuciones', 'children'),
    Input('fecha_ejecucion', 'value'),)
def update_indicador_total_ejecuciones(fecha):
    with engine.connect() as connection:
        df = pd.read_sql(f"""SELECT * FROM dbo.vista_ejecucion_dashboard() where fecha_ejecucion = '{fecha}' 
                     """, connection)
    total_ejecuciones = len(df['id_ejecucion'].value_counts())

    return html.Div([
            html.H6(children='Total de ejecuciones',
                    style={
                        'textAlign': 'center',
                        'color': '#3C1053'}
                    ),

            html.P(f"{total_ejecuciones}",
                   style={
                       'textAlign': 'center',
                       'color': '#3C1053',
                       'fontSize': 20}
                   ),])

if __name__ == '__main__':
    app.run(debug=True)