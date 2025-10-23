
"""
Javier Mendoza Guerrero
Bertrán Vidal Campos
"""
import dash
from dash import html, dcc, Input, Output, State
import plotly.graph_objs as go
import pymysql
from pymongo import MongoClient
from collections import Counter
from wordcloud import WordCloud
from datetime import datetime
import matplotlib.pyplot as plt
import io
import base64
from configuracion import MYSQL_CONFIG, MONGO_CONFIG

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server


# CONEXIONES

# CONFIGURACIÓN MYSQL
HOST_SQL = MYSQL_CONFIG["host"]
USUARIO_SQL = MYSQL_CONFIG["user"]
CONTRASENIA_SQL = MYSQL_CONFIG["password"]
NOMBRE_BBDD_SQL = MYSQL_CONFIG["database"]

# CONFIGURACIÓN MONGO
HOST_MONGO = MONGO_CONFIG["host"]
NOMBRE_BBDD_MONGO = MONGO_CONFIG["database"]
NOMBRE_COLECCION_MONGO = MONGO_CONFIG["collection"]


# Función para conectarse a la base de datos MySQL
def obtener_conexion():
    return pymysql.connect(
        host=HOST_SQL,
        user=USUARIO_SQL,
        password=CONTRASENIA_SQL,
        database=NOMBRE_BBDD_SQL,
        cursorclass=pymysql.cursors.DictCursor
    )

# Conexión a MongoDB
cliente = MongoClient(f"mongodb://{HOST_MONGO}")
db_mongo = cliente[NOMBRE_BBDD_MONGO]
collection = db_mongo[NOMBRE_COLECCION_MONGO]


# CONSULTAS Y FUNCIONES AUXILIARES


# Devuelve los reviewID de MongoDB asociados a un tipo de producto específico
def ids_para_mongo(tipo_producto):
    with obtener_conexion().cursor() as cursor:
        query = '''
            SELECT r.reviewID
            FROM reviews r
            JOIN products p ON r.asin = p.asin
            WHERE p.product_type = %s;
        '''
        cursor.execute(query, (tipo_producto,))
        return [row['reviewID'] for row in cursor.fetchall()]


# Devuelve la lista de ASINs para un tipo de producto dado
def obtener_asins_por_tipo(tipo_producto):
    with obtener_conexion().cursor() as cursor:
        query = '''
            SELECT asin FROM products WHERE product_type = %s
        '''
        cursor.execute(query, (tipo_producto,))
        return [row['asin'] for row in cursor.fetchall()]


# Devuelve los tipos de producto disponibles en la base de datos
def obtener_tipos_producto():
    with obtener_conexion().cursor() as cursor:
        query = '''
            SELECT DISTINCT product_type FROM products
        '''
        cursor.execute(query)
        return [row['product_type'] for row in cursor.fetchall()]
    

# Devuelve el número de reviews por año, filtrado opcionalmente por tipo de producto
def consulta_reviews_por_anio(tipo_producto=None):
    with obtener_conexion().cursor() as cursor:
        if tipo_producto:
            query = '''
                SELECT YEAR(reviewTime) AS anio, COUNT(*) AS total_reviews
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                WHERE p.product_type = %s
                GROUP BY anio
                ORDER BY anio;
            '''
            cursor.execute(query, (tipo_producto,))
        else:
            query = '''
                SELECT YEAR(reviewTime) AS anio, COUNT(*) AS total_reviews
                FROM reviews
                GROUP BY anio
                ORDER BY anio;
            '''
            cursor.execute(query)
        return cursor.fetchall()

# Devuelve los productos más populares por número de reviews, con filtro opcional por tipo

def consulta_popularidad(tipo_producto=None):
    with obtener_conexion().cursor() as cursor:
        if tipo_producto:
            query = '''
                SELECT r.asin, COUNT(*) AS total_reviews
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                WHERE p.product_type = %s
                GROUP BY r.asin
                ORDER BY total_reviews DESC;
            '''
            cursor.execute(query, (tipo_producto,))
        else:
            query = '''
                SELECT r.asin, COUNT(*) AS total_reviews
                FROM reviews r
                GROUP BY r.asin
                ORDER BY total_reviews DESC;
            '''
            cursor.execute(query)
        return cursor.fetchall()
    

# Devuelve la distribución de notas (overall) por producto o tipo de producto
def consulta_histograma_nota(tipo_producto=None, asin=None):
    with obtener_conexion().cursor() as cursor:
        if asin:
            query = '''
                SELECT overall, COUNT(*) AS total_reviews
                FROM reviews
                WHERE asin = %s
                GROUP BY overall
                ORDER BY overall;
            '''
            cursor.execute(query, (asin,))
        elif tipo_producto:
            query = '''
                SELECT r.overall, COUNT(*) AS total_reviews
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                WHERE p.product_type = %s
                GROUP BY r.overall
                ORDER BY r.overall;
            '''
            cursor.execute(query, (tipo_producto,))
        else:
            query = '''
                SELECT overall, COUNT(*) AS total_reviews
                FROM reviews
                GROUP BY overall
                ORDER BY overall;
            '''
            cursor.execute(query)
        return cursor.fetchall()


# Devuelve la evolución acumulada del número de reviews en el tiempo 
def consulta_evolucion_acumulada(tipo_producto=None):
    with obtener_conexion().cursor() as cursor:
        if tipo_producto:
            query = '''
                SELECT r.unixReviewTime
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                WHERE p.product_type = %s
                ORDER BY r.unixReviewTime ASC;
            '''
            cursor.execute(query, (tipo_producto,))
        else:
            query = '''
                SELECT unixReviewTime
                FROM reviews
                ORDER BY unixReviewTime ASC;
            '''
            cursor.execute(query)
        resultados = cursor.fetchall()
        return [r['unixReviewTime'] for r in resultados]


# Devuelve la distribución de número de reviews por usuario
def consulta_reviews_por_usuario():
    with obtener_conexion().cursor() as cursor:
        query = '''
            SELECT num_reviews, COUNT(*) AS num_usuarios
            FROM (
                SELECT reviewerID, COUNT(*) AS num_reviews
                FROM reviews
                GROUP BY reviewerID
            ) AS sub
            GROUP BY num_reviews
            ORDER BY num_reviews;
        '''
        cursor.execute(query)
        return cursor.fetchall()


# Devuelve la media de valoración por tipo de producto, con opción de filtrar por año
def consulta_media_valoracion_por_tipo(anio=None):
    with obtener_conexion().cursor() as cursor:
        if anio:
            query = '''
                SELECT p.product_type, AVG(r.overall) AS media_valoracion
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                WHERE YEAR(r.reviewTime) = %s
                GROUP BY p.product_type
                ORDER BY media_valoracion DESC;
            '''
            cursor.execute(query, (anio,))
        else:
            query = '''
                SELECT p.product_type, AVG(r.overall) AS media_valoracion
                FROM reviews r
                JOIN products p ON r.asin = p.asin
                GROUP BY p.product_type
                ORDER BY media_valoracion DESC;
            '''
            cursor.execute(query)
        return cursor.fetchall()


# Devuelve los años distintos disponibles en los datos
def obtener_anios_disponibles():
    with obtener_conexion().cursor() as cursor:
        cursor.execute("SELECT DISTINCT YEAR(reviewTime) AS anio FROM reviews ORDER BY anio")
        return [row['anio'] for row in cursor.fetchall()]



# Filtra palabras irrelevantes de los textos para la nube de palabras
def palabras_filtradas(textos):
    stopwords = set([
        "this", "that", "with", "have", "they", "very", "like", "just", "it's",
        "from", "about", "would", "could", "should", "which", "there", "their",
        "been", "will", "them", "when", "these"
    ])
    return [p.lower().strip('.,!?"') for t in textos for p in t.split() if len(p) > 3 and p.lower() not in stopwords]



# Genera imagen de la nube de palabras a partir de los textos

def generar_nube_palabras(textos):
    palabras = palabras_filtradas(textos)
    conteo = Counter(palabras)
    nube = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(conteo)
    imagen = io.BytesIO()
    nube.to_image().save(imagen, format='PNG')
    return base64.b64encode(imagen.getvalue()).decode()


# Obtiene los reviewText desde MongoDB a partir de una lista de reviewID

def obtener_reviewtext_por_tipo(review_ids):
    result = collection.find(
        {"reviewID": {"$in": review_ids}},
        {"reviewText": 1, "_id": 0}
    )
    return [doc["reviewText"] for doc in result if "reviewText" in doc]


# LAYOUT Y CALLBACKS



# Opciones del dropdown para tipo de producto, incluyendo "Todos"
TIPO_PRODUCTO_OPTIONS = [{'label': 'Todos', 'value': 'all'}] + [
    {'label': tipo.title().replace('_', ' '), 'value': tipo} for tipo in obtener_tipos_producto()
]


# Opciones del dropdown para los años disponibles en los datos
VALORACION_ANIO_OPTIONS = [{'label': 'Todos', 'value': 'all'}] + [
    {'label': str(a), 'value': a} for a in obtener_anios_disponibles()
]


# Layout general del dashboard con tres pestañas
app.layout = html.Div([
    dcc.Tabs(id="tabs", value='tab1', children=[
        
        #TENDENCIAS TEMPORALES
        dcc.Tab(label='Tendencias Temporales', value='tab1', children=[
            html.Div([
                html.H2("Evolución de reviews por año"),
                html.Label("Tipo de producto:"),
                dcc.Dropdown(id='dropdown-anio-producto', options=TIPO_PRODUCTO_OPTIONS, value='all'),
                dcc.Graph(id='grafico-anio'),

                html.H2("Evolución temporal acumulada por categoría"),
                html.Label("Tipo de producto:"),
                dcc.Dropdown(id='dropdown-acumulado-producto', options=TIPO_PRODUCTO_OPTIONS, value='all'),
                dcc.Graph(id='grafico-acumulado')
            ])
        ]),

        # VALORACIONES Y POPULARIDAD 
        dcc.Tab(label='Valoraciones y Popularidad', value='tab2', children=[
            html.Div([
                html.H2("Popularidad de artículos"),
                html.Label("Tipo de producto:"),
                dcc.Dropdown(id='dropdown-popularidad-producto', options=TIPO_PRODUCTO_OPTIONS, value='all'),
                dcc.Graph(id='grafico-popularidad'),

                html.H2("Histograma por nota (por tipo o producto individual)"),
                html.Label("Tipo de producto:"),
                dcc.Dropdown(id='dropdown-histograma-tipo', options=TIPO_PRODUCTO_OPTIONS, value='all'),
                html.Label("ID del producto:"),
                dcc.Dropdown(id='dropdown-histograma-asin'),
                dcc.Graph(id='grafico-histograma-nota'),

                html.H2("Valoración media por tipo de producto y año"),
                html.Label("Selecciona el año:"),
                dcc.Dropdown(id='dropdown-valoracion-anio', options=VALORACION_ANIO_OPTIONS, value='all'),
                dcc.Graph(id='grafico-media-valoracion'),

                html.H2("Distribución de reviews por usuario"),
                dcc.Graph(id='grafico-usuarios')
            ])
        ]),

        # ANÁLISIS DE TEXTO
        
        dcc.Tab(label='Análisis de Texto', value='tab3', children=[
            html.Div([
                html.H2("Nube de palabras (reviewText por categoría)"),
                html.Label("Tipo de producto:"),
                dcc.Dropdown(id='dropdown-nube-producto', options=TIPO_PRODUCTO_OPTIONS[1:], value='toys'),
                html.Img(id='nube-palabras', style={'width': '100%', 'height': 'auto'})
            ])
        ])
    ])
])


# Callback para el gráfico de evolución anual
@app.callback(Output('grafico-anio', 'figure'), Input('dropdown-anio-producto', 'value'))
def actualizar_grafico_anio(tipo):
    tipo = None if tipo == 'all' else tipo
    datos = consulta_reviews_por_anio(tipo)
    x = [d['anio'] for d in datos]
    y = [d['total_reviews'] for d in datos]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=y))
    fig.update_layout(xaxis_title="Año", yaxis_title="Número de reviews", xaxis_tickangle=-45)
    return fig


# Callback para la evolución temporal acumulada
@app.callback(Output('grafico-acumulado', 'figure'), Input('dropdown-acumulado-producto', 'value'))
def actualizar_grafico_acumulado(tipo):
    tipo = None if tipo == 'all' else tipo
    tiempos = consulta_evolucion_acumulada(tipo)
    tiempos_ordenados = sorted(tiempos)
    acumulados = list(range(1, len(tiempos_ordenados)+1))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=tiempos_ordenados, y=acumulados, mode='lines'))
    fig.update_layout(xaxis_title="Tiempo", yaxis_title="Número de reviews hasta ese momento")
    return fig


# Callback para la distribución de reviews por usuario
@app.callback(Output('grafico-usuarios', 'figure'), Input('tabs', 'value'))
def actualizar_grafico_usuarios(tab):
    if tab != 'tab2':
        return go.Figure()
    datos = consulta_reviews_por_usuario()
    x = [d['num_reviews'] for d in datos]
    y = [d['num_usuarios'] for d in datos]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=y))
    fig.update_layout(xaxis_title="Número de reviews por usuario", yaxis_title="Número de usuarios")
    return fig


# Callback para gráfico de popularidad de productos
@app.callback(Output('grafico-popularidad', 'figure'), Input('dropdown-popularidad-producto', 'value'))
def actualizar_grafico_popularidad(tipo):
    tipo = None if tipo == 'all' else tipo
    datos = consulta_popularidad(tipo)
    x = list(range(1, len(datos)+1))
    y = [d['total_reviews'] for d in datos]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y, mode='lines'))
    fig.update_layout(xaxis_title="Artículos", yaxis_title="Número de reviews")
    return fig


# Callback para actualizar el dropdown de ASINs al cambiar el tipo de producto
@app.callback(Output('dropdown-histograma-asin', 'options'), Input('dropdown-histograma-tipo', 'value'))
def actualizar_asins(tipo):
    if tipo == 'all':
        return []
    return [{'label': asin, 'value': asin} for asin in obtener_asins_por_tipo(tipo)]


# Callback para mostrar el histograma de notas por tipo o producto
@app.callback(
    Output('grafico-histograma-nota', 'figure'),
    Input('dropdown-histograma-tipo', 'value'),
    Input('dropdown-histograma-asin', 'value')
)
def actualizar_histograma(tipo, asin):
    tipo = None if tipo == 'all' else tipo
    datos = consulta_histograma_nota(tipo_producto=tipo, asin=asin)
    x = [d['overall'] for d in datos]
    y = [d['total_reviews'] for d in datos]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=y))
    fig.update_layout(xaxis_title="Nota", yaxis_title="Número de reviews")
    return fig


# Callback para generar y mostrar la nube de palabras desde MongoDB
@app.callback(Output('nube-palabras', 'src'), Input('dropdown-nube-producto', 'value'))
def actualizar_nube_palabras(tipo):
    ids = ids_para_mongo(tipo)
    textos = obtener_reviewtext_por_tipo(ids)
    imagen_base64 = generar_nube_palabras(textos)
    return f'data:image/png;base64,{imagen_base64}'


# Callback para mostrar la media de valoraciones por categoría y año
@app.callback(
    Output('grafico-media-valoracion', 'figure'),
    Input('dropdown-valoracion-anio', 'value')
)
def actualizar_grafico_media_valoracion(anio):
    anio = None if anio == 'all' else int(anio)
    datos = consulta_media_valoracion_por_tipo(anio)
    x = [d['product_type'] for d in datos]
    y = [round(d['media_valoracion'], 2) for d in datos]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=y))
    fig.update_layout(
        title="Valoración media por tipo de producto",
        xaxis_title="Tipo de producto",
        yaxis_title="Valoración media",
        yaxis_range=[0, 5]
    )
    return fig

# Ejecución del servidor
if __name__ == '__main__':
    app.run_server(debug=True)