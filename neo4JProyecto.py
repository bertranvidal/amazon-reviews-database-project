"""
Javier Mendoza Guerrero 
Luis Bertrán Vidal Campos
"""

import pymysql
import math
from neo4j import GraphDatabase
import random
from configuracion import MYSQL_CONFIG, DATA_FILES
import csv
from datetime import datetime
import json


NOMBRE_BBDD_SQL = MYSQL_CONFIG["database"]
USUARIO_SQL = MYSQL_CONFIG["user"]
CONTRASENIA_SQL = MYSQL_CONFIG["password"]

#Conexion a MySQL
conexion_mysql = pymysql.connect(
        host="localhost",
        user=USUARIO_SQL,
        password=CONTRASENIA_SQL,
        database=NOMBRE_BBDD_SQL
    ) 
cursor_mysql = conexion_mysql.cursor()

# Conexión con Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "aaaaaaaa"))


def limpiar_base_de_datos_neo4j():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("Base de datos de Neo4j limpiada correctamente.")

# ---> Funciones apartado 4.1
def usuarios_top_reviews(n):
    query = """
        SELECT reviewerID, COUNT(*) AS numero_reviews
        FROM Reviews
        GROUP BY reviewerID
        ORDER BY numero_reviews DESC
        LIMIT %s
    """
    cursor_mysql.execute(query, (n,)) #usando %s permitimos que el valor de 30 sea fácilmente configurable
    resultados = cursor_mysql.fetchall()
    lista_top_N_usuarios = [fila[0] for fila in resultados]
    return lista_top_N_usuarios

def reviews_top_usuarios(lista_top_N_usuarios):
    lista_top_N_usuarios_sql = ','.join(['%s'] * len(lista_top_N_usuarios)) #generamos una lista string de con el mismo número de %s como haya elementos en la lista de top users
    consulta = f"""
        SELECT reviewerID, asin, overall
        FROM Reviews
        WHERE reviewerID IN ({lista_top_N_usuarios_sql})
    """
    cursor_mysql.execute(consulta, tuple(lista_top_N_usuarios)) #covertimos el string en tupla antes de pasarselo al cursor
    lista_resutados = cursor_mysql.fetchall()
    return lista_resutados  #devuleve una lista con tuplas (las filas) con estrcutura (usuario, asin, puntuación)

def calcular_pearson(valores_a, valores_b):
    cantidad = len(valores_a)
    if cantidad == 0:
        return 0

    suma_a = sum(valores_a)
    suma_b = sum(valores_b)
    suma_cuadrados_a = sum([x**2 for x in valores_a])
    suma_cuadrados_b = sum([x**2 for x in valores_b])
    suma_productos = sum([x * y for x, y in zip(valores_a, valores_b)])

    numerador = suma_productos - (suma_a * suma_b / cantidad)
    denominador = math.sqrt((suma_cuadrados_a - suma_a * 2 / cantidad) * (suma_cuadrados_b - suma_b * 2 / cantidad))

    if denominador == 0:
        return 0

    return numerador / denominador

def generar_lista_similitudes(lista_reviews, usuarios_relevantes, path_salida):
    mapa_puntuaciones = {}
    for usuario_id, producto_id, puntuacion in lista_reviews:
        if usuario_id not in mapa_puntuaciones:
            mapa_puntuaciones[usuario_id] = {}
        mapa_puntuaciones[usuario_id][producto_id] = puntuacion

    lista_similitudes = []

    for i in range(len(usuarios_relevantes)):
        for j in range(i+1 , len(usuarios_relevantes)):
            usuario_a = usuarios_relevantes[i]
            usuario_b = usuarios_relevantes[j]
            items_a = mapa_puntuaciones[usuario_a]
            items_b = mapa_puntuaciones[usuario_b]

            productos_comunes = set(items_a.keys()) & set(items_b.keys())
            if len(productos_comunes) >= 2:
                puntuaciones_a = [items_a[prod] for prod in productos_comunes]
                puntuaciones_b = [items_b[prod] for prod in productos_comunes]
                similitud = calcular_pearson(puntuaciones_a, puntuaciones_b)
                lista_similitudes.append((usuario_a, usuario_b, round(similitud, 3)))
    with open(path_salida, mode="w", newline='', encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(["usuario_1", "usuario_2", "similitud"])
        writer.writerows(lista_similitudes)
        print(f"Similitudes guardadas correctamente en '{path_salida}'")
    return lista_similitudes

def insertar_similitudes_en_neo4j(lista_similitudes):
    with driver.session() as sesion_neo:
        sesion_neo.run("MATCH (n) DETACH DELETE n")
        for usuario_a, usuario_b, puntuacion in lista_similitudes:
            sesion_neo.run("""
                MERGE (a:User {id: $id_a})
                MERGE (b:User {id: $id_b})
                MERGE (a)-[:SIMILAR_TO {score: $punt}]->(b)
                MERGE (b)-[:SIMILAR_TO {score: $punt}]->(a)
            """, {"id_a": usuario_a, "id_b": usuario_b, "punt": puntuacion})
    print("Relaciones de similitud insertadas en Neo4j 4.1")


def usuario_mas_relacionado():
    with driver.session() as sesion:
        resultado = sesion.run("""
            MATCH (u:User)-[:SIMILAR_TO]->()
            RETURN u.id AS nombre_usuario, COUNT(*) AS total_conexiones
            ORDER BY total_conexiones DESC
            LIMIT 1
        """)
        for fila in resultado:
            print(f"Usuario con más conexiones: {fila['nombre_usuario']} ({fila['total_conexiones']} conexiones)")

# --> Funciones apartado 4.2

def seleccionar_articulos_y_usuarios_desde_archivo():
    tipo_articulo = input("Introduce el tipo de artículo (por ejemplo: [toys, video_games, music, instruments]): ")
    while tipo_articulo not in DATA_FILES:
        tipo_articulo = input("El tipo debe estar en la lista [toys, video_games, music, instruments]: ")

    archivo = DATA_FILES[tipo_articulo]

    with open(archivo, "r") as f:
        datos = [json.loads(linea) for linea in f]

    asins = list({d["asin"] for d in datos if "asin" in d})

    #tomamos maximo posible como n=numero de productos /10 para que el grafo tenga unas dimensión razonable  
    max_cantidad = round(len(asins) / 10)

    cantidad = int(input(f"Introduce la cantidad de productos a seleccionar (máximo {max_cantidad}): "))
    while cantidad > max_cantidad:
        cantidad = int(input(f"Cantidad demasiado grande. Debes elegir un número menor o igual a {max_cantidad}."))
       
    print(f"Número de artículos elegido: {cantidad}")
    articulos_seleccionados = set(random.sample(asins, cantidad))

    reviews = []
    for review in datos:
        if review.get("asin") in articulos_seleccionados:
            reviewerID = review.get("reviewerID")
            asin = review.get("asin")
            overall = review.get("overall", 0)

            try:
                reviewTime_orginal = review.get("reviewTime", "Null")
                reviewTime_convertido = datetime.strptime(reviewTime_orginal, "%m %d, %Y").date()
            except ValueError:
                reviewTime_convertido = None

            reviewerName = review.get("reviewerName", "Unknown")

            reviews.append((reviewerID, asin, overall, reviewTime_convertido, reviewerName))

    with driver.session() as session:
        for reviewerID, asin, overall, timestamp, reviewerName in reviews:
            session.run("""
                MERGE (u:User {id: $usuario})
                SET u.name = $nombre
                MERGE (a:Item {id: $articulo})
                MERGE (u)-[:REVIEWED {score: $puntuacion, time: $tiempo}]->(a)
            """, {
                "usuario": reviewerID,
                "nombre": reviewerName,
                "articulo": asin,
                "puntuacion": overall,
                "tiempo": str(timestamp) if timestamp else None
            })

    print("Artículos y reviews ya están cargados en Neo4j 4.2.")


# ---> Funciones apartado 4.3

 
def cargar_usuarios_y_tipos_articulos():
     
    #creamos diccionario de usuarios con la información necesaria (usuario, tipo de artículo)
    usuarios_dict = {}

    for categoria_nombre, archivo_ruta in DATA_FILES.items():
        with open(archivo_ruta, 'r', encoding='utf-8') as archivo:
            registros = [json.loads(linea) for linea in archivo]
        
            for review in registros:
                id_revisor = review['reviewerID']
                categoria = categoria_nombre
                reviewerName = review.get("reviewerName", "Unknown")
                
                if id_revisor not in usuarios_dict:
                    usuarios_dict[id_revisor] = {'nombre': reviewerName, 'categorias': {}}
                
                if categoria not in usuarios_dict[id_revisor]['categorias']:
                    usuarios_dict[id_revisor]['categorias'][categoria] = 0
                
                usuarios_dict[id_revisor]['categorias'][categoria] += 1

    #aquí filtramos por los que tienen al menos 2 tipos 
    usuarios_filtrados = {k: v for k, v in usuarios_dict.items() if len(v['categorias']) > 1}

    #nos quedamos ahora con los 400 primero
    usuarios_ordenados = sorted(usuarios_filtrados.items(), key=lambda x: x[1]['nombre'])[:400]

    with driver.session() as sesion:
        for id_revisor, info in usuarios_ordenados:
            sesion.run("MERGE (u:Usuario {id: $id_revisor, nombre: $nombre})", 
                    {"id_revisor": id_revisor, "nombre": info['nombre']})
            
            for categoria, cantidad in info['categorias'].items():
                sesion.run("MERGE (c:Categoria {nombre: $categoria})", {"categoria": categoria})
                sesion.run("""
                MATCH (u:Usuario {id: $id_revisor}), (c:Categoria {nombre: $categoria})
                MERGE (u)-[:CONSUME {cantidad: $cantidad}]->(c)
                """, {"id_revisor": id_revisor, "categoria": categoria, "cantidad": cantidad})
    print("Finalizada carga en Neo4j 4.3")
                
#-->Funciones apartado 4.4
def seleccionar_articulos_menor_40_reviews():
    cursor_mysql.execute("""
        SELECT asin, COUNT(*) as total_reviews
        FROM Reviews
        GROUP BY asin
        HAVING total_reviews < 40
        ORDER BY total_reviews DESC
        LIMIT 5
    """)
    articulos_populares = cursor_mysql.fetchall()
    
    return [articulo[0] for articulo in articulos_populares]  #solo devolvemos los asin (artículos)

def cargar_articulos_y_usuarios_en_neo4j():
    articulos_populares = seleccionar_articulos_menor_40_reviews()
    
    #para esos artículos obtenemos los usuarios y la puntuación de esos artículos
    formato_ids = ','.join(['%s'] * len(articulos_populares))
    consulta_reviews = f"""
        SELECT reviewerID, asin, overall
        FROM Reviews
        WHERE asin IN ({formato_ids})
    """
    cursor_mysql.execute(consulta_reviews, tuple(articulos_populares))
    reviews = cursor_mysql.fetchall()
 
    with driver.session() as session:
        for reviewerID, asin, overall in reviews:
            session.run("""
                MERGE (u:User {id: $usuario})
                MERGE (a:Item {id: $articulo})
                MERGE (u)-[:REVIEWED {score: $puntuacion}]->(a)
            """, {
                "usuario": reviewerID,
                "articulo": asin,
                "puntuacion": overall
            })
    
    print("Artículos y usuarios cargados en Neo4j 4.4.")

def calcular_enlaces_entre_usuarios():
    #calculamos cuántos enlaces entre usuarios
    with driver.session() as session:
        session.run("""
            MATCH (u1:User)-[r:REVIEWED]->(a:Item)<-[r2:REVIEWED]-(u2:User)
            WITH u1, u2, COUNT(a) AS num_comun
            WHERE u1.id <> u2.id
            MERGE (u1)-[enlace:ENLACE]->(u2)
            SET enlace.num_comun = num_comun
            RETURN u1.id AS usuario_1, u2.id AS usuario_2, enlace.num_comun AS num_comun
            ORDER BY num_comun DESC
        """)
        
    print("Enlaces entre usuarios calculados.")

#-->Menu
def menu():
    while True:
        print("\n--- Menú ---")
        print("1. Ejecutar Apartado 4.1")
        print("2. Ejecutar Apartado 4.2")
        print("3. Ejecutar Apartado 4.3")
        print("4. Ejecutar Apartado 4.4")
        print("5. Salir")
        opcion = input("Elige una opción (1-5): ")

        if opcion in {"1", "2", "3", "4", "5"}:
            return int(opcion)
        else:
            print("Entrada no válida. Debe ser un número del 1 al 5.")
 


if __name__ == "__main__":
    while True:
        opcion = menu()

        if opcion == 1:
            # Apartado 4.1
            limpiar_base_de_datos_neo4j()
            top_users = usuarios_top_reviews(30)
            reviews = reviews_top_usuarios(top_users)
            ruta_salida = "similitudes.csv"
            similitudes = generar_lista_similitudes(reviews, top_users, ruta_salida)
            insertar_similitudes_en_neo4j(similitudes)
            usuario_mas_relacionado()

        elif opcion == 2:
            # Apartado 4.2
            limpiar_base_de_datos_neo4j()
            seleccionar_articulos_y_usuarios_desde_archivo()

        elif opcion == 3:
            # Apartado 4.3
            limpiar_base_de_datos_neo4j()
            cargar_usuarios_y_tipos_articulos()

        elif opcion == 4:
            # Apartado 4.4
            limpiar_base_de_datos_neo4j()
            cargar_articulos_y_usuarios_en_neo4j()
            calcular_enlaces_entre_usuarios()

        elif opcion == 5:
            print("Sale del programa")
            break