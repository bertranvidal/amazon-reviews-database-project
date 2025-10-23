"""
Javier Mendoza Guerrero 
Luis Bertrán Vidal Campos
"""

import json
import pymysql
from pymongo import MongoClient 
from configuracion import MYSQL_CONFIG, MONGO_CONFIG
from datetime import datetime

#Hemos importado datos del archivo configuracion.py

#configuración de Mongo
MONGO_HOST = MONGO_CONFIG["host"]
NOMBRE_BBDD_MONGO = MONGO_CONFIG["database"]
NOMBRE_COLECCION_MONGO = MONGO_CONFIG["collection"]

#configuración de SQL
NOMBRE_BBDD_SQL = MYSQL_CONFIG["database"]
USUARIO_SQL = MYSQL_CONFIG["user"]
CONTRASENIA_SQL = MYSQL_CONFIG["password"]

def conectar_sql():
    #reabrimos conexión con la base de datos 
    conexion_mysql = pymysql.connect(
        host="localhost",
        user=USUARIO_SQL,
        password=CONTRASENIA_SQL,
        database=NOMBRE_BBDD_SQL
    ) 
    cursor = conexion_mysql.cursor()
    return conexion_mysql, cursor  

def conectar_mongo():
    mongo_client = MongoClient(f"mongodb://{MONGO_HOST}")
    mongo_db = mongo_client[NOMBRE_BBDD_MONGO]
    return mongo_db[NOMBRE_COLECCION_MONGO]

def inserta_datos(conexion_mysql, cursor_mysql, collection_mongo):
    nombre_tipo = "Pets"
    ruta_tipo = "./data/Pet_Supplies_5.json" #MODIFICAR AQUI 
    print(f"Insertamos coleccion {nombre_tipo}")
    
    #obtener el número total de reseñas en la tabla Reviews
    cursor_mysql.execute("SELECT COUNT(*) FROM Reviews")
    result = cursor_mysql.fetchone()
    total_reviews = result[0]
    
    #se inicia el contador de reviewID a partir del total de reseñas 
    autoincrement = total_reviews + 1

    with open(ruta_tipo, "r", encoding="utf-8") as file:
        for linea in file:
            try:
                review = json.loads(linea)   

                #insertamos en MongoDB
                diccionario_datos_mongo = {
                    "reviewID": autoincrement,
                    "reviewText": review.get("reviewBody", ""),
                    "summary": review.get("summary", ""),
                    "helpful": review.get("helpful", {}),  
                }
                collection_mongo.insert_one(diccionario_datos_mongo)

                #insertamos en MySQL
                try:
                    reviewTime_original = review.get("reviewTime", "Null") 
                    reviewTime_convertido = datetime.strptime(reviewTime_original, "%m %d, %Y").date()
                except ValueError:
                    reviewTime_convertido = None  

                reviewerName = review.get("reviewerName", "Unknown")
                
                cursor_mysql.execute("""
                    INSERT IGNORE INTO Users (reviewerID, reviewerName)
                    VALUES (%s, %s)
                """, (review["reviewerID"], reviewerName))
                
                cursor_mysql.execute("""
                    INSERT IGNORE INTO Products (asin, product_type)
                    VALUES (%s, %s)
                """, (review["asin"], nombre_tipo))
                
                cursor_mysql.execute("""
                    INSERT INTO Reviews (reviewID, reviewerID, asin, overall, unixReviewTime, reviewTime)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (autoincrement, review["reviewerID"], review["asin"], review["overall"], review["unixReviewTime"], reviewTime_convertido))

                autoincrement += 1

            except json.JSONDecodeError as e:
                print(f"Error al procesar línea: {e}")
                
    conexion_mysql.commit()
    cursor_mysql.close()
    conexion_mysql.close()
    print("Carga de datos completada.")

if __name__  == "__main__":
    conexion_mysql, cursor_mysql  = conectar_sql()
    collection_mongo = conectar_mongo()
    inserta_datos(conexion_mysql, cursor_mysql, collection_mongo)
