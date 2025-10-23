"""
Javier Mendoza Guerrero 
Luis Bertrán Vidal Campos
"""
 
import json
import pymysql
from pymongo import MongoClient 
from configuracion import MYSQL_CONFIG, MONGO_CONFIG, DATA_FILES
from datetime import datetime


#configuración de Mongo
MONGO_HOST = MONGO_CONFIG["host"]
NOMBRE_BBDD_MONGO = MONGO_CONFIG["database"]
NOMBRE_COLECCION_MONGO = MONGO_CONFIG["collection"]

#configuración de SQL 
NOMBRE_BBDD_SQL = MYSQL_CONFIG["database"]
USUARIO_SQL = MYSQL_CONFIG["user"]
CONTRASENIA_SQL = MYSQL_CONFIG["password"]

def eliminar_bbdd_sql(): #Elimina la base de datos SQL si ya existe.
    conexion_mysql = pymysql.connect(
        host="localhost",
        user=USUARIO_SQL,
        password=CONTRASENIA_SQL
    )     
    cursor = conexion_mysql.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {NOMBRE_BBDD_SQL}")
    conexion_mysql.commit()  
    cursor.close()  
    conexion_mysql.close()

def eliminar_bbdd_mongo(): #Elimina la base de datos MongoDB si ya existe.
    mongo_client = MongoClient(f"mongodb://{MONGO_HOST}")
    mongo_client.drop_database(NOMBRE_BBDD_MONGO)   


def crear_tablas(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Users (
            reviewerID VARCHAR(50) PRIMARY KEY,
            reviewerName VARCHAR(255)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products (
            asin VARCHAR(50) PRIMARY KEY,
            product_type VARCHAR(50)
        )
    """)
    
        
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Reviews (
            reviewID INT PRIMARY KEY,
            reviewerID VARCHAR(50),
            asin VARCHAR(50),
            overall INT,
            unixReviewTime INT,
            reviewTime DATE,
            FOREIGN KEY (reviewerID) REFERENCES Users(reviewerID),
            FOREIGN KEY (asin) REFERENCES Products(asin)
        )
    """)


def crear_bbdd_sql():
    conexion_mysql = pymysql.connect(
        host="localhost",
        user=USUARIO_SQL,
        password=CONTRASENIA_SQL,
    )     
    cursor = conexion_mysql.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {NOMBRE_BBDD_SQL}")  # Crea la BD si no existe
    
    cursor.close()
    conexion_mysql.close()

def conectar_sql():
    #reabrimos conexión con la base de datos específica
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
   
    
    autoincrement = 1 
    for nombre_tipo, ruta_tipo in DATA_FILES.items():
        print(f"Insertamos coleccion {nombre_tipo}")

        with open(ruta_tipo, "r", encoding="utf-8") as file:
                for linea in file:
                    try:
                        review = json.loads(linea)  #covertimos a dict
                        
                        #-------insertamos en MongoDB
                        diccionario_datos_mongo = {
                            "reviewID": autoincrement,
                            "reviewText": review["reviewText"],
                            "summary": review["summary"],
                            "helpful": review["helpful"],  
                           
                        }
                        collection_mongo.insert_one(diccionario_datos_mongo)
                        
                        
    
                        #---------insertamos datos en MySQL
    
                        #filtramos datos problematicos 
                        #1)cambiamos formato de reviewtime y consideramos nulos
                        try:
                            reviewTime_orginal = review.get("reviewTime", "Null") 
                            reviewTime_convertido = datetime.strptime(reviewTime_orginal, "%m %d, %Y").date()
                        except ValueError:
                            reviewTime_convertido = None  

                        #2)consideramos nulos de ReviewerName
                        reviewerName = review.get("reviewerName", "Unknown")
                        
                        cursor_mysql.execute("""
                            INSERT IGNORE INTO Users (reviewerID, reviewerName)
                            VALUES (%s, %s)
                        """, (review["reviewerID"], reviewerName))
                        
                        cursor_mysql.execute("""
                            INSERT IGNORE INTO Products (asin, product_type)
                            VALUES (%s, %s)
                        """, (review["asin"],nombre_tipo))
                        
                        
                        cursor_mysql.execute("""
                            INSERT INTO Reviews (reviewID, reviewerID, asin, overall, unixReviewTime, reviewTime)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (autoincrement, review["reviewerID"], review["asin"], review["overall"], review["unixReviewTime"], reviewTime_convertido))
                      
                        # Aumentamos el contador para la siguiente review
                        autoincrement += 1
                            
                    except json.JSONDecodeError as e:
                        print(f"Error al procesar línea: {e}")
                        
    conexion_mysql.commit()
    cursor_mysql.close()
    conexion_mysql.close()
    print("Carga de datos completada.")
    
if __name__  == "__main__":
    eliminar_bbdd_sql()
    eliminar_bbdd_mongo()
    crear_bbdd_sql() 
    
    conexion_mysql, cursor_mysql  = conectar_sql()
    collection_mongo = conectar_mongo()
    crear_tablas(cursor_mysql)
    inserta_datos(conexion_mysql, cursor_mysql, collection_mongo)
    
 