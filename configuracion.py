"""
Javier Mendoza Guerrero 
Luis Bertrán Vidal Campos
"""

MYSQL_CONFIG = {
    "host": "localhost",   
    "user": "",       #MODIFCAR ESTE CAMPO
    "password": "", #MODIFCAR ESTE CAMPO
    "database": "reviews"   
}

MONGO_CONFIG = {
    "host": "localhost",  
    "database": "reviews",
    "collection": "reviews"
}

#MODIFCAR LAS RUTAS 
DATA_FILES = {
    "toys": "./data/Toys_and_Games_5.json",
    "video_games": "./data/Video_Games_5.json",
    "music": "./data/Digital_Music_5.json",
    "instruments": "./data/Musical_Instruments_5.json"
}
 