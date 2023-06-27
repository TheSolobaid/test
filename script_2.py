

import paho.mqtt.client as mqtt
import pymysql.cursors
from datetime import datetime
from collections import deque
import time
cache = deque()
cache_capteur = deque()

def test(msg):
    data = msg.payload.decode().split(',')
    
    

def insert_data_with_cache(conn, data):
    # add data to the cache
    cache.append(data)

    while cache:
        try:
            macaddr, piece, time, date, temp = cache[0]
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO data (macaddr, piece, time, date, temp)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (macaddr, piece, time, date, temp))
                conn.commit()
                print('Sensor history data inserted into \'data\' successfully')

            # remove data from the cache after successful insertion
            cache.popleft()
        except pymysql.Error as e:
            print(f'An error {e.args[0]} occurred')
            # sleep for a while before retrying
            time.sleep(5)
            conn = create_connection()
            print("Reconnected to DB")
            
def insert_capteur_with_cache(conn, data):
    # add data to the cache
    cache_capteur.append(data)

    while cache_capteur:
        try:
            piece, lieu = cache_capteur[0]
            with conn.cursor() as cursor:
                sql = """
                INSERT IGNORE INTO capteur (piece, lieu, piece_affich, lieu_affich) 
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (piece, lieu, piece, lieu))
                conn.commit()
                print(f"Data inserted into 'capteur' successfully, or already existed")

            # remove data from the cache after successful insertion
            cache_capteur.popleft()
        except pymysql.Error as e:
            print(f'An error {e.args[0]} occurred')
            # sleep for a while before retrying
            time.sleep(5)
            conn = create_connection()
            print("Reconnected to DB")

# Connexion à la base de données MySQL
def create_connection():
    conn = None
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='toto',
            db='mqtt',
            cursorclass=pymysql.cursors.DictCursor
        )
        print(f'Successful connection with MySQL version {pymysql.__version__}')
    except pymysql.Error as e:
        print(f'An error {e.args[0]} occurred')
    return conn

def create_capteur_table(conn):
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS capteur (
                id INT NOT NULL AUTO_INCREMENT,
                piece VARCHAR(50) NOT NULL,
                lieu VARCHAR(50) NOT NULL,
                piece_affich VARCHAR(50) NOT NULL UNIQUE,
                lieu_affich VARCHAR(50),
                PRIMARY KEY (id),
                UNIQUE(piece)
            )
            """
            cursor.execute(sql)
            conn.commit()
            print('Table \'capteur\' created successfully')
    except pymysql.Error as e:
        print(f'An error {e.args[0]} occurred')

def create_data_table(conn):
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS data (
                id INT NOT NULL AUTO_INCREMENT,
                macaddr VARCHAR(12) NOT NULL,
                piece VARCHAR(50) NOT NULL,
                time TIME,
                date DATE,
                temp DOUBLE,
                PRIMARY KEY (id)
            )
            """
            cursor.execute(sql)
            conn.commit()
            print('Table \'data\' created successfully')
    except pymysql.Error as e:
        print(f'An error {e.args[0]} occurred')


def insert_capteur(conn, piece, lieu):
    
    with conn.cursor() as cursor:
        sql = """
        INSERT IGNORE INTO capteur (piece, lieu, piece_affich, lieu_affich) 
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(sql, (piece, lieu, piece, lieu))
        conn.commit()
        print(f"Data inserted into 'capteur' successfully, or already existed")
    


def insert_data(conn, macaddr, piece, time, date, temp):
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO data (macaddr, piece, time, date, temp)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (macaddr, piece, time, date, temp))
        conn.commit()
        print('Sensor history data inserted into \'data\' successfully')
"""    
def on_message(client, userdata, msg):
    print(f'Received message: {msg.topic} {msg.payload.decode()}')

    # Analyse du message MQTT pour extraire les données
    data = msg.payload.decode().split(',')
    data_dict = {item.split('=')[0]: item.split('=')[1] for item in data}
    maison = topic.split("/")
    maison.reverse()
    # Récupération des données du capteur
    macaddr = data_dict.get('Id')
    piece = data_dict.get('piece')
    time = data_dict.get('time')
    temp = round(float(data_dict.get('temp')),2)
    date = data_dict.get('date')
    date_object = datetime.strptime(date, '%d/%m/%Y')
    date = date_object.strftime('%Y-%m-%d')  # '2023-06-22'
  
    lieu = maison[0]

    insert_capteur(userdata, piece, lieu)
    insert_data(userdata,macaddr,piece,time,date,temp)
"""
def on_message(client, userdata, msg):
    try:
        print(f'Received message: {msg.topic} {msg.payload.decode()}')
        data = msg.payload.decode().split(',')
        data_dict = {item.split('=')[0]: item.split('=')[1] for item in data}
        # Analyse du message MQTT pour extraire les données
    
        maison = topic.split("/")
        maison.reverse()
        # Récupération des données du capteur
        macaddr = data_dict.get('Id')
        piece = data_dict.get('piece')
        time = data_dict.get('time')
        temp = round(float(data_dict.get('temp')),2)
        date = data_dict.get('date')
        date_object = datetime.strptime(date, '%d/%m/%Y')
        date = date_object.strftime('%Y-%m-%d')  # '2023-06-22'
    
        lieu = maison[0]

        insert_capteur_with_cache(userdata, (piece, lieu))
        insert_data_with_cache(userdata, (macaddr, piece, time, date, temp))
    except:
        print('nul le message')

broker_address = 'test.mosquitto.org'
client = mqtt.Client()
client.user_data_set(create_connection())
topic = 'IUT/Colmar2023/SAE2.04/Maison1'

create_capteur_table(client._userdata)
create_data_table(client._userdata)

client.on_message = on_message

client.connect(broker_address)
client.subscribe(topic)

client.loop_forever()

