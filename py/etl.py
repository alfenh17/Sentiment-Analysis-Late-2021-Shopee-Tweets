# Import sqlalchemy, kafka, regular expression dan time
from sqlalchemy import *
from sqlalchemy.sql import * 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from kafka import KafkaConsumer, consumer
import re
import time

# Mendefinisikan proses ETL (Extract, Transform dan Load)
def etl():
    # Menghubungkan Ke Kafka Consumer
    consumer = KafkaConsumer(bootstrap_servers = ['localhost:9092'], api_version=(0,10))
    consumer.subscribe('shop')

    # EXTRACT
    tweet = []
    max = 0
    for message in consumer:
        if max <= 20:
            tweet.append(message)
        else:
            break
        max += 1
        print("Baca ", max , " tweet")


    # TRANSFORM
    # Tweet Cleaning Sebelum di Load Ke DataBase Nanti
    pembersihan = [re.sub("b'","", str(i.value)) for i in tweet]
    kumpulan_tweet = [i.split('~') for i in pembersihan]

    # Membuat tabel baru bernama shopee dengan kolom index, id_user, cuitan dan tanggal_cuit
    class Users (Base):
        __tablename__ = "shopee"
        index = Column(Integer, primary_key = True)
        id_user = Column(String(1000))
        cuitan = Column(String(1000))
        tanggal_cuit = Column(String(1000))

    Users.__table__.create(bind = engine, checkfirst = True)

    # Disini, data akan diubah ke dalam 3 kolom terpisah, kemudian kolom-kolom diberi nama id_user, cuitran, dan tanggal cuit
    tweet = []
    index = 0
    for i in kumpulan_tweet:
        row = {}
        row['index'] = index
        row['id_user'] = i[0]
        row['cuitan'] = i[1]
        row['tanggal_cuit'] = i[2]
        tweet.append(row)


    # LOAD ke Database MySQL
    # Menghubungkan ke schema shop pada database mysql melalui localhost
    engine = create_engine('mysql+mysqlconnector://root:@localhost/shop')
    Base = declarative_base()

    # Membuat session baru
    Session = sessionmaker(bind = engine)
    session = Session()
    
    # Ini adalah proses me-load tweet ke database
    for cuitan in tweet:
        row = Users(**cuitan)
        session.add(row)

    session.commit()
    session.close()


def periodic_work(interval):
    while True:
        etl()
        time.sleep(interval)

periodic_work(2*1)