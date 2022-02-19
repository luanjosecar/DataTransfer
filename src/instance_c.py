import psycopg2
import pandas as pd
import datetime

host = 'localhost'
db = 'rio'
user = 'postgres'
password = 'root'
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST_C') | 'localhost'
db = os.getenv("DB_c") | 'rio'
user = os.getenv("PSTG_USER_c") | 'postgres'
password = os.getenv("PSTG_PSW_c") | "root"


con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)

cur = con.cursor()

def create_bases():
    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            VENDAS (
                ID_VENDA INTEGER,
                FUNCIONARIO VARCHAR(1024),
                CATEGORIA VARCHAR(1024),
                DATA_VENDA DATE,
                VENDA INTEGER
            )
        ''')
    con.commit()


def input_data(data:list):
    data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
    cur.execute('INSERT INTO VENDAS VALUES ' + str(data_string))
    con.commit()

def check_codes():
    sql = "select id_venda from vendas"
    cur.execute(sql)
    return cur.fetchall()

def check_data():
    sql = 'SELECT CASE WHEN EXISTS (SELECT * FROM vendas LIMIT 1) THEN 1 ELSE 0 END'
    cur.execute(sql)
    index, =  cur.fetchone()
    if index == 1:
        return True
    return False

def delete_data():
    cur.execute('truncate table vendas')
    con.commit()





