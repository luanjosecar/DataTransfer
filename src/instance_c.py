import psycopg2
import pandas as pd
import datetime

import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST_C')
DB = os.getenv("DB_C")
USER = os.getenv("PSTG_USER_C")
PASSWORD = os.getenv("PSTG_PSW")
PORT = os.getenv("PORT_C")


def create_bases():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
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

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            FUNCIONARIOS (
                ID_FUNCIONARIO INTEGER,
                FUNCIONARIO VARCHAR(1024)
            )
        ''')
    con.commit()


    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            CATEGORIAS (
                ID_CATEGORIA INTEGER,
                CATEGORIA VARCHAR(1024)
            )
        ''')
    con.commit()
    con.close()



def check_id_vend_c():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select id_venda from vendas"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda'])
    con.close()
    return pd_data


def check_funcs():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select * from FUNCIONARIOS"
    cur.execute(sql)
    data = cur.fetchall()
    con.close()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])
    return pd_data

def check_cats():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select * from CATEGORIAS"
    cur.execute(sql)
    data = cur.fetchall()
    con.close()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    return pd_data

def check_table():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = 'SELECT CASE WHEN EXISTS (SELECT * FROM vendas LIMIT 1) THEN 1 ELSE 0 END'
    cur.execute(sql)
    index, =  cur.fetchone()
    con.close()
    if index == 1:
        return True
    return False


def add_vend_table(frame):
    data = list(frame.itertuples(index=False, name=None))
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return

    cur.execute('INSERT INTO VENDAS VALUES ' + str(data_string))
    con.commit()
    con.close()

def add_func_table(frame):
    data = list(frame.itertuples(index=False, name=None))
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    data_string = ','.join(cur.mogrify("(%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return

    cur.execute('INSERT INTO FUNCIONARIOS VALUES ' + str(data_string))
    con.commit()
    con.close()

def add_cat_table(frame):
    data = list(frame.itertuples(index=False, name=None))
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    data_string = ','.join(cur.mogrify("(%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return

    cur.execute('INSERT INTO CATEGORIAS VALUES ' + str(data_string))
    con.commit()
    con.close()
    



