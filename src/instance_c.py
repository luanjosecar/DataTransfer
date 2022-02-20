import psycopg2
import pandas as pd
import datetime

import os
from dotenv import load_dotenv

load_dotenv()

host ='localhost'
db = 'rio'
user = 'postgres'
password = "root"


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

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            FUNCIONARIOS (
                ID_FUNC INTEGER,
                FUNCIONARIO VARCHAR(1024)
            )
        ''')
    con.commit()


    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            CATEGORIAS (
                ID_CAT INTEGER,
                CATEGORIA VARCHAR(1024)
            )
        ''')
    con.commit()



def check_id_vend_c():
    sql = "select id_venda from vendas"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda'])
    return pd_data


def check_funcs():
    sql = "select * from FUNCIONARIOS"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])
    return pd_data

def check_cats():
    sql = "select * from CATEGORIAS"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    return pd_data

def check_table():
    sql = 'SELECT CASE WHEN EXISTS (SELECT * FROM vendas LIMIT 1) THEN 1 ELSE 0 END'
    cur.execute(sql)
    index, =  cur.fetchone()
    if index == 1:
        return True
    return False




def add_vend_table(data):
    data = list(frame.itertuples(index=False, name=None))
    data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return
    cur.execute('INSERT INTO VENDAS VALUES ' + str(data_string))
    con.commit()

def add_func_table(data):
    data = list(frame.itertuples(index=False, name=None))
    data_string = ','.join(cur.mogrify("(%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return
    cur.execute('INSERT INTO FUNCIONARIOS VALUES ' + str(data_string))
    con.commit()

def add_cat_table(data):
    data = list(frame.itertuples(index=False, name=None))
    data_string = ','.join(cur.mogrify("(%s,%s)",values ).decode('utf-8')for values in data)
    if(data_string == ""):
        return
    cur.execute('INSERT INTO CATEGORIAS VALUES ' + str(data_string))
    con.commit()
    



