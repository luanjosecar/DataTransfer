import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST_B')
DB = os.getenv("DB")
USER = os.getenv("PSTG_USER")
PASSWORD = os.getenv("PSTG_PSW")
PORT = os.getenv("PORT_B")




def get_cats():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select * from categoria"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    con.close()
    return pd_data


def get_funcs():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    cur = con.cursor()
    sql = "select * from funcionario"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])
    con.close()
    return pd_data

def get_func_by_id(frame):
    frame = frame.drop(columns=['func'])
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values, in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_func', 'func'])
    sql = "SELECT * FROM funcionario WHERE ID IN ( "+data_string+" )" 
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])
    con.close()
    return pd_data

def get_cat_by_id(frame):
    frame = frame.drop(columns=['cat'])
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values, in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_func', 'cat'])
    sql = "SELECT * FROM categoria WHERE ID IN ( "+data_string+" )" 
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()

    cur.execute(sql)
    data = cur.fetchall()
    con.close()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    return pd_data
