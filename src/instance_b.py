import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST_B')
db = os.getenv("DB")
user = os.getenv("PSTG_USER")
password = os.getenv("PSTG_PSW")


con = psycopg2.connect(host=host, database=db, user=user, password=password)
cur = con.cursor()

def get_cats():
    sql = "select * from categoria"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    return pd_data


def get_funcs():
    cur = con.cursor()
    sql = "select * from funcionario"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])

    return pd_data

def get_func_by_id(data):
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_func', 'func'])
    sql = "SELECT * FROM funcionario WHERE ID_VENDA IN ( "+data_string+" )" 

    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])
    return pd_data

def get_cat_by_id(data):
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_func', 'cat'])
    sql = "SELECT * FROM categoria WHERE ID_VENDA IN ( "+data_string+" )" 

    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'cat'])
    return pd_data
