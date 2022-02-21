import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


HOST = os.getenv('HOST_A')
DB = os.getenv("DB")
USER = os.getenv("PSTG_USER")
PASSWORD = os.getenv("PSTG_PSW")
PORT = os.getenv("PORT_A")

def instance_a():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select * from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    
    con.close()
    return pd_data

def instance_time(time):
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select * from venda where data_venda > " + time
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    con.close()
    return pd_data

def check_id_vend_a():
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    sql = "select id_venda from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda'])
    con.close()
    return pd_data

def get_vendas_by_id(frame):
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values, in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])

    sql = "SELECT * FROM VENDA WHERE ID_VENDA IN ( "+data_string+" )" 
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    con.close()
    return pd_data

def get_vendas_great_id(max_id):
    sql = "SELECT * FROM VENDA WHERE ID_VENDA > "+data_string+"" 
    con = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD, port=PORT)
    cur = con.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    con.close()
    return pd_data