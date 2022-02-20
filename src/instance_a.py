import psycopg2
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST_A')
db = os.getenv("DB")
user = os.getenv("PSTG_USER")
password = os.getenv("PSTG_PSW")
port = os.getenv("PORT_A")

con = psycopg2.connect(host=host, database=db, user=user, password=password, port=port)
cur = con.cursor()

def instance_a():
    sql = "select * from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    print(pd_data.head())
    return pd_data

def instance_time(time):
    sql = "select * from venda where data_venda > " + time
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    return pd_data

def check_id_vend_a():
    sql = "select id_venda from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda'])
    return pd_data

def get_vendas_by_id(frame):
    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values, in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])

    sql = "SELECT * FROM VENDA WHERE ID_VENDA IN ( "+data_string+" )" 
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    return pd_data