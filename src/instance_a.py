import psycopg2
import pandas as pd


import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST_A')
db = os.getenv("DB")
user = os.getenv("PSTG_USER")
password = os.getenv("PSTG_PSW")


con = psycopg2.connect(host=host, database=db, user=user, password=password)
cur = con.cursor()

def instance_a():
    sql = "select * from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    return pd_data

def instance_time(time):
    sql = "select * from venda where data_venda > " + time
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    return pd_data