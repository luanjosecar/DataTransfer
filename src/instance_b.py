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

def intance_b_cat():
    sql = "select * from categoria"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_cat', 'cat'])
    return pd_data


def intance_b_func():
    cur = con.cursor()
    sql = "select * from funcionario"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_func', 'func'])

    return pd_data