import pandas as pd
import datetime 
import psycopg2
from instance_a import instance_a
import os

def validate_date(frame):
    invalid_date = frame.loc[frame["data"] > datetime.date.today()]
    
    if not invalid_date.empty:
        create_invalid_dates(invalid_date)

    return frame.loc[frame["data"] <= datetime.date.today()]


def validate_func(frame):
    invalid_func = frame.loc[frame['func'] == '' ]

    if not invalid_func.empty:
        create_invalid_func(invalid_func)

    return frame.loc[frame['func'] != '' ]

def create_invalid_dates(frame):
    host =  'localhost'
    db =  'rio'
    user =  'postgres'
    password = "root"
    con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)
    cur = con.cursor()

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            VALIDAR_DATAS (
                ID_VENDA INTEGER,
                FUNCIONARIO VARCHAR(1024),
                CATEGORIA VARCHAR(1024),
                DATA_VENDA DATE,
                VENDA INTEGER
            )
    ''')

    con.commit()
    data = list(frame.itertuples(index=False, name=None))
    data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
    cur.execute('INSERT INTO VALIDAR_DATAS VALUES ' + str(data_string))
    con.commit()
    con.close()


def create_invalid_func(frame):
    host =  'localhost'
    db = 'rio'
    user =  'postgres'
    password = "root"
    con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)
    cur = con.cursor()

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            VALIDAR_FUNC (
                ID_VENDA INTEGER,
                CATEGORIA VARCHAR(1024),
                DATA_VENDA DATE,
                VENDA INTEGER
            )
    ''')

    con.commit()
    frame = frame.drop(columns=['func'])
    data = list(frame.itertuples(index=False, name=None))
    data_string = ','.join(cur.mogrify("(%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
    cur.execute('INSERT INTO VALIDAR_FUNC VALUES ' + str(data_string))
    con.commit()
    con.close()


def verify_difs():

    host ='localhost'
    db = 'rio'
    user = 'postgres'
    password = "root"
    con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)
    cur = con.cursor()
    sql = "select id_venda from vendas"
    cur.execute(sql)
    data = cur.fetchall()
    con.close()
    pd_data_c = pd.DataFrame(data, columns =['id_venda'])


    host = os.getenv('HOST_A')
    db = os.getenv("DB")
    user = os.getenv("PSTG_USER")
    password = os.getenv("PSTG_PSW")
    con = psycopg2.connect(host=host, database=db, user=user, password=password)
    cur = con.cursor()
    sql = "select id_venda from venda"
    cur.execute(sql)
    data = cur.fetchall()
    pd_data_a = pd.DataFrame(data, columns =['id_venda'])
    

    frame = pd.concat([pd_data_c,pd_data_a]).drop_duplicates(keep=False)

    data = list(frame.itertuples(index=False, name=None))

    data_string = ','.join( str(values) for values in data)
    if data_string == "":
        return pd.DataFrame( columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    sql = "SELECT * FROM VENDA WHERE ID_VENDA IN ( '' )" 

    cur.execute(sql)
    data = cur.fetchall()
    pd_data = pd.DataFrame(data, columns =['id_venda', 'id_func', 'id_cat', 'data','venda'])
    con.close()

    return pd_data
