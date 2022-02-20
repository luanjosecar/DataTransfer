import pandas as pd
import datetime 
import psycopg2
from instance_a import *
from instance_c import *
from instance_b import *

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

# def create_invalid_dates(frame):
#     host =  'localhost'
#     db =  'rio'
#     user =  'postgres'
#     password = "root"
#     con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)
#     cur = con.cursor()

#     cur.execute(''' CREATE TABLE IF NOT EXISTS 
#             VALIDAR_DATAS (
#                 ID_VENDA INTEGER,
#                 FUNCIONARIO VARCHAR(1024),
#                 CATEGORIA VARCHAR(1024),
#                 DATA_VENDA DATE,
#                 VENDA INTEGER
#             )
#     ''')

#     con.commit()
#     data = list(frame.itertuples(index=False, name=None))
#     data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
#     cur.execute('INSERT INTO VALIDAR_DATAS VALUES ' + str(data_string))
#     con.commit()
#     con.close()


# def create_invalid_func(frame):
#     host =  'localhost'
#     db = 'rio'
#     user =  'postgres'
#     password = "root"
#     con = psycopg2.connect(host=host, database=db, user=user, password=password, port=8003)
#     cur = con.cursor()

#     cur.execute(''' CREATE TABLE IF NOT EXISTS 
#             VALIDAR_FUNC (
#                 ID_VENDA INTEGER,
#                 CATEGORIA VARCHAR(1024),
#                 DATA_VENDA DATE,
#                 VENDA INTEGER
#             )
#     ''')

#     con.commit()
#     frame = frame.drop(columns=['func'])
#     data = list(frame.itertuples(index=False, name=None))
#     data_string = ','.join(cur.mogrify("(%s,%s,%s,%s)",values ).decode('utf-8')for values in data)
#     cur.execute('INSERT INTO VALIDAR_FUNC VALUES ' + str(data_string))
#     con.commit()
#     con.close()


def verify_difs_vendas():
    pd_data_c = check_id_vend_c()
    pd_data_a = check_id_vend_a()
    frame = pd.concat([pd_data_c,pd_data_a]).drop_duplicates(keep=False)
    pd_data = get_data_by_id(frame)
    return pd_data

def verify_difs_func():
    pd_func = get_funcs()
    pd_func_c = check_funcs()
    frame = pd.concat([pd_func,pd_func_c]).drop_duplicates(keep=False)
    pd_data = get_data_by_id(frame)
    return pd_data

def verify_difs_cat():
    pd_cat = get_cat()
    pd_cat_c = check_cats()
    frame = pd.concat([pd_cat,pd_cat_c]).drop_duplicates(keep=False)
    pd_data = get_data_by_id(frame)
    return pd_data