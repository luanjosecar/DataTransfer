import pandas as pd
import datetime 
from instance_a import instance_a
import os
## Adicionar validação para compras > data atual
def validate_date(frame):
    invalid_date = frame.loc[frame["data"] > datetime.date.today()]
    if not invalid_date.empty:
        create_invalid_dates(invalid_date)


## adicionar validação para compras sem associação com vendedor
def validate_func(frame):
    invalid_func = frame.loc[frame['funcionário'] == '' ]
    return invalid_func

def create_invalid_dates(frame):
    host = os.getenv('HOST_C') | 'localhost'
    db = os.getenv("DB_c") | 'rio'
    user = os.getenv("PSTG_USER_c") | 'postgres'
    password = os.getenv("PSTG_PSW_c") | "root"
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


def create_invalid_func():
    pass