import psycopg2
import datetime
import random
import names
import time
import schedule

def data_1():

    con = psycopg2.connect(host='localhost', database='desafio', user='postgres', password='root', port=8005)
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS VENDA")
    con.commit()

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            VENDA (
                ID_VENDA INTEGER,
                ID_FUNCIONARIO INTEGER,
                ID_CATEGORIA INTEGER,
                DATA_VENDA DATE,
                VENDA INTEGER
            )
        ''')
    con.commit()
    con.close()

def data_2():

    con = psycopg2.connect(host='localhost', database='desafio', user='postgres', password='root', port=8004)
    cur = con.cursor()


    cur.execute("DROP TABLE IF EXISTS FUNCIONARIO")
    con.commit()

    cur.execute("DROP TABLE IF EXISTS CATEGORIA")
    con.commit()

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            FUNCIONARIO (
                ID INTEGER,
                NOME_FUNCIONARIO VARCHAR(1024)
            )
        ''')
    con.commit()

    cur.execute(''' CREATE TABLE IF NOT EXISTS 
            CATEGORIA (
                ID INTEGER,
                NOME_CATEGORIA VARCHAR(1024)
            )
        ''')

    con.commit()
    con.close()


def create_mock_a():

    con = psycopg2.connect(host='localhost', database='desafio', user='postgres', password='root', port=8005)
    cur = con.cursor()

    print("Adicionando valores em Vendas")
    sql = "INSERT INTO VENDA VALUES (" + str(random.randint(0, 1000)) +","+ str(random.randint(0, 1000)) + ","+ str(random.randint(0, 1000)) +", '" + str(datetime.date.today()) + "'," + str(random.randint(0, 1000)) + " )"
    cur.execute(sql)
    con.commit()
    con.close()

def create_mock_b():

    con = psycopg2.connect(host='localhost', database='desafio', user='postgres', password='root', port=8004)
    cur = con.cursor()
    print("Adicionando valores em Funcionarios")
    sql = "INSERT INTO FUNCIONARIO VALUES (" + str(random.randint(0, 1000)) +",'"+ str(names.get_full_name()) + "')"
    cur.execute(sql)
    con.commit()

    print("Adicionando valores em Categorias")
    sql = "INSERT INTO CATEGORIA VALUES (" + str(random.randint(0, 1000)) +",'"+ str(names.get_first_name()) + "')"
    cur.execute(sql)
    con.commit()
    con.close()
    


print("Criando tabelas auxiliares")
data_1()
data_2()

print("Iniciando o processo de MockData")




def create_data():
    create_mock_a()
    create_mock_b()
    print("--------------------------------------")

while True:
    create_data()
    time.sleep(10)

