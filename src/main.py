import pandas as pd
import os
import schedule
import time

from instance_a import *
from instance_b import *
from instance_c import *
from validate import *

import os
from dotenv import load_dotenv

load_dotenv()

TIMER = os.getenv('TIMER')

def join_info(frame1, frame2, frame3):
    frame = pd.merge(frame1,frame2,on="id_cat", how='left')
    frame = pd.merge(frame,frame3,on="id_func", how='left')
    frame = frame.drop(columns=['id_cat', 'id_func'])
    frame = frame[['id_venda', 'func', 'cat', 'data', 'venda']]
    return frame

def feed_table():
    try:
        create_bases()

        frame_1 = verify_difs_vendas()

        frame_2 = get_cats()
        frame_3 = get_funcs()

        frame_vendas = join_info(frame_1, frame_2, frame_3)
        frame_vendas = frame_vendas.drop_duplicates()
        print(str(len(frame_vendas)) + " Itens novos encontrados na tablea VENDA")

        print("Adicionando itens a Tabela Vendas \n\n")


        add_vend_table(frame_vendas)

        frame_func = verify_difs_func(frame_3)
        frame_func = frame_func.drop_duplicates()
        print(str(len(frame_func)) + " Itens novos encontrados na tablea FUNCIONARIOS")
        print("Adicionando itens a Tabela FUNCIONARIOS \n\n")
        add_func_table(frame_func)

        frame_cat = verify_difs_cat(frame_2)

        frame_cat = frame_cat.drop_duplicates()    
        print(str(len(frame_func)) + " Itens novos encontrados na tablea CATEGORIAS")

        print("Adicionando itens a Tabela CATEGORIAS \n\n")
        add_cat_table(frame_cat)
    except Exception as e:
        print("Erro ao realizar operação")
        print(str(e))
        print("Tentando novamente em 5 segundos")
        time.sleep(5)
        feed_table()
    print("---- Operação realizada com sucesso, Banco de dados Atualizado -----")



if __name__ == "__main__":
    schedule.every().day.at(TIMER).do(feed_table)
    while True:
        schedule.run_pending()
        time.sleep(10)


    
