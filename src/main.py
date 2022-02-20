import pandas as pd
import os
import schedule
import time

from instance_a import *
from instance_b import *
from instance_c import *
from validate import *

def join_info(frame1, frame2, frame3):
    frame = pd.merge(frame1,frame2,on="id_cat")
    frame = pd.merge(frame,frame3,on="id_func")
    frame = frame.drop(columns=['id_cat', 'id_func'])
    frame = frame[['id_venda', 'func', 'cat', 'data', 'venda']]
    return frame

def feed_table():
    create_bases()

    frame_1 = verify_difs_vendas()
    frame_2 = get_cats()
    frame_3 = get_funcs()
    frame_vendas = join_info(frame_1, frame_2, frame_3)

    print(frame_vendas.head())

    #Remove Duplicadas
    frame_vendas = frame_vendas.drop_duplicates()

    add_vend_table(frame_vendas)

    frame_func = verify_difs_func()
    add_func_table(frame_func)

    frame_cat = verify_difs_cat()
    add_cat_table(frame_cat)




if __name__ == "__main__":
    
    feed_table()


    
