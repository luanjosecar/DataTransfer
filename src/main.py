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

def pd_to_tuples(frame):
    data = frame.itertuples(index=False, name=None)
    return list(data)


def feed_table():

    frame_a = verify_difs()
    
    frame_b = instance_b_cat()
    frame_c = instance_b_func()
    frame = join_info(frame_a, frame_b, frame_c)

    print(frame.head())

    #Remove Duplicadas
    frame = frame.drop_duplicates()

    # Remover datas Invalidas
    frame = validate_date(frame)
    
    # Remover Funcionários Invalidos
    frame = validate_func(frame)


    data = pd_to_tuples(frame)
    input_data(data)




def request_feed():
    create_bases()
    feed_table()

if __name__ == "__main__":
    
    request_feed()


    
