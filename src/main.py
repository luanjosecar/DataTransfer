import pandas as pd
from instance_a import *
from instance_b import *
from instance_c import *

def join_info(frame1, frame2, frame3):
    frame = pd.merge(frame1,frame2,on="id_cat")
    frame = pd.merge(frame,frame3,on="id_func")
    frame = frame.drop(columns=['id_cat', 'id_func'])
    frame = frame[['id_venda', 'func', 'cat', 'data', 'venda']]
    return frame

def pd_to_tuples(frame):
    data = frame.itertuples(index=False, name=None)
    return list(data)

def feed_all():
    
    frame1 = instance_a()
    frame2 = intance_b_cat()
    frame3 = intance_b_func()

    frame = join_info(frame1, frame2, frame3)

    print(frame.head())

    data = pd_to_tuples(frame)
    input_data(data)

def get_data(frame):
    return frame.data.max()