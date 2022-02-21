import pandas as pd
import datetime 
import psycopg2
from instance_a import *
from instance_c import *
from instance_b import *

import os

def verify_difs_vendas():
    pd_data_c = check_id_vend_c()
    pd_data_a = check_id_vend_a()
    frame = pd.concat([pd_data_c,pd_data_a]).drop_duplicates(keep=False)
    pd_data = get_vendas_by_id(frame)
    return pd_data

def verify_difs_func(func_frame):
    pd_func = func_frame
    pd_func_c = check_funcs()
    frame = pd.concat([pd_func,pd_func_c]).drop_duplicates(keep=False)
    pd_data = get_func_by_id(frame)
    return pd_data

def verify_difs_cat(cat_frame):
    pd_cat = cat_frame
    pd_cat_c = check_cats()
    frame = pd.concat([pd_cat,pd_cat_c]).drop_duplicates(keep=False)
    pd_data = get_cat_by_id(frame)
    return pd_data

def verify_non_func(frame):
    non_func = frame[frame['func'].isna()]
    valid_func = frame[frame['func'].notna()]
    if non_func.empty and len(non_func) > 0:
        #func
        print(str(len(non_func)) + " Funcionários não definidos")
        return valid_func
    return frame

def verify_non_cat(frame):
    non_cat = frame[frame['cat'].isna()]
    valid_cat = frame[frame['cat'].notna()]
    if non_func.empty:
        #func
        print(str(len(non_cat)) + "Categorias não encontradas")
        return valid_cat
    return frame