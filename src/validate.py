import pandas as pd
import datetime 
from instance_a import instance_a
## Adicionar validação para compras > data atual
def check_date(frame):
    invalid_date = frame.loc[frame["data"] > datetime.date.today()]
    return invalid_date


## adicionar validação para compras sem associação com vendedor
def check_func(frame):
    invalid_func = frame.loc[frame['funcionário'] == '' ]
    return invalid_func


## adicionar validação para linhas duplicadas
    # --  Fazer no pandas com o remove_duplicates()

frame1 = instance_a()
check_date(frame1)