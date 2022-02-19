import pandas as pd
import datetime 
from instance_a import instance_a
## Adicionar validação para compras > data atual
def validate_date(frame):
    invalid_date = frame.loc[frame["data"] > datetime.date.today()]
    if invalid_date.empty:
        create_invalid_dates()


## adicionar validação para compras sem associação com vendedor
def validate_func(frame):
    invalid_func = frame.loc[frame['funcionário'] == '' ]
    return invalid_func

def create_invalid_dates():
    pass

def create_invalid_func():
    pass