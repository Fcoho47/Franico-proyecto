import pandas as pd
from datetime import datetime
from pvlib import pvsystem
import numpy as np
import os
from os.path import dirname
from .default_losses import DEFAULT_LOSSES

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

parent_path = dirname(dirname(os.path.abspath(__file__)))
file_path = lambda path: os.path.join(parent_path, path)

MODULES_DB_FILEPATH = 'modelling-info/CEC_modules_DB.csv'
PVSYST_LOSSES_FILEPATH = 'modelling-info/losses-pvsyst.xlsx'

###################### DATOS METEOROLOGICOS ###############################

METEO_DATA_QUERY_TEMPLATE =  """SELECT fecha, temperature, pressure, wind_speed, precipitation FROM datosMeteorologicosPlantas dmp 
                                WHERE id_planta = {plant_id}
                                AND fecha >= '{start}'
                                AND fecha <= '{end}';
                                """

def meteomatics_meteo_data(plant_id, times, solarity_db_conn, resample_step='30Min'):
    """ 
    Retorna datos meteorologicos asociados a la planta según su plant id en el rango de fechas definido en el parametros times
    """
    start_str = datetime.strftime(times[0], '%Y-%m-%d %H:%M:%S') 
    end_str = datetime.strftime(times[-1], '%Y-%m-%d %H:%M:%S') 
    meteo_data = solarity_db_conn.query_to_df(METEO_DATA_QUERY_TEMPLATE.format(plant_id=plant_id, start=start_str, end=end_str))


    meteo_data = meteo_data.rename(columns={'temperature': 'air_temp', 'fecha': 'period_end'})
    meteo_data.index = pd.DatetimeIndex(meteo_data['period_end']); del meteo_data['period_end']
    meteo_data = meteo_data.resample(resample_step).pad()

    return meteo_data[['wind_speed', 'air_temp']]


###################### DATOS DE BASES DE DATOS PVLIB ###############################

# CEC PV Module and Inverters Database
cec_mod_db = pvsystem.retrieve_sam('CECmod')
cec_mod_db_2 = pd.read_csv(file_path(MODULES_DB_FILEPATH)); cec_mod_db_2 = cec_mod_db_2.set_index('Name')
invdb = pvsystem.retrieve_sam('CECInverter')

def db_module_data(module_name):
    mod_data = {}
    try: 
        mod_data = cec_mod_db[module_name].copy()
    except KeyError:
        try:
            mod_data = cec_mod_db_2.loc[module_name].copy()
        
            for key in mod_data.keys():
                try: mod_data[key] = float(mod_data[key])
                except ValueError: pass
        except:
            logging.warning(f'Imposible encontrar modulo {module_name} en bases de datos')
    
    return mod_data

def get_inverter_parameters(inverter_model_name, inverter_ac_power):
    try: 
        inverter_data = invdb[inverter_model_name]
        ac_model = 'sandia'
    except KeyError:
        ac_model = 'pvwatts'
        inverter_data = {'pdc0': inverter_ac_power*1.02}
    
    return ac_model, inverter_data

###################### DATOS DE BASES DE DATOS PVLIB ###############################

pvsyst_losses = pd.read_excel(file_path(PVSYST_LOSSES_FILEPATH)); pvsyst_losses = pvsyst_losses.set_index('planta')
def get_losses(plant_name):
    try:
        losses = pvsyst_losses.loc[plant_name]
        logging.info(f"{plant_name} sin datos de pérdidas")
    except:
        return DEFAULT_LOSSES

    for key in losses.keys():
        if np.isnan(losses[key]):
            losses[key] = DEFAULT_LOSSES[key]
    
    return losses


def module_degradation_factor(installation_date, current_date, first_year_deg=2.5/100.0, second_year_deg=0.7/100.0):
    elapsed_days = (current_date - installation_date).total_seconds()/(86400)
    elapsed_years = elapsed_days/365

    if elapsed_years < 1:
        degradation = first_year_deg*(elapsed_days/365)
    else:
        degradation = first_year_deg + (elapsed_years - 1)*second_year_deg

    if degradation > 0: return 1 - degradation
    else: return 1