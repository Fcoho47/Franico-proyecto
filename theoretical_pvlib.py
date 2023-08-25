#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

import logging
from utils.data import module_degradation_factor, meteomatics_meteo_data, get_losses, get_inverter_parameters, db_module_data
from database.database import SolarityDB
from database.queries import *
from database.queries import PLANTS_STRINGS_QUERY_TEMPLATE

import pandas as pd
from datetime import datetime, timedelta
from pvlib import location, temperature, modelchain, pvsystem

import numpy as np
import re
np.seterr(divide='ignore')


logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROYECCION_PLANTA_TABLE_NAME = 'proyeccionGeneracion'
POTENCIA_TEORICA_TABLE_NAME = 'potenciaTeoricaPlanta'
POTENCIA_TEORICA_EQUIPO_TABLE_NAME = 'potenciaTeoricaEquipo'

PROYECCION_EQUIPO_TABLE_NAME = 'proyeccionGeneracionEquipo'
# PROYECCION_EQUIPO_TABLE_NAME = 'proyeccionGeneracionEquipo_v2'

DAYS_TIME_WINDOW = 27
def power_to_kwh_energy(col): return np.sum(col)*0.5/1000.0

# Para calculo de energia incidente en plano inclinado


def for_poa_energy(dataframe, idx, arrays):
    dataframe['string'] = idx
    dataframe['kWp'] = arrays[idx].module_parameters.STC * \
        arrays[idx].modules_per_string / 1000.0
    return dataframe[['string', 'kWp', 'poa_global', 'poa_direct']]


# Conexión a base de datos
solarityDB = SolarityDB()

# Strings e inversores de las plantas
all_plants_strings_df = solarityDB.query_to_df(PLANTS_STRINGS_QUERY_TEMPLATE)
all_plants_devices_df = solarityDB.query_to_df(PLANTS_DEVICES_QUERY_TEMPLATE)

#all_plants_strings_df.to_excel("all_plants_strings_df.xlsx")
#all_plants_devices_df.to_excel("all_plants_devices_df.xlsx")


# Query de plantas con latitud y longitud distinto a null (es decir, que tienen asociados datos de radiacion)
plants_df = solarityDB.query_to_df(PLANTS_QUERY)
plants_df.index = plants_df['nombre']
del plants_df['nombre']

temperature_parameters = {'Rooftop': temperature.TEMPERATURE_MODEL_PARAMETERS['sapm']['insulated_back_glass_polymer'],
                          'Ground mount': temperature.TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_polymer']}

#plants_df.to_excel("plants_df.xlsx")

def theoretical_metrics(plant_name: str, start_time: str, end_time: str):

    ## Datos de la planta y fechas
    plant_data = plants_df.loc[plant_name]
    latitud, longitud, plant_id = plant_data['latitud'], plant_data['longitud'], plant_data['id']
    site_location = location.Location(latitud, longitud)
    plant_losses = get_losses(plant_name=plant_name)

    # El timestamp de inicio corresponde al primer dato de Solcast de la planta
    try:
        plant_PEM_time = solarityDB.query_to_df(SOLCAST_FIRST_DATA_QUERY_TEMPLATE.format(
            plant_name=plant_name)).loc[0, 'period_end']
    except (IndexError, KeyError):
        logging.warning("No hay datos de radiación, ABORTANDO...")
        return

    start_datetime = datetime.strftime(start_time, '%Y-%m-%d')
    end_datetime = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')

    ################### DATOS DE STRINGS ################
    # Filtrar strings y dispositivos de la planta
    plant_strings = all_plants_strings_df[all_plants_strings_df['nombre_planta'] == plant_name]
    plant_devices = all_plants_devices_df[all_plants_devices_df['nombre_planta'] == plant_name]

    print(all_plants_strings_df)

    #print(plant_strings)
    #print(plant_devices)

    if plant_strings.empty:
        logging.warning(f"{plant_name} sin datos de STRINGS, abortando...")
        return

    ################### DATOS METEOROLOGICOS, DEGRADACION DE PANELES #########################
    solcast_data_query = SOLCAST_DATA_QUERY_TEMPLATE.format(plant_name=plant_name, start_datetime=start_datetime, end_datetime=end_datetime)
    solcast_data = solarityDB.query_to_df(solcast_data_query)
    solcast_data.set_index('period_end', inplace=True)

    times = pd.DatetimeIndex(solcast_data.index)
    if times.empty:
        logging.warning("Iterando en dataframe de tiempo vacío, ABORTANDO...")
        return

    # Creacion de dataframe de datos meteorologicos (mezcla entre meteomatics y datos de solcast)
    meteomatics_timeseries = meteomatics_meteo_data(plant_id=plant_id, times=times, solarity_db_conn=solarityDB)
    columns_renaming = {'air_temp': 'temp_air'}
    weather_df = solcast_data.join(meteomatics_timeseries, how='inner')
    weather_df = weather_df.rename(columns=columns_renaming)

    # Lista con degradacion horaria de los modulos
    module_degradation = list(map(lambda c_time: module_degradation_factor(plant_PEM_time, c_time), pd.DatetimeIndex(weather_df.index)))

    print(module_degradation)

    ############################ ITERACION POR CADA INVERSOR ###########################
    full_iteration = True
    inverters_power = []
    inverters_poa = []

    for _, device_data in plant_devices.iterrows():
        serial = device_data.numeroSerie
        id_equipo = device_data.id_equipo

        inverter_ac_power = device_data.potencia_ac
        inverter_model = device_data.modeloCecInversor

        ############### CREACION DE ARRAYS ###############################
        inverter_strings = plant_strings[plant_strings.numeroSerie == serial]

        if 'Not assigned' in list(inverter_strings.modeloCecPanel):
            logging.warning(f"{plant_name} sin datos de modelo de modulo")
            logging.warning("Abortando")
            full_iteration = False
            break

        arrays = [pvsystem.Array(mount=pvsystem.FixedMount(surface_tilt=array_data[1].inclinacion,
                                                           surface_azimuth=array_data[1].azimuth),
                                 module_parameters=db_module_data(array_data[1].modeloCecPanel),
                                 temperature_model_parameters=temperature_parameters[array_data[1].montaje],
                                 strings=1,
                                 modules_per_string=array_data[1].cantidad)
                  for array_data in inverter_strings.iterrows()]
        
        print("ARRAYS")
        print(arrays)

        print(db_module_data(inverter_strings.iloc[1].modeloCecPanel))

        if not arrays:
            full_iteration = False
            logging.warning(
                f"{plant_name} sin datos de arrays, equipo SN: {serial}")
            logging.warning("ABORTANDO")
            break

        # Creacion de sistema compuesto por un inversor
        ac_model, inverter_data = get_inverter_parameters(inverter_model_name=inverter_model,  inverter_ac_power=inverter_ac_power)
        inverter_system = pvsystem.PVSystem(arrays=arrays, inverter_parameters=inverter_data)


        # Asignacion de perdidas para modelo
        for key in plant_losses.keys():
            inverter_system.losses_parameters[key] = plant_losses[key]

        mc = modelchain.ModelChain(inverter_system, site_location,
                                   aoi_model='physical',
                                   spectral_model='no_loss',
                                   ac_model=ac_model,
                                   losses_model='pvwatts',
                                   name=f'{plant_name}-{serial}')
        
    
        mc.run_model(weather=weather_df, module_degradation=module_degradation)


        ###### Para calculo de energia incidente en plano receptor #######
        if type(mc.results.total_irrad) == tuple:
            arrays_total_irrad = mc.results.total_irrad
        else:
            arrays_total_irrad = [mc.results.total_irrad]

        arrays_poa = pd.concat([for_poa_energy(result, idx, arrays)
                                for idx, result in enumerate(arrays_total_irrad)])

        arrays_poa['energia_incidente_poa'] = arrays_poa['kWp'] * arrays_poa['poa_global'] / 2000.0
        inverters_poa.append(arrays_poa.groupby(pd.Grouper(freq='1D')).agg({'energia_incidente_poa': 'sum'}).copy())

        # Concatenacion de datos de potencia
        inverter_power = pd.DataFrame(mc.results.ac.copy())
        inverter_power['id_equipo'] = id_equipo
        inverters_power.append(inverter_power)

        break

    ########################### SUBIDA DE DATOS A DB ####################################
    
    if full_iteration:
        inverters_power_dfs = pd.concat(inverters_power)
        inverters_power_dfs = inverters_power_dfs.rename(columns={'p_mp': 'valorTeoricoModelChain', 0: 'valorTeoricoModelChain'})
        inverters_power_dfs = inverters_power_dfs[['valorTeoricoModelChain', 'id_equipo']]

        # Dejar en 0 valores negativos
        # inverters_power_dfs['valorTeoricoModelChain'][inverters_power_dfs['valorTeoricoModelChain'] < 0] = 0.0
        inverters_power_dfs.loc[inverters_power_dfs['valorTeoricoModelChain'] < 0, 'valorTeoricoModelChain'] = 0.0
        plant_power = inverters_power_dfs.groupby(pd.Grouper(freq='30Min')).agg({'valorTeoricoModelChain': 'sum'})

        ################ SUBIDA DE DATOS DE POTENCIA #############
        plant_power['id_planta'] = plant_id
        plant_power.rename(columns={'valorTeoricoModelChain': 'valorTeorico'}, inplace=True)
        f_plant_power = solarityDB.format_dataframe_to_DB_upload(dataframe=plant_power,  time_format="%Y-%m-%d %H:%M:%S",
                                                                 float_headers=['valorTeorico'], int_headers=['id_planta'])
        solarityDB.upload_df_to_DB(f_plant_power, POTENCIA_TEORICA_TABLE_NAME)

        ############## SUBIDA DE DATOS DE GENERACION #############
        plant_power['valorTeorico'] = plant_power['valorTeorico']/(2.0*1000.0)
        plant_energy = plant_power.groupby(pd.Grouper(freq='D')).agg({'valorTeorico': 'sum'})
        plant_energy['id_planta'] = plant_id

        # Energia incidente en plano inclinado
        poa_energy = pd.concat(inverters_poa).groupby(
            pd.Grouper(freq='1D')).agg({'energia_incidente_poa': 'sum'})

        plant_energy = plant_energy.join(poa_energy)
        plant_energy.rename(columns={'energia_incidente_poa': 'energiaIncidentePOA'}, inplace=True)

        f_plant_energy = solarityDB.format_dataframe_to_DB_upload(dataframe=plant_energy,  time_format="%Y-%m-%d",
                                                                  float_headers=['valorTeorico', 'energiaIncidentePOA'], int_headers=['id_planta'])

        solarityDB.upload_df_to_DB(
            f_plant_energy, PROYECCION_PLANTA_TABLE_NAME)

        # SUBIDA DE DATOS DE POTENCIA TEORICA POR EQUIPO
        inverters_power_dfs = inverters_power_dfs.rename(columns={'valorTeoricoModelChain': 'potenciaTeorica'})
        inverters_power_dfs.index.name = 'fecha'
        f_inverters_power = solarityDB.format_dataframe_to_DB_upload(dataframe=inverters_power_dfs, time_format="%Y-%m-%d %H:%M:%S",
                                                                     float_headers=['potenciaTeorica'], int_headers=['id_equipo'])

        print(f_inverters_power)
        solarityDB.upload_df_to_DB(
            f_inverters_power, POTENCIA_TEORICA_EQUIPO_TABLE_NAME)

        # SUBIDA DE DATOS DE ENERGIA TEORICA POR EQUIPO
        # inverters_power_dfs = inverters_power_dfs.set_index('period_end')
        inverters_daily_energy = inverters_power_dfs.groupby(
            [pd.Grouper(key='id_equipo'), pd.Grouper(freq='1D')]).agg(power_to_kwh_energy)
        inverters_daily_energy = inverters_daily_energy.reset_index(level=['id_equipo'])

        # Renaming
        inverters_daily_energy = inverters_daily_energy.rename(columns={'potenciaTeorica': 'valor'})
        f_inverters_daily_energy = solarityDB.format_dataframe_to_DB_upload(dataframe=inverters_daily_energy,
                                                                            time_format="%Y-%m-%d", float_headers=['valor'], int_headers=['id_equipo'])

        solarityDB.upload_df_to_DB(
            f_inverters_daily_energy, PROYECCION_EQUIPO_TABLE_NAME)



if __name__ == '__main__':

    print("here")
    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0)
    start_time = end_time - timedelta(days=DAYS_TIME_WINDOW)

    start_time = datetime(2023, 5, 1)
    end_time = datetime(2023, 5, 5, 0)

    for plant_name in plants_df.index:
        if plant_name != 'SODIMAC HC ÑUBLE':
            continue

        # if not re.match('^TRES SOLES', plant_name): continue
        logger.warning(plant_name)
        theoretical_metrics(plant_name=plant_name,
                            start_time=start_time, end_time=end_time)
