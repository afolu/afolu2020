#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
from numpy import arange
from datetime import datetime


sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_data(esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None):
    """
    Funcion para traer la inforamcion de la base de datos corresponiente a los datos de de actividad y especie
    para los cálculos de las absorciones y emiones del modulo de plantaciones forestales
    :param esp: Especies para las cuales se va a hacer los cálculos  acorde con la tabla de especies
                Ej. [69, 158] 69: Cordia alliodora, 158: Pinus patula
    :param sub_reg: Sub region con las cuales se va a hacer los cálculos acorde con la tabla de regiones
                Ej. [1, 3] 1: región Andina, 3: región Caribe
    :param z_upra: Zona UPRA con las cuales se va a hacer los cálculos acorde con la tabla de zona UPRA
                Ej. [1, 3] 1: Eje Cafetero, 3: Orinoquia
    :param dpto: Departamento con los cuales se va a hacer los cálculos acorde con la tabla de departamentos
                Ej. [99, 17] 99: Vichada, 17: Caldas
    :param muni: Municipios con los cuales se va a hacer los cálculos acorde con la tabla de municipios
                Ej. [5001, 13838] 5001: Medellin, 17: Turbaná
    :return: Tabla con calulos de emisiones y absorciones brutas y netas del modulo de plantaciones forestales
    :return: tabla de datos correspondiente al resultado de la consulta de la base de datos
    """
    query = """SELECT DA.id
               ,DA.id_subregion
               ,DA.id_zona_upra
               ,DA.cod_depto
               ,DA.cod_muni
               ,DA.ano_establecimiento
               ,DA.id_especie
               ,ESP.turno
               ,DA.hectareas 
               ,ESP.factor_cap_carb_ba
               ,ESP.factor_cap_carb_bt
               FROM b_especie as ESP
               INNER JOIN b1aiii_datos_actividad as DA ON DA.id_especie = ESP.id
           """
    if esp:
        esp_query = """WHERE DA.id_especie in {} """.format(str(esp).replace('[', '(').replace(']', ')'))
        query = query + esp_query
    if sub_reg:
        sub_reg_query = """AND DA.id_subregion in {} """.format(str(sub_reg).replace('[', '(').replace(']', ')'))
        query = query + sub_reg_query
    if z_upra:
        z_upra_query = """AND DA.id_zona_upra in {} """.format(str(z_upra).replace('[', '(').replace(']', ')'))
        query = query + z_upra_query
    if dpto:
        dpto_query = """AND DA.cod_depto in {} """.format(str(dpto).replace('[', '(').replace(']', ')'))
        query = query + dpto_query
    if muni:
        muni_query = """AND DA.muni in {} """.format(str(muni).replace('[', '(').replace(']', ')'))
        query = query + muni_query

    df = pd.read_sql_query(query, con=pg_connection_str())
    return df


def years_calc(ano_est, year, all=False):
    """
    Calculo del numero de años que tiene la plantacion forestal a partir del año de establecimiento y el número de años
    para el calculo de las absorciones totales en el rango de años solicitado
    :param all: Para consultar todos los años desde el año de establecimiento de la plantación forestal
    :param ano_est: Año de establecimiento del cultivo. Ej. 2004
    :param year: Rango de años para los cuales se quiere calcular las absorciones  Ej. [2000, 2011]
    :return:Numero de años para los cuales la plantacion forestal genera absorciones.
    """
    if (len(year) == 1) and (year[0] == ano_est) and (all is True):
        year_min = ano_est
        year_max = datetime.today().year
        return year_max - year_min
    else:
        year_min, year_max = year[0], year[-1]

    if (len(year) == 1) and (year_max < ano_est):
        return 0
    elif (len(year) == 1) and (year_max > ano_est):
        return 1
    if (year_min <= ano_est) and (year_max > ano_est):
        return year_max - ano_est
    elif year_min > ano_est:
        return year_max - year_min
    elif year_max == ano_est:
        return 1


def turns(ano_est, turno, year_min, year_max):
    """
    Funcion para  calcular el turno de las diferentes plantaciones forestales
    :param ano_est: Año de establecimiento del cultivo. Ej. 2004
    :param turno: Turno de la plantacion forestal. Ej. 20
    :param year_min: Año menor del rango de años con los cuales se va a hacer los cálculos Ej. 2000
    :param year_max: Año mayor del rango de años con los cuales se va a hacer los cálculos Ej. 2011
    :return: numero de turnos que ha completado la plantacion forestal en el rango de fechas establecido
    """
    assert year_max >= year_min, 'El rango de consulta debe ser año inferior y año superior. Por favor verificar ' \
                                 'años consultados'
    turnos = [int(ano_est + turno * i) for i in range(1000)]
    turnos.pop(0)
    if year_max == year_min:
        years_range = [year_max]
    else:
        years_range = arange(year_min, year_max)
    matches = set(turnos).intersection(years_range)
    return len(matches)


def forest_emissions(year=None, esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None):
    """
    Calculo de las emisiones y absorciones brutas y netas del modulo de plantaciones forestales
    :param year: rango de años para los cuales se va a hacer los cálculos  Ej. [2000, 2018] o [2002]
    :param esp: Especies para las cuales se va a hacer los cálculos  acorde con la tabla de especies
                Ej. [69, 158] 69: Cordia alliodora, 158: Pinus patula
    :param sub_reg: Sub region con las cuales se va a hacer los cálculos acorde con la tabla de regiones
                Ej. [1, 3] 1: región Andina, 3: región Caribe
    :param z_upra: Zona UPRA con las cuales se va a hacer los cálculos acorde con la tabla de zona UPRA
                Ej. [1, 3] 1: Eje Cafetero, 3: Orinoquia
    :param dpto: Departamento con los cuales se va a hacer los cálculos acorde con la tabla de departamentos
                Ej. [99, 17] 99: Vichada, 17: Caldas
    :param muni: Municipios con los cuales se va a hacer los cálculos acorde con la tabla de municipios
                Ej. [5001, 13838] 5001: Medellin, 17: Turbaná
    :return: Tabla con calulos de emisiones y absorciones brutas y netas del modulo de plantaciones forestales
    """
    df = get_data(esp=esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni)
    df['abs_BA_year'] = df['factor_cap_carb_ba'] * df['hectareas']
    df['abs_BT_year'] = df['factor_cap_carb_bt'] * df['hectareas']
    if (year[0] == 'all') or (year is None):
        df['cant'] = df['ano_establecimiento'].apply(lambda x: years_calc(x, year=[x], all=True))
        df['accum'] = - df['ano_establecimiento'] - 1

    else:
        df['cant'] = df['ano_establecimiento'].apply(lambda x: years_calc(x, year=year))
        df['accum'] = - df['ano_establecimiento'] + year[0]

    df['accum'].loc[df['accum'] < 0] = 0
    df['abs_BA_consulta'] = - df['abs_BA_year'] * df['cant']
    df['abs_BT_consulta'] = - df['abs_BT_year'] * df['cant']
    df['abs_BA_accum_con'] = - df['abs_BA_year'] * df['accum']
    df['abs_BT_accum_con'] = - df['abs_BT_year'] * df['accum']
    df['abs_BA_year'] = df['factor_cap_carb_ba'] * df['hectareas']
    df['abs_BT_year'] = df['factor_cap_carb_bt'] * df['hectareas']
    if year[0] == 'all':
        year_max = datetime.today().year
        df['turnos'] = df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                year_max=year_max, year_min=x['ano_establecimiento']), axis=1)
    else:
        df['turnos'] = df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'], year_max=year[-1],
                                                year_min=year[0]), axis=1)
    df['ems_BA_consulta'] = df['abs_BA_year'] * df['turnos'] * df['turno']
    df['ems_BT_consulta'] = df['abs_BT_year'] * df['turnos'] * df['turno']
    df['ems_BA_neta'] = df['abs_BA_consulta'] + df['ems_BA_consulta']
    df['ems_BT_neta'] = df['abs_BT_consulta'] + df['ems_BT_consulta']
    df = df.fillna(0)
    df.to_sql('land_use_res', con=pg_connection_str(), index=False, if_exists='replace', method="multi", chunksize=5000)
    print('Done')


def main():
    forest_emissions(year=['all'], esp=[95], dpto=[25])


if __name__ == '__main__':
    main()
