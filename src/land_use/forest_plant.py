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
        muni_query = """AND DA.cod_muni in {} """.format(str(muni).replace('[', '(').replace(']', ')'))
        query = query + muni_query

    df = pd.read_sql_query(query, con=pg_connection_str())
    return df


def years_calc(ano_est, year, all_years=False):
    """
    Calculo del numero de años que tiene la plantacion forestal a partir del año de establecimiento y el número de años
    para el calculo de las absorciones totales en el rango de años solicitado
    :param all_years: Para consultar todos los años desde el año de establecimiento de la plantación forestal
    :param ano_est: Año de establecimiento del cultivo. Ej. 2004
    :param year: Rango de años para los cuales se quiere calcular las absorciones  Ej. [2000, 2011]
    :return:Numero de años para los cuales la plantacion forestal genera absorciones.
    """
    if (len(year) == 1) and (year[0] == ano_est) and (all_years is True):
        year_min = ano_est
        year_max = datetime.today().year
        return year_max - year_min
    else:
        year_min, year_max = min(year), max(year)

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
    years_range = arange(year_min, year_max + 1)
    matches = set(turnos).intersection(years_range)
    return len(matches), list(matches)


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
    # query = create_query(year=year, esp=esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni)
    # df = pd.read_sql_query(query, con=pg_connection_str())
    df = get_data(esp=esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni)
    df['abs_BA'] = - df['factor_cap_carb_ba'] * df['hectareas']
    df['abs_BT'] = - df['factor_cap_carb_bt'] * df['hectareas']
    if year[0] == 'all':
        year_max = datetime.today().year
        df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                    year_max=year_max, year_min=x['ano']), axis=1))
    else:
        df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                    year_max=max(year), year_min=min(year)), axis=1))

    range_years = arange(df['ano_establecimiento'].min(), max(year) + 1)
    multi_index = [(x, y) for x in df['id_especie']. unique() for y in range_years]
    df_ems = pd.DataFrame(data=multi_index, columns=['esp', 'year'])
    df_ems['ems_BA'], df_ems['ems_BT'],  df_ems['ems_BA_accum'],  df_ems['ems_BT_accum'] = 0, 0, 0, 0
    for index, row in df.iterrows():
        if row['turnos']:
            anos = row['matches']
            for ano in anos:
                ems_ba = row['factor_cap_carb_ba'] * row['hectareas'] * row['turno']
                ems_bt = row['factor_cap_carb_bt'] * row['hectareas'] * row['turno']
                df_ems['ems_BA'].loc[(df_ems['esp'] == row['id_especie']) & (df_ems['year'] == ano)] = ems_ba
                df_ems['ems_BT'].loc[(df_ems['esp'] == row['id_especie']) & (df_ems['year'] == ano)] = ems_bt

    floor = df_ems.index // (max(year) - df['ano_establecimiento'].min() + 1)

    df_ems['ems_BA_accum'] = df_ems['ems_BA'].groupby(floor).cumsum()
    df_ems['ems_BT_accum'] = df_ems['ems_BT'].groupby(floor).cumsum()
    df = df.groupby(by=['ano_establecimiento', 'id_especie'])['hectareas', 'abs_BA', 'abs_BT'].sum().\
        sort_values(by=['id_especie'], ascending=False)
    df_ems['abs_BA'], df_ems['abs_BT'], df_ems['hectareas'] = 0, 0, 0
    for year, especie in df.groupby(level=[0, 1]):
        df_ems['abs_BA'].loc[(df_ems['esp'] == year[1]) & (df_ems['year'] == year[0])] = especie['abs_BA'].values
        df_ems['abs_BT'].loc[(df_ems['esp'] == year[1]) & (df_ems['year'] == year[0])] = especie['abs_BT'].values
        df_ems['hectareas'].loc[(df_ems['esp'] == year[1]) & (df_ems['year'] == year[0])] = \
            especie['hectareas'].values
    df_ems['hectareas_accum'] = df_ems['hectareas'].groupby(floor).cumsum()
    df_ems['abs_BA_accum'] = df_ems['abs_BA'].groupby(floor).cumsum()
    df_ems['abs_BT_accum'] = df_ems['abs_BT'].groupby(floor).cumsum()
    df_ems['ems_BA_neta'] = df_ems['abs_BA'] + df_ems['ems_BA']
    df_ems['ems_BT_neta'] = df_ems['abs_BT'] + df_ems['ems_BT']
    df_ems['ems_BA_neta_accum'] = df_ems['abs_BA_accum'] + df_ems['ems_BA_accum']
    df_ems['ems_BT_neta_accum'] = df_ems['abs_BT_accum'] + df_ems['ems_BT_accum']
    df_ems.to_sql('3b1iii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                  method="multi", chunksize=5000)
    print('Done')


def main():
    forest_emissions(year=[2000, 2020], sub_reg=[1])


if __name__ == '__main__':
    main()
