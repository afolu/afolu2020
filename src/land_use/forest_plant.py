#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import warnings
import pandas as pd
from numpy import arange
from datetime import datetime
from pandas.core.common import SettingWithCopyWarning
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_data(esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None, fue=None, sie=None):
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
               ,DA.id_sistema_siembra
               ,DA.id_fuente
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
    elif z_upra:
        z_upra_query = """AND DA.id_zona_upra in {} """.format(str(z_upra).replace('[', '(').replace(']', ')'))
        query = query + z_upra_query
    elif dpto:
        dpto_query = """AND DA.cod_depto in {} """.format(str(dpto).replace('[', '(').replace(']', ')'))
        query = query + dpto_query
    elif muni:
        muni_query = """AND DA.cod_muni in {} """.format(str(muni).replace('[', '(').replace(']', ')'))
        query = query + muni_query
    if fue:
        muni_query = """AND DA.id_fuente in {} """.format(str(fue).replace('[', '(').replace(']', ')'))
        query = query + muni_query
    if sie:
        muni_query = """AND DA.id_sistema_siembra in {} """.format(str(sie).replace('[', '(').replace(']', ')'))
        query = query + muni_query

    return query


def turns(ano_est, turno, year_max):
    """
    Funcion para  calcular el turno de las diferentes plantaciones forestales
    :param ano_est: Año de establecimiento del cultivo. Ej. 2004
    :param turno: Turno de la plantacion forestal. Ej. 20
    :param year_max: Año mayor del rango de años con los cuales se va a hacer los cálculos Ej. 2011
    :return: numero de turnos que ha completado la plantacion forestal en el rango de fechas establecido
    """
    turnos = [int(ano_est + turno * i) for i in range(1000)]
    turnos.pop(0)
    years_range = arange(ano_est, year_max + 1)
    matches = set(turnos).intersection(years_range)
    return len(matches), list(matches)


def forest_emissions(year=None, esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None, fue=None, sie=None):
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
    query = get_data(esp=esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni, fue=fue, sie=sie)
    df = pd.read_sql_query(query, con=pg_connection_str())
    if not year:
        year_max = datetime.today().year
        range_years = arange(df['ano_establecimiento'].min(), year_max + 1)
        df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                    year_max=year_max),  axis=1))
    try:
        year_max = max(year)
        if not df['ano_establecimiento'].min():
            return print('No Data')
        df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                    year_max=year_max), axis=1))
        range_years = arange(df['ano_establecimiento'].min(), max(year) + 1)
    except ValueError:
        print('No Data')
        return 0
    if sub_reg:
        multi_index = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in df['id_subregion'].unique()]
        df_ems = pd.DataFrame(data=multi_index, columns=['id_especie', 'ano', 'id_subregion'])
    elif z_upra:
        multi_index = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in df['id_zona_upra'].unique()]
        df_ems = pd.DataFrame(data=multi_index, columns=['id_especie', 'ano', 'id_zona_upra'])
    elif dpto:
        multi_index = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in df['cod_depto'].unique()]
        df_ems = pd.DataFrame(data=multi_index, columns=['id_especie', 'ano', 'cod_depto'])
    elif muni:
        multi_index = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in df['cod_muni'].unique()]
        df_ems = pd.DataFrame(data=multi_index, columns=['id_especie', 'ano', 'cod_muni'])

    if (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
        multi_index = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in df['id_subregion'].unique()]
        df_ems = pd.DataFrame(data=multi_index, columns=['id_especie', 'ano', 'id_subregion'])

    df_ems['ems_BA'], df_ems['ems_BT'],  df_ems['ems_BA_accum'],  df_ems['ems_BT_accum'] = 0, 0, 0, 0
    for index, row in df.iterrows():
        if row['turnos']:
            anos = row['matches']
            for ano in anos:
                ems_ba = row['factor_cap_carb_ba'] * row['hectareas'] * row['turno']
                ems_bt = row['factor_cap_carb_bt'] * row['hectareas'] * row['turno']
                if sub_reg:
                    df_ems['ems_BA'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['id_subregion'] == row['id_subregion'])] = ems_ba
                    df_ems['ems_BT'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                         (df_ems['ano'] == ano) &
                                         (df_ems['id_subregion'] == row['id_subregion'])] = ems_bt
                elif z_upra:
                    df_ems['ems_BA'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['id_zona_upra'] == row['id_zona_upra'])] = ems_ba
                    df_ems['ems_BT'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['id_zona_upra'] == row['id_zona_upra'])] = ems_bt
                elif dpto:
                    df_ems['ems_BA'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['cod_depto'] == row['cod_depto'])] = ems_ba
                    df_ems['ems_BT'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['cod_depto'] == row['cod_depto'])] = ems_bt
                elif muni:
                    df_ems['ems_BA'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['cod_muni'] == row['cod_muni'])] = ems_ba
                    df_ems['ems_BT'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['cod_muni'] == row['cod_muni'])] = ems_bt
                elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
                    df_ems['ems_BA'].loc[(df_ems['id_especie'] == row['id_especie']) & (df_ems['ano'] == ano) &
                                         (df_ems['id_subregion'] == row['id_subregion'])] = ems_ba
                    df_ems['ems_BT'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                         (df_ems['ano'] == ano) &
                                         (df_ems['id_subregion'] == row['id_subregion'])] = ems_bt

    if sub_reg:
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
    elif z_upra:
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_zona_upra', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
    elif dpto:
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
    elif muni:
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
    elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
    elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
        df = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                            'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()

    df_ems['hectareas'], df_ems['factor_cap_carb_ba'], df_ems['factor_cap_carb_bt'], df_ems['turno'], = 0, 0, 0, 0

    for index, row in df.iterrows():
        if sub_reg:
            df_ems['hectareas'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                    (df_ems['ano'] == row['ano_establecimiento']) &
                                    (df_ems['id_subregion'] == row['id_subregion'])] = row['hectareas']
            df_ems['factor_cap_carb_ba'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_subregion'] == row['id_subregion'])] = row['factor_cap_carb_ba']
            df_ems['factor_cap_carb_bt'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_subregion'] == row['id_subregion'])] = row['factor_cap_carb_bt']
            df_ems['turno'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                 (df_ems['id_subregion'] == row['id_subregion'])] = row['turno']
        elif z_upra:
            df_ems['hectareas'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                    (df_ems['ano'] == row['ano_establecimiento']) &
                                    (df_ems['id_zona_upra'] == row['id_zona_upra'])] = row['hectareas']
            df_ems['factor_cap_carb_ba'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_zona_upra'] == row['id_zona_upra'])] = row['factor_cap_carb_ba']
            df_ems['factor_cap_carb_bt'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_zona_upra'] == row['id_zona_upra'])] = row['factor_cap_carb_bt']
            df_ems['turno'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                (df_ems['id_zona_upra'] == row['id_zona_upra'])] = row['turno']

        elif dpto:
            df_ems['hectareas'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                    (df_ems['ano'] == row['ano_establecimiento']) &
                                    (df_ems['cod_depto'] == row['cod_depto'])] = row['hectareas']
            df_ems['factor_cap_carb_ba'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['cod_depto'] == row['cod_depto'])] = row['factor_cap_carb_ba']
            df_ems['factor_cap_carb_bt'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['cod_depto'] == row['cod_depto'])] = row['factor_cap_carb_bt']
            df_ems['turno'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                (df_ems['cod_depto'] == row['cod_depto'])] = row['turno']
        elif muni:
            df_ems['cod_muni'].loc[(df_ems['id_especie'] == row['id_especie'])] = row['cod_muni']
            df_ems['hectareas'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                    (df_ems['ano'] == row['ano_establecimiento']) &
                                    (df_ems['cod_muni'] == row['cod_muni'])] = row['hectareas']
            df_ems['factor_cap_carb_ba'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['cod_muni'] == row['cod_muni'])] = row['factor_cap_carb_ba']
            df_ems['factor_cap_carb_bt'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['cod_muni'] == row['cod_muni'])] = row['factor_cap_carb_bt']
            df_ems['turno'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                (df_ems['cod_muni'] == row['cod_muni'])] = row['turno']
        elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
            df_ems['hectareas'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                    (df_ems['ano'] == row['ano_establecimiento']) &
                                    (df_ems['id_subregion'] == row['id_subregion'])] = row['hectareas']
            df_ems['factor_cap_carb_ba'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_subregion'] == row['id_subregion'])] = row['factor_cap_carb_ba']
            df_ems['factor_cap_carb_bt'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                             (df_ems['id_subregion'] == row['id_subregion'])] = row['factor_cap_carb_bt']
            df_ems['turno'].loc[(df_ems['id_especie'] == row['id_especie']) &
                                 (df_ems['id_subregion'] == row['id_subregion'])] = row['turno']

    if sub_reg:
        df_ems = df_ems.sort_values(by=['id_especie', 'id_subregion']).reset_index()
    elif z_upra:
        df_ems = df_ems.sort_values(by=['id_especie', 'id_zona_upra']).reset_index()
    elif dpto:
        df_ems = df_ems.sort_values(by=['id_especie', 'cod_depto']).reset_index()
    elif muni:
        df_ems = df_ems.sort_values(by=['id_especie', 'cod_muni']).reset_index()
    elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
        df_ems = df_ems.sort_values(by=['id_especie', 'id_subregion']).reset_index()

    floor = df_ems.index // (year_max - df['ano_establecimiento'].min() + 1)
    df_ems['hectareas_accum'] = df_ems['hectareas'].groupby(floor).cumsum()
    df_ems['ems_BA_accum'] = df_ems['ems_BA'].groupby(floor).cumsum()
    df_ems['ems_BT_accum'] = df_ems['ems_BT'].groupby(floor).cumsum()
    df_ems['abs_BA_accum'] = - df_ems['factor_cap_carb_ba'] * df_ems['hectareas_accum']
    df_ems['abs_BT_accum'] = - df_ems['factor_cap_carb_bt'] * df_ems['hectareas_accum']
    df_ems['ems_BA_neta'] = df_ems['abs_BA_accum'] + df_ems['ems_BA']
    df_ems['ems_BT_neta'] = df_ems['abs_BT_accum'] + df_ems['ems_BT']
    df_ems['ems_BA_neta_accum'] = df_ems['abs_BA_accum'] + df_ems['ems_BA_accum']
    df_ems['ems_BT_neta_accum'] = df_ems['abs_BT_accum'] + df_ems['ems_BT_accum']

    if sub_reg:
        cols = ['id_especie', 'ano', 'id_subregion', 'hectareas', 'hectareas_accum', 'turno', 'factor_cap_carb_ba',
                'factor_cap_carb_bt', 'abs_BA_accum', 'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
        df_ems.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
        df_sub = pd.read_sql('b_subregion', con=pg_connection_str())
        df_ems['id'] = pd.merge(df_ems, df_sub[['id', 'nombre']], on='id')['nombre']
        df_esp = pd.read_sql('b_especie', con=pg_connection_str())
        df_ems.rename(columns=dict([('id_especie', 'id'), ('id', 'region')]), inplace=True)
        df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)

    elif z_upra:
        cols = ['id_especie', 'ano', 'id_zona_upra', 'hectareas', 'hectareas_accum', 'turno', 'factor_cap_carb_ba',
                'factor_cap_carb_bt', 'abs_BA_accum', 'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
        df_ems.rename(columns=dict([('id_zona_upra', 'id')]), inplace=True)
        df_zp = pd.read_sql('b_zona_upra', con=pg_connection_str())
        df_ems['id'] = pd.merge(df_ems, df_zp[['id', 'nombre']], on='id')['nombre']
        df_esp = pd.read_sql('b_especie', con=pg_connection_str())
        df_ems.rename(columns=dict([('id_especie', 'id'), ('id', 'zona_upra')]), inplace=True)
        df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)

    elif dpto:
        cols = ['id_especie', 'ano', 'cod_depto', 'hectareas', 'hectareas_accum', 'turno', 'factor_cap_carb_ba',
                'factor_cap_carb_bt', 'abs_BA_accum', 'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
        df_ems.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
        df_zp = pd.read_sql('departamento', con=pg_connection_str())
        df_ems['codigo'] = pd.merge(df_ems, df_zp[['codigo', 'nombre']], on='codigo')['nombre']
        df_esp = pd.read_sql('b_especie', con=pg_connection_str())
        df_ems.rename(columns=dict([('id_especie', 'id'), ('codigo', 'departamento')]), inplace=True)
        df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)

    elif muni:
        cols = ['id_especie', 'ano', 'cod_muni', 'hectareas', 'hectareas_accum', 'turno', 'factor_cap_carb_ba',
                'factor_cap_carb_bt', 'abs_BA_accum', 'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
        df_ems.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
        df_zp = pd.read_sql('municipio', con=pg_connection_str())
        df_ems['codigo'] = pd.merge(df_ems, df_zp[['codigo', 'nombre']], on='codigo')['nombre']
        df_esp = pd.read_sql('b_especie', con=pg_connection_str())
        df_ems.rename(columns=dict([('id_especie', 'id'), ('codigo', 'municipio')]), inplace=True)
        df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)

    elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
        cols = ['id_especie', 'ano', 'id_subregion', 'hectareas', 'hectareas_accum', 'turno', 'factor_cap_carb_ba',
                'factor_cap_carb_bt', 'abs_BA_accum', 'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
        df_ems.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
        df_sub = pd.read_sql('b_subregion', con=pg_connection_str())
        df_ems['id'] = pd.merge(df_ems, df_sub[['id', 'nombre']], on='id')['nombre']
        df_esp = pd.read_sql('b_especie', con=pg_connection_str())
        df_ems.rename(columns=dict([('id_especie', 'id'), ('id', 'region')]), inplace=True)
        df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)

    if not esp:
        if sub_reg:
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['especie'], df_ems['region'], = 'Todas', 'Todas'
            cols = ['ano',  'especie', 'region', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]

        elif z_upra:
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['especie'], df_ems['Zona_UPRA'], = 'Todas', 'Todas'
            cols = ['ano',  'especie', 'Zona_UPRA', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]

        elif dpto:
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['especie'], df_ems['departamento'], = 'Todas', 'Todas'
            cols = ['ano',  'especie', 'departamento', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]
        elif muni:
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['especie'], df_ems['municipio'], = 'Todas', 'Todas'
            cols = ['ano',  'especie', 'municipio', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]
        elif (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['especie'], df_ems['region'], = 'Todas', 'Todas'
            cols = ['ano',  'especie', 'region', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]

    if esp:
        if len(esp) != 1:
            if (not sub_reg) & (not z_upra) & (not dpto) & (not muni):
                df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                    'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                    'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                    'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
                df_ems['especie'], df_ems['region'], = 'Todas', 'Todas'
                cols = ['ano', 'especie', 'region', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                        'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                        'ems_BA_neta_accum', 'ems_BT_neta_accum']
                df_ems = df_ems[cols]
        else:
            df_ems = df_ems.groupby(by=['ano'], as_index=False)['hectareas', 'hectareas_accum', 'abs_BA_accum',
                                                                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum',
                                                                'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                                                                'ems_BA_neta_accum', 'ems_BT_neta_accum'].sum()
            df_ems['region'] = 'Todas'
            df_ems['id_especie'] = esp[0]
            df_esp = pd.read_sql('b_especie', con=pg_connection_str())
            df_ems.rename(columns=dict([('id_especie', 'id')]), inplace=True)
            df_ems['id'] = pd.merge(df_ems, df_esp[['id', 'nombre']], on='id')['nombre']
            df_ems.rename(columns=dict([('id', 'especie')]), inplace=True)
            cols = ['ano', 'especie', 'region', 'hectareas', 'hectareas_accum', 'abs_BA_accum', 'abs_BT_accum',
                    'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum', 'ems_BA_neta', 'ems_BT_neta',
                    'ems_BA_neta_accum', 'ems_BT_neta_accum']
            df_ems = df_ems[cols]
    if year:
        if len(year) == 1:
            df_ems = df_ems.loc[(df_ems['ano'] >= min(year)) & (df_ems['ano'] < min(year) + 1)]
        else:
            df_ems = df_ems.loc[(df_ems['ano'] >= min(year)) & (df_ems['ano'] < max(year) + 1)]

    if sie:
        df_ems['id_sistema_siembra'] = sie[0]
        df_ems.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
        df_zp = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
        df_ems['id'] = pd.merge(df_ems, df_zp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'Sistema_siembra')]), inplace=True)
        cols = ['especie', 'ano', 'Sistema_siembra', 'departamento', 'hectareas', 'hectareas_accum',
                'turno', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'abs_BA_accum',
                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum',
                'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
    else:
        df_ems['id_sistema_siembra'] = 'Todas'
        df_ems.rename(columns=dict([('id_sistema_siembra', 'Sistema_siembra')]), inplace=True)
        cols = ['especie', 'ano', 'Sistema_siembra', 'hectareas', 'hectareas_accum',
                'abs_BA_accum',
                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum',
                'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
    if fue:
        df_ems['id_fuente'] = fue[0]
        df_ems.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
        df_zp = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
        df_ems['id'] = pd.merge(df_ems, df_zp[['id', 'nombre']], on='id')['nombre']
        df_ems.rename(columns=dict([('id', 'Fuente')]), inplace=True)
        cols = ['especie', 'ano', 'Sistema_siembra', 'Fuente', 'departamento', 'hectareas', 'hectareas_accum',
                'abs_BA_accum',
                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum',
                'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]
    else:
        df_ems['id_fuente'] = 'Todas'
        df_ems.rename(columns=dict([('id_fuente', 'Fuente')]), inplace=True)
        cols = ['especie', 'ano', 'Sistema_siembra', 'Fuente', 'hectareas', 'hectareas_accum',
                'abs_BA_accum',
                'abs_BT_accum', 'ems_BA', 'ems_BT', 'ems_BA_accum', 'ems_BT_accum',
                'ems_BA_neta', 'ems_BT_neta', 'ems_BA_neta_accum', 'ems_BT_neta_accum']
        df_ems = df_ems[cols]

    df_ems.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                  method="multi", chunksize=5000)
    print('Done')


def main():
    forest_emissions(year=[2015], esp=[2], dpto=[99])


if __name__ == '__main__':
    main()
