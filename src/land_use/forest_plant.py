#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import warnings
import pandas as pd
from numpy import arange, VisibleDeprecationWarning
from datetime import datetime

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=VisibleDeprecationWarning)

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_data(esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None, fue=None, sie=None):
    """
    Funcion para traer la inforamcion de la base de datos corresponiente a los datos de de actividad y especie
    para los cálculos de las absorciones y emiones del modulo de plantaciones forestales
    :param sie: Sistema de siembra a consultar
    :param fue: Fuente de donde proviene la información
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
    :param sie: Tipo de siembra a consultar. Ej. [1] para Plantaciones Forestales
    :param fue: Fuente de informacion a consultar. Ej. [4] para MADR
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
        try:
            year_max = datetime.today().year
            range_years = arange(df['ano_establecimiento'].min(), year_max + 1)
            df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                        year_max=year_max), axis=1))
        except ValueError:
            print('No Data')
            return
    else:
        try:
            year_max = max(year)
            if not df['ano_establecimiento'].min():
                return print('No Data')
            elif df['ano_establecimiento'].min() > year_max:
                return print('No Data')
            else:
                df['turnos'], df['matches'] = zip(*df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'],
                                                                            year_max=year_max), axis=1))
                range_years = arange(df['ano_establecimiento'].min(), max(year) + 1)
        except ValueError:
            print('No Data')
            return

    if (not esp) & (not sub_reg) & (not z_upra) & (not dpto) & (not muni) & (not fue) & (not sie):
        matches = df.groupby(by=['id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                             )['matches'].sum()
        df_ems = df.groupby(by=['id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                            as_index=False)['hectareas'].sum()
        df_ems['matches'] = matches.values
        df_tot = pd.DataFrame(data=[x for x in arange(df['ano_establecimiento'].min(),
                                                      df['ano_establecimiento'].max())], columns=['anio'])
        df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
        for row in df_ems.itertuples():
            anos = getattr(row, 'matches')
            for ano in anos:
                ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                df_tot.loc[lambda x: (x['anio'] == ano), ['ems_ba']] += ems_ba
                df_tot.loc[lambda x: (x['anio'] == ano), ['ems_bt']] += ems_bt

        df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                'turno'], as_index=False)['hectareas'].sum()
        df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
        df_abs['abs_bt'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
        df_abs = df_abs.groupby(by=['ano_establecimiento'], as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
        df_tot['abs_ba'] = 0
        df_tot['abs_bt'] = 0
        df_tot['hectareas'] = 0

        for row in df_abs.itertuples():
            df_tot.loc[lambda x: (x['anio'] == row.ano_establecimiento), ['abs_ba']] = row.abs_ba
            df_tot.loc[lambda x: (x['anio'] == row.ano_establecimiento), ['abs_bt']] = row.abs_bt
            df_tot.loc[lambda x: (x['anio'] == row.ano_establecimiento), ['hectareas']] = row.hectareas
            df_tot.loc[lambda x: (x['anio'] == row.ano_establecimiento), ['abs_bt']] = row.abs_bt

        df_tot['ha_acc'] = df_tot['hectareas'].cumsum()
        df_tot['abs_ba_acc'] = df_tot['abs_ba'].cumsum()
        df_tot['abs_bt_acc'] = df_tot['abs_bt'].cumsum()
        df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
        df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
        df_tot['id_especie'] = 'Todas'
        df_tot['id_subregion'] = 'Todas'
        df_tot['id_fuente'] = 'Todas'
        df_tot['id_sistema_siembra'] = 'Todas'

        cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
        df_tot = df_tot[cols]
        if year:
            if len(year) == 1:
                df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
            else:
                df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

        df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                      method="multi", chunksize=5000)
        print('Done')
        return

    if esp:
        if (not sub_reg) & (not z_upra) & (not dpto) & (not muni) & (not fue) & (not sie):
            matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                     'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['id_especie'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum().sort_values(
                by=['id_especie'])
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_especie', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_subregion'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_sistema_siembra'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_esp = pd.read_sql('b_especie', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)

            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return
        elif sub_reg:
            if (not fue) & (not sie):
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                                         'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in
                        df['id_subregion'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_subregion'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_subregion'] == row.id_subregion)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_subregion'] == row.id_subregion)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion)), ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'id_subregion', 'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'
                df_tot['id_sistema_siembra'] = 'Todas'

                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            if fue:
                if not sie:
                    matches = df.groupby(by=['id_especie', 'id_subregion', 'id_fuente', 'ano_establecimiento',
                                             'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['id_especie', 'id_subregion', 'id_fuente', 'ano_establecimiento',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                            df['id_subregion'].unique() for w in df['id_fuente'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_subregion', 'id_fuente'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['id_subregion'] == row.id_subregion) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['id_subregion'] == row.id_subregion) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['id_especie', 'id_subregion', 'id_fuente', 'ano_establecimiento',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_fuente'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'id_subregion', 'id_fuente', 'anio']).reset_index()
                    df_tot.drop(columns=['index'], inplace=True, axis=1)
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum().values
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum().values
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum().values
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                    df_tot['id_sistema_siembra'] = 'Todas'

                    cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas',
                            'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_a = pd.DataFrame()
                    df_a['id'] = df_tot['id_fuente'].values
                    df_a['names'] = pd.merge(df_a, df_fue[['id', 'nombre']], on='id')['nombre']
                    df_tot = df_tot.rename(columns=dict([('id_fuente', 'id')]))
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
                elif sie:
                    matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_fuente',
                                             'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                             'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                            'turno'], as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w, t) for x in df['id_especie'].unique() for y in range_years for z in
                            df['id_subregion'].unique() for w in df['id_fuente'].unique() for t in
                            df['id_sistema_siembra'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_subregion', 'id_fuente',
                                                              'id_sistema_siembra'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['id_subregion'] == row.id_subregion) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['id_subregion'] == row.id_subregion) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_fuente',
                                                'id_sistema_siembra'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'id_subregion', 'id_fuente',
                                                    'id_sistema_siembra']).reset_index()
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']

                    cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas',
                            'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
            if sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_sistema_siembra',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                        df['id_subregion'].unique() for w in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_subregion', 'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_subregion'] == row.id_subregion) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_subregion'] == row.id_subregion) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_subregion', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_subregion'] == row.id_subregion) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'id_subregion', 'id_sistema_siembra',
                                                'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'

                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        elif dpto:
            if (not fue) & (not sie):
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'factor_cap_carb_ba',
                                         'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in
                        df['cod_depto'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_depto'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_depto'] == row.cod_depto)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_depto'] == row.cod_depto)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto)), ['hectareas']] = row.hectareas

                df_tot = df_tot.sort_values(by=['id_especie', 'cod_depto', 'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'
                df_tot['id_sistema_siembra'] = 'Todas'

                cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_dpto = pd.read_sql('departamento', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            if fue:
                if not sie:
                    matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                             'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                            df['cod_depto'].unique() for w in df['id_fuente'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_depto', 'id_fuente'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_depto'] == row.cod_depto) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_depto'] == row.cod_depto) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'cod_depto', 'id_fuente', 'anio']).reset_index()
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                    df_tot['id_sistema_siembra'] = 'Todas'

                    cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_dpto = pd.read_sql('departamento', con=pg_connection_str())
                    df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
                    df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')[
                        'nombre']
                    df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                    df_tot['id_1'] = df_tot['id']
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_1', 'id_sistema_siembra', 'anio',
                            'hectareas', 'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
                elif sie:
                    matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                             'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                             'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                            'turno'], as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w, t) for x in df['id_especie'].unique() for y in range_years for z in
                            df['cod_depto'].unique() for w in df['id_fuente'].unique() for t in
                            df['id_sistema_siembra'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_depto', 'id_fuente',
                                                              'id_sistema_siembra'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_depto'] == row.cod_depto) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_depto'] == row.cod_depto) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_fuente',
                                                'id_sistema_siembra'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra',
                                                    'anio']).reset_index()
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']

                    cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_muni = pd.read_sql('municipio', con=pg_connection_str())
                    df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                    df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')[
                        'nombre']
                    df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
            if sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_sistema_siembra',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                        df['cod_depto'].unique() for w in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_depto', 'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_depto'] == row.cod_depto) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_depto'] == row.cod_depto) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_depto', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_depto'] == row.cod_depto) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'cod_depto', 'id_sistema_siembra',
                                                'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'

                cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_dpto = pd.read_sql('departamento', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        elif muni:
            if (not fue) & (not sie):
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'factor_cap_carb_ba',
                                         'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z) for x in df['id_especie'].unique() for y in range_years for z in
                        df['cod_muni'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_muni'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_muni'] == row.cod_muni)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_muni'] == row.cod_muni)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni)), ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'cod_muni', 'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'
                df_tot['id_sistema_siembra'] = 'Todas'

                cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_muni = pd.read_sql('municipio', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            if fue:
                if not sie:
                    matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                             'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                            df['cod_muni'].unique() for w in df['id_fuente'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_muni', 'id_fuente'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_muni'] == row.cod_muni) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_muni'] == row.cod_muni) &
                                                  (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                            'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente)),
                            ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'cod_muni', 'id_fuente', 'anio']).reset_index()
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                    df_tot['id_sistema_siembra'] = 'Todas'

                    cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_muni = pd.read_sql('municipio', con=pg_connection_str())
                    df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                    df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')[
                        'nombre']
                    df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
                elif sie:
                    matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                             'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                             'turno'])['matches'].sum()
                    df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                            'turno'], as_index=False)['hectareas'].sum()
                    df_ems['matches'] = matches.values
                    data = [(x, y, z, w, t) for x in df['id_especie'].unique() for y in range_years for z in
                            df['cod_muni'].unique() for w in df['id_fuente'].unique() for t in
                            df['id_sistema_siembra'].unique()]
                    df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_muni', 'id_fuente',
                                                              'id_sistema_siembra'])
                    df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                    for row in df_ems.itertuples():
                        anos = getattr(row, 'matches')
                        for ano in anos:
                            ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row,
                                                                                                              'turno')
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_muni'] == row.cod_muni) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_ba']] += ems_ba
                            df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                                  (x['cod_muni'] == row.cod_muni) &
                                                  (x['id_fuente'] == row.id_fuente) &
                                                  (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                       ['ems_bt']] += ems_bt

                    df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                            'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                        as_index=False)['hectareas'].sum()
                    df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                    df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                    df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_fuente',
                                                'id_sistema_siembra'],
                                            as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                    df_tot['abs_ba'] = 0
                    df_tot['abs_bt'] = 0
                    df_tot['hectareas'] = 0

                    for row in df_abs.itertuples():
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                        df_tot.loc[
                            lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                       (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente) &
                                       (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                            ['hectareas']] = row.hectareas
                    df_tot = df_tot.sort_values(by=['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra',
                                                    'anio']).reset_index()
                    floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                    df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                    df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                    df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                    df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                    df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']

                    cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                            'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net',
                            'ems_bt_net']
                    df_tot = df_tot[cols]
                    if year:
                        if len(year) == 1:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                        else:
                            df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                    df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                    df_muni = pd.read_sql('municipio', con=pg_connection_str())
                    df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                    df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')[
                        'nombre']
                    df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                    df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                    df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                    df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                    df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                    df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                    df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                                  method="multi", chunksize=5000)
                    print('Done')
                    return
            if sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_sistema_siembra',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z, w) for x in df['id_especie'].unique() for y in range_years for z in
                        df['cod_muni'].unique() for w in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'cod_muni', 'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_muni'] == row.cod_muni) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['cod_muni'] == row.cod_muni) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_sistema_siembra',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'cod_muni', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['cod_muni'] == row.cod_muni) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'cod_muni', 'id_sistema_siembra',
                                                'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_fuente'] = 'Todas'

                cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_muni = pd.read_sql('municipio', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        elif fue:
            if not sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente', 'factor_cap_carb_ba',
                                         'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, z) for x in df['id_fuente'].unique() for y in range_years for z in
                        df['id_especie'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_fuente', 'anio', 'id_especie'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == getattr(row, 'id_fuente')) &
                                              (x['id_especie'] == getattr(row, 'id_especie'))), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == getattr(row, 'id_fuente')) &
                                              (x['id_especie'] == getattr(row, 'id_especie'))), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_especie'] == row.id_especie)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_especie'] == row.id_especie)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_especie'] == row.id_especie)), ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'id_fuente', 'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_subregion'] = 'Todas'
                df_tot['id_sistema_siembra'] = 'Todas'

                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_reg = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)

                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            elif sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente',
                                         'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                         'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                        'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(x, y, w, t) for x in df['id_especie'].unique() for y in range_years for w in
                        df['id_fuente'].unique() for t in
                        df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_fuente',
                                                          'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_fuente',
                                            'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                          (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_especie', 'id_fuente', 'id_sistema_siembra',
                                                'anio']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_subregion'] = 'Todas'

                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_esp = pd.read_sql('b_especie', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        elif sie:
            matches = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                     'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y, t) for x in df['id_especie'].unique() for y in range_years for t in
                    df['id_sistema_siembra'].unique()]
            df_tot = pd.DataFrame(data=data, columns=['id_especie', 'anio', 'id_sistema_siembra'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_especie'] == getattr(row, 'id_especie')) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_especie', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_especie', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_especie'] == row.id_especie) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_especie', 'id_sistema_siembra', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_subregion'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_esp = pd.read_sql('b_especie', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_especie', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_esp[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_especie')]), inplace=True)
            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return

    if sub_reg:
        if (not esp) & (not z_upra) & (not dpto) & (not muni) & (not fue) & (not sie):
            matches = df.groupby(by=['ano_establecimiento', 'id_subregion', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                     'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_subregion', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['id_subregion'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['id_subregion', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == getattr(row, 'id_subregion'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == getattr(row, 'id_subregion'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_subregion', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_subregion'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_subregion'] == row.id_subregion)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_subregion'] == row.id_subregion)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_subregion'] == row.id_subregion)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_subregion', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_sistema_siembra'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)

            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return
        if fue:
            if not sie:
                matches = df.groupby(by=['id_subregion', 'id_fuente', 'ano_establecimiento',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['id_subregion', 'id_fuente', 'ano_establecimiento',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w) for y in range_years for z in df['id_subregion'].unique() for w in
                        df['id_fuente'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'id_subregion', 'id_fuente'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['id_subregion', 'id_fuente', 'ano_establecimiento', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_subregion', 'id_fuente'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                               ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                               ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_subregion', 'id_fuente', 'anio']).reset_index()
                df_tot.drop(columns=['index'], inplace=True, axis=1)
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum().values
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum().values
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum().values
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_sistema_siembra'] = 'Todas'
                df_tot['id_especie'] = 'Todas'

                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot = df_tot.rename(columns=dict([('id_fuente', 'id')]))
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            elif sie:
                matches = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_fuente',
                                         'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                         'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                        'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w, t) for y in range_years for z in df['id_subregion'].unique() for w in
                        df['id_fuente'].unique() for t in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'id_subregion', 'id_fuente',
                                                          'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_subregion', 'id_fuente', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['id_subregion'] == row.id_subregion) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['id_subregion', 'id_fuente', 'id_sistema_siembra']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_especie'] = 'Todas'
                cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        if sie:
            matches = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_sistema_siembra',
                                     'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(y, z, w) for y in range_years for z in df['id_subregion'].unique() for w in
                    df['id_sistema_siembra'].unique()]
            df_tot = pd.DataFrame(data=data, columns=['anio', 'id_subregion', 'id_sistema_siembra'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_subregion'] == row.id_subregion) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_subregion', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_subregion', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_subregion'] == row.id_subregion) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_subregion'] == row.id_subregion) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_subregion'] == row.id_subregion) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_subregion', 'id_sistema_siembra', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_especie'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_reg = pd.read_sql('b_subregion', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_subregion', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_reg[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_subregion')]), inplace=True)
            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return

    if dpto:
        if (not esp) & (not z_upra) & (not sub_reg) & (not muni) & (not fue) & (not sie):
            matches = df.groupby(by=['ano_establecimiento', 'cod_depto', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                     'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'cod_depto', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['cod_depto'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['cod_depto', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == getattr(row, 'cod_depto'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == getattr(row, 'cod_depto'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'cod_depto', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_depto'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['cod_depto', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_sistema_siembra'] = 'Todas'

            cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_dpto = pd.read_sql('departamento', con=pg_connection_str())
            df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
            df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
            df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)

            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return
        if fue:
            if not sie:
                matches = df.groupby(by=['cod_depto', 'id_fuente', 'ano_establecimiento',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['cod_depto', 'id_fuente', 'ano_establecimiento',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w) for y in range_years for z in df['cod_depto'].unique() for w in
                        df['id_fuente'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_depto', 'id_fuente'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['cod_depto', 'id_fuente', 'ano_establecimiento', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_depto', 'id_fuente'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_fuente'] == row.id_fuente)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_fuente'] == row.id_fuente)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_fuente'] == row.id_fuente)), ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['cod_depto', 'id_fuente', 'anio']).reset_index()
                df_tot.drop(columns=['index'], inplace=True, axis=1)
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum().values
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum().values
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum().values
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_sistema_siembra'] = 'Todas'
                df_tot['id_especie'] = 'Todas'

                cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_dpto = pd.read_sql('departamento', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot = df_tot.rename(columns=dict([('id_fuente', 'id')]))
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            elif sie:
                matches = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_fuente',
                                         'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                         'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                        'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w, t) for y in range_years for z in df['cod_depto'].unique() for w in
                        df['id_fuente'].unique() for t in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_depto', 'id_fuente',
                                                          'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_depto', 'id_fuente', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['cod_depto'] == row.cod_depto) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['cod_depto', 'id_fuente', 'id_sistema_siembra']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_especie'] = 'Todas'
                cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_dpto = pd.read_sql('departamento', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        if sie:
            matches = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_sistema_siembra',
                                     'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(y, z, w) for y in range_years for z in df['cod_depto'].unique() for w in
                    df['id_sistema_siembra'].unique()]
            df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_depto', 'id_sistema_siembra'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_depto'] == row.cod_depto) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'cod_depto', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_depto', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_depto'] == row.cod_depto) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['cod_depto', 'id_sistema_siembra', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_especie'] = 'Todas'

            cols = ['id_especie', 'cod_depto', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_dpto = pd.read_sql('departamento', con=pg_connection_str())
            df_tot.rename(columns=dict([('cod_depto', 'codigo')]), inplace=True)
            df_tot['codigo'] = pd.merge(df_tot, df_dpto[['codigo', 'nombre']], on='codigo', how='left')['nombre']
            df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return

    if muni:
        if (not esp) & (not z_upra) & (not sub_reg) & (not dpto) & (not fue) & (not sie):
            matches = df.groupby(by=['ano_establecimiento', 'cod_muni', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                     'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'cod_muni', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['cod_muni'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['cod_muni', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == getattr(row, 'cod_muni'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == getattr(row, 'cod_muni'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'cod_muni', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_muni'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['cod_muni', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_sistema_siembra'] = 'Todas'

            cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_muni = pd.read_sql('municipio', con=pg_connection_str())
            df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
            df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')['nombre']
            df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return
        if fue:
            if not sie:
                matches = df.groupby(by=['cod_muni', 'id_fuente', 'ano_establecimiento',
                                         'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
                df_ems = df.groupby(by=['cod_muni', 'id_fuente', 'ano_establecimiento',
                                        'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w) for y in range_years for z in df['cod_muni'].unique() for w in
                        df['id_fuente'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_muni', 'id_fuente'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                              (x['id_fuente'] == row.id_fuente)), ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['cod_muni', 'id_fuente', 'ano_establecimiento', 'factor_cap_carb_ba',
                                        'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_muni', 'id_fuente'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_fuente'] == row.id_fuente)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_fuente'] == row.id_fuente)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_fuente'] == row.id_fuente)), ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['cod_muni', 'id_fuente', 'anio']).reset_index()
                df_tot.drop(columns=['index'], inplace=True, axis=1)
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum().values
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum().values
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum().values
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_sistema_siembra'] = 'Todas'
                df_tot['id_especie'] = 'Todas'

                cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_muni = pd.read_sql('municipio', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot = df_tot.rename(columns=dict([('id_fuente', 'id')]))
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
            elif sie:
                matches = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_fuente',
                                         'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                         'turno'])['matches'].sum()
                df_ems = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                        'turno'], as_index=False)['hectareas'].sum()
                df_ems['matches'] = matches.values
                data = [(y, z, w, t) for y in range_years for z in df['cod_muni'].unique() for w in
                        df['id_fuente'].unique() for t in df['id_sistema_siembra'].unique()]
                df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_muni', 'id_fuente',
                                                          'id_sistema_siembra'])
                df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
                for row in df_ems.itertuples():
                    anos = getattr(row, 'matches')
                    for ano in anos:
                        ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_ba']] += ems_ba
                        df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                              (x['id_fuente'] == row.id_fuente) &
                                              (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                                   ['ems_bt']] += ems_bt

                df_abs = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_fuente',
                                        'id_sistema_siembra', 'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                    as_index=False)['hectareas'].sum()
                df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
                df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
                df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_muni', 'id_fuente', 'id_sistema_siembra'],
                                        as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
                df_tot['abs_ba'] = 0
                df_tot['abs_bt'] = 0
                df_tot['hectareas'] = 0

                for row in df_abs.itertuples():
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                    df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                          (x['cod_muni'] == row.cod_muni) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['hectareas']] = row.hectareas
                df_tot = df_tot.sort_values(by=['cod_muni', 'id_fuente', 'id_sistema_siembra']).reset_index()
                floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
                df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
                df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
                df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
                df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
                df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
                df_tot['id_especie'] = 'Todas'
                cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                        'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
                df_tot = df_tot[cols]
                if year:
                    if len(year) == 1:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                    else:
                        df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

                df_muni = pd.read_sql('municipio', con=pg_connection_str())
                df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
                df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')['nombre']
                df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
                df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
                df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
                df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
                df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
                df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
                df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                              method="multi", chunksize=5000)
                print('Done')
                return
        if sie:
            matches = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_sistema_siembra',
                                     'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(y, z, w) for y in range_years for z in df['cod_muni'].unique() for w in
                    df['id_sistema_siembra'].unique()]
            df_tot = pd.DataFrame(data=data, columns=['anio', 'cod_muni', 'id_sistema_siembra'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['cod_muni'] == row.cod_muni) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'cod_muni', 'id_sistema_siembra',
                                    'factor_cap_carb_ba', 'factor_cap_carb_bt', 'turno'],
                                as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'cod_muni', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['cod_muni'] == row.cod_muni) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['cod_muni', 'id_sistema_siembra', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_fuente'] = 'Todas'
            df_tot['id_especie'] = 'Todas'

            cols = ['id_especie', 'cod_muni', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_muni = pd.read_sql('municipio', con=pg_connection_str())
            df_tot.rename(columns=dict([('cod_muni', 'codigo')]), inplace=True)
            df_tot['codigo'] = pd.merge(df_tot, df_muni[['codigo', 'nombre']], on='codigo', how='left')['nombre']
            df_tot.rename(columns=dict([('codigo', 'id_subregion')]), inplace=True)
            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return

    if fue:
        if (not esp) & (not z_upra) & (not dpto) & (not muni) & (not sie) & (not sub_reg):
            matches = df.groupby(by=['ano_establecimiento', 'id_fuente', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                     'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_fuente', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['id_fuente'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['id_fuente', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == getattr(row, 'id_fuente'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == getattr(row, 'id_fuente'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_fuente', 'factor_cap_carb_ba', 'factor_cap_carb_bt',
                                    'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_fuente'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente)),
                           ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente)),
                           ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_fuente', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_subregion'] = 'Todas'
            df_tot['id_sistema_siembra'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)

            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return
        elif sie:
            matches = df.groupby(by=['ano_establecimiento', 'id_fuente', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                     'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_fuente', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(y, w, t) for y in range_years  for w in
                    df['id_fuente'].unique() for t in df['id_sistema_siembra'].unique()]
            df_tot = pd.DataFrame(data=data, columns=['anio', 'id_fuente', 'id_sistema_siembra'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_fuente'] == row.id_fuente) &
                                          (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_fuente', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_fuente', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) & (x['id_fuente'] == row.id_fuente) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_fuente', 'id_sistema_siembra']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_region'] = 'Todas'
            cols = ['id_especie', 'id_region', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_fue = pd.read_sql('b_fuente_actividad', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_fuente', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_fue[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_fuente')]), inplace=True)
            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return

    if sie:
        if (not esp) & (not z_upra) & (not dpto) & (not muni) & (not fue) & (not sub_reg):
            matches = df.groupby(by=['ano_establecimiento', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                     'factor_cap_carb_bt', 'turno'])['matches'].sum()
            df_ems = df.groupby(by=['ano_establecimiento', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_ems['matches'] = matches.values
            data = [(x, y) for x in df['id_sistema_siembra'].unique() for y in range_years]
            df_tot = pd.DataFrame(data=data, columns=['id_sistema_siembra', 'anio'])
            df_tot['ems_ba'], df_tot['ems_bt'] = 0, 0
            for row in df_ems.itertuples():
                anos = getattr(row, 'matches')
                for ano in anos:
                    ems_ba = getattr(row, 'factor_cap_carb_ba') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    ems_bt = getattr(row, 'factor_cap_carb_bt') * getattr(row, 'hectareas') * getattr(row, 'turno')
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_sistema_siembra'] ==
                                                                getattr(row, 'id_sistema_siembra'))),
                               ['ems_ba']] += ems_ba
                    df_tot.loc[lambda x: ((x['anio'] == ano) & (x['id_sistema_siembra'] ==
                                                                getattr(row, 'id_sistema_siembra'))),
                               ['ems_bt']] += ems_bt

            df_abs = df.groupby(by=['ano_establecimiento', 'id_sistema_siembra', 'factor_cap_carb_ba',
                                    'factor_cap_carb_bt', 'turno'], as_index=False)['hectareas'].sum()
            df_abs['abs_ba'] = df_abs['factor_cap_carb_ba'] * df_abs['hectareas']
            df_abs['abs_bt'] = df_abs['factor_cap_carb_bt'] * df_abs['hectareas']
            df_abs = df_abs.groupby(by=['ano_establecimiento', 'id_sistema_siembra'],
                                    as_index=False)['hectareas', 'abs_ba', 'abs_bt'].sum()
            df_tot['abs_ba'] = 0
            df_tot['abs_bt'] = 0
            df_tot['hectareas'] = 0

            for row in df_abs.itertuples():
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_ba']] = row.abs_ba
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)), ['abs_bt']] = row.abs_bt
                df_tot.loc[lambda x: ((x['anio'] == row.ano_establecimiento) &
                                      (x['id_sistema_siembra'] == row.id_sistema_siembra)),
                           ['hectareas']] = row.hectareas
            df_tot = df_tot.sort_values(by=['id_sistema_siembra', 'anio']).reset_index()
            floor = df_tot.index // (year_max - df['ano_establecimiento'].min() + 1)
            df_tot['ha_acc'] = df_tot['hectareas'].groupby(floor).cumsum()
            df_tot['abs_ba_acc'] = df_tot['abs_ba'].groupby(floor).cumsum()
            df_tot['abs_bt_acc'] = df_tot['abs_bt'].groupby(floor).cumsum()
            df_tot['ems_ba_net'] = - df_tot['abs_ba'] + df_tot['ems_ba']
            df_tot['ems_bt_net'] = - df_tot['abs_bt'] + df_tot['ems_bt']
            df_tot['id_especie'] = 'Todas'
            df_tot['id_subregion'] = 'Todas'
            df_tot['id_fuente'] = 'Todas'

            cols = ['id_especie', 'id_subregion', 'id_fuente', 'id_sistema_siembra', 'anio', 'hectareas', 'ha_acc',
                    'abs_ba', 'abs_bt', 'abs_ba_acc', 'abs_bt_acc', 'ems_ba', 'ems_bt', 'ems_ba_net', 'ems_bt_net']
            df_tot = df_tot[cols]
            if year:
                if len(year) == 1:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < min(year) + 1)]
                else:
                    df_tot = df_tot.loc[(df_tot['anio'] >= min(year)) & (df_tot['anio'] < max(year) + 1)]

            df_sie = pd.read_sql('b_sistema_siembra', con=pg_connection_str())
            df_tot.rename(columns=dict([('id_sistema_siembra', 'id')]), inplace=True)
            df_tot['id'] = pd.merge(df_tot, df_sie[['id', 'nombre']], on='id', how='left')['nombre']
            df_tot.rename(columns=dict([('id', 'id_sistema_siembra')]), inplace=True)
            df_tot.to_sql('3b1aiii_resultados', con=pg_connection_str(), index=False, if_exists='replace',
                          method="multi", chunksize=5000)
            print('Done')
            return


def main():
    forest_emissions(year=[2019,2010], esp=[2])


if __name__ == '__main__':
    main()
