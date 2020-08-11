#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
from datetime import datetime
from numpy import arange

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_data(esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None):
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


def years_calc(ano_est, year_min, year_max):
    assert year_max > year_min, 'El rango de consulta debe ser año inferior y año superior. Por favor verificar años ' \
                                'consultados'
    if (year_min <= ano_est) and (year_max > ano_est):
        cant = year_max - ano_est
    elif year_min > ano_est:
        cant = year_max - year_min
    elif year_max == ano_est:
        cant = 1
    elif year_max < ano_est:
        cant = 0
    return cant


def turns(ano_est, turno, year_min, year_max):
    turnos = [int(ano_est + turno * i) for i in range(1000)]
    turnos.pop(0)
    if year_max == year_min:
        years_range = [year_max]
    else:
        years_range = arange(year_min, year_max)
    matches = set(turnos).intersection(years_range)
    return len(matches)


def gross_abs(year, esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None):
    df = get_data(esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni)
    df['abs_BA_year'] = df['factor_cap_carb_ba'] * df['hectareas']
    df['abs_BT_year'] = df['factor_cap_carb_bt'] * df['hectareas']
    if len(year) == 1:
        df['abs_BA_tot'] = df['abs_BA_year']
        df['abs_BT_tot'] = df['abs_BT_year']
    else:
        df['cant'] = df['ano_establecimiento'].apply(lambda x: years_calc(x, year_min=year[0], year_max=year[-1]))
        df['abs_BA_tot'] = df['abs_BA_year'] * df['cant']
        df['abs_BT_tot'] = df['abs_BT_year'] * df['cant']
    # df.to_csv('/home/alfonso/Documents/afolu/results/abs.csv')
    print(df.head)


def gross_emi(year, esp=None, sub_reg=None, z_upra=None, dpto=None, muni=None):
    df = get_data(esp=esp, sub_reg=sub_reg, z_upra=z_upra, dpto=dpto, muni=muni)
    df['abs_BA_year'] = df['factor_cap_carb_ba'] * df['hectareas']
    df['abs_BT_year'] = df['factor_cap_carb_bt'] * df['hectareas']
    df['turnos'] = df.apply(lambda x: turns(x['ano_establecimiento'], x['turno'], year_max=year[-1], year_min=year[0]),
                            axis=1)
    df['ems_BA_tot'] = df['abs_BA_year'] * df['turnos'] * df['turno']
    df['ems_BT_tot'] = df['abs_BT_year'] * df['turnos'] * df['turno']
    df.head()


def main():
    gross_abs(year=[2000], esp=[69, 158], sub_reg=[1, 2], z_upra=[1], dpto=[23, 17, 15])
    gross_emi(year=[1980, 2011], esp=[69, 158], sub_reg=[1, 2], z_upra=[1], dpto=[23, 17, 15])


if __name__ == '__main__':
    main()
