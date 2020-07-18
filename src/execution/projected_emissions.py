#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.database.db_utils import pg_connection_str, pg_connection


def get_act_data():
    query = """ SELECT DA.id, ano as año, id_reg_ganadera, id_ani_tipo_ipcc, numero,
                FE.fe_fermentacion_ent as fe, FE.fe_gestion_est as gen
                FROM datos_act_bovinos_ipcc_proyectados as DA
                INNER JOIN fe_fermentacion_temporal as FE ON FE.id_reg = DA.id_reg_ganadera AND 
                FE.id_at = DA.id_ani_tipo_ipcc
            """
    df_bovinos = pd.read_sql_query(query, con=pg_connection_str())
    query_2 = "SELECT DA.id, DA.ano as año, DA.id_reg_ganadera, DA.id_ani_tipo_ipcc, DA.numero," \
              "V.fe, V.gen FROM datos_act_bovinos_ipcc_proyectados as DA " \
              "INNER JOIN (select id_animal_tipo_ipcc,  AVG(fe_ch4_fermentacin_enterica) as fe, " \
              "avg(fe_ch4_gestion_estiercol) as gen from fe_otras_especies group by (id_animal_tipo_ipcc)) as V " \
              "on V.id_animal_tipo_ipcc = DA.id_ani_tipo_ipcc "
    df_others = pd.read_sql_query(query_2, con=pg_connection_str())
    df = pd.concat([df_bovinos, df_others], ignore_index=True)
    df = df.sort_values(by=['id'])
    return df


def update_db(df) -> None:
    df = df.where(pd.notnull(df), 'nan')
    zipped = zip(df.id, df.emision_fe, df.emision_ge)
    tp_to_str = str(tuple(zipped))
    update = tp_to_str[1:len(tp_to_str) - 1]
    query_ = f"""UPDATE datos_act_bovinos_ipcc_proyectados AS da
                 SET emision_fe = v.emision_fe, emision_ge = v.emision_ge
                 FROM (VALUES {update}) AS v (id, emision_fe, emision_ge)
                 WHERE da.id = v.id"""
    conn_ = pg_connection()
    with conn_ as connection:
        cur = connection.cursor()
        cur.execute(query_)
    conn_.close()


def calculation():
    df = get_act_data()
    df['emision_ge'] = df.gen * df.numero / 1000000.0
    df['emision_fe'] = df.fe * df.numero / 1000000.0
    update_db(df)


def main():
    calculation()


if __name__ == '__main__':
    main()