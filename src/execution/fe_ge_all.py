#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.database.db_utils import pg_connection_str, pg_connection
from src.execution.ef_exe import ef_execution


def get_data():
    conn = pg_connection_str()
    query = "SELECT * " \
            "FROM fe_fermentacion"
    df = pd.read_sql_query(query, conn)
    return df


def update_db(df):
    df = df.where(pd.notnull(df), 'nan')
    zipped = zip(df.id, df.fe_fermentacion_ent, df.ym, df.fe_gestion_est)
    tp_to_str = str(tuple(zipped))
    update = tp_to_str[1:len(tp_to_str) - 1]
    query_ = f"""UPDATE fe_fermentacion_temporal AS fet 
                 SET fe_fermentacion_ent = v.fe_fermentacion_ent, ym = v.ym, fe_gestion_est = v.fe_gestion_est 
                 FROM (VALUES {update}) AS v (id, fe_fermentacion_ent, ym, fe_gestion_est) 
                 WHERE fet.id = v.id"""
    conn_ = pg_connection()
    with conn_ as connection:
        cur = connection.cursor()
        cur.execute(query_)


def masive_calc():
    df = get_data()
    for i in df.index:
        try:
            at_id: int = int(df.at[i, 'id_at'])
            ca_id: int = int(df.at[i, 'id_ca'])
            coe_act_id: int = int(df.at[i, 'id_coe_acti'])
            ta: float = df.at[i, 'temp']
            pf: float = df.at[i, 'por_pasto']
            ps: float = df.at[i, 'por_suple']
            vs_id: int = int(df.at[i, 'id_suple'])
            vp_id: int = int(df.at[i, 'id_pasto'])
            weight: float = df.at[i, 'peso']
            adult_w: float = df.at[i, 'adult_peso']
            cp_id: int = int(df.at[i, 'id_cp'])
            gan: float = df.at[i, 'gn_peso']
            milk: float = df.at[i, 'leche']
            grease: float = df.at[i, 'grasa']
            ht: float = df.at[i, 'ht']
            cs_id: int = int(df.at[i, 'id_cs'])
            sp_id: int = int(df.at[i, 'id_prod_metano'])
            sgea_id: int = int(df.at[i, 'id_gestion_est1'])
            sgra_id: int = int(df.at[i, 'id_gestion_res1'])
            p_sga: float = df.at[i, 'por_gestion1']
            sgeb_id: int = int(df.at[i, 'id_gestion_est2'])
            sgrb_id: int = int(df.at[i, 'id_gestion_res2'])
            p_sgb: float = df.at[i, 'por_gestion2']
            fe, _ceb, _cms, _cf, _cc, _cpms, _ccms, _dpcms, ym, fge = ef_execution(at_id=at_id,
                                                                                   ca_id=ca_id,
                                                                                   coe_act_id=coe_act_id,
                                                                                   ta=ta,
                                                                                   pf=pf,
                                                                                   ps=ps,
                                                                                   vp_id=vp_id,
                                                                                   vs_id=vs_id,
                                                                                   weight=weight,
                                                                                   adult_w=adult_w,
                                                                                   cp_id=cp_id,
                                                                                   gan=gan,
                                                                                   milk=milk,
                                                                                   grease=grease,
                                                                                   ht=ht,
                                                                                   cs_id=cs_id,
                                                                                   sp_id=sp_id,
                                                                                   sgea_id=sgea_id,
                                                                                   sgra_id=sgra_id,
                                                                                   p_sga=p_sga,
                                                                                   sgeb_id=sgeb_id,
                                                                                   sgrb_id=sgrb_id,
                                                                                   p_sgb=p_sgb,
                                                                                   prnt=False)
            df.loc[i, 'fe_fermentacion_ent'] = fe
            df.loc[i, 'ym'] = ym
            df.loc[i, 'fe_gestion_est'] = fge
        except (IndexError, ValueError):
            pass
    update_db(df)
    print("salida=OK")


def main():
    masive_calc()
    pass


if __name__ == '__main__':
    main()
