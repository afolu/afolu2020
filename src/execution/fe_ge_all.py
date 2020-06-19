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
    print('entró')
    zipped = zip(df.id, df.fe_fermentacion_ent, df.ym, df.fe_gestion_est)
    tp_to_str = str(tuple(zipped))
    update = tp_to_str[1:len(tp_to_str) - 1]
    query_ = f"""UPDATE fe_fermentacion_temporal AS fet 
                 SET fe_fermentacion_ent = v.fe_fermentacion_ent, ym = v.ym, fe_gestion_est = v.fe_gestion_est 
                 FROM (VALUES {update}) AS v (id, fe_fermentacion_ent, ym, fe_gestion_est) 
                 WHERE fet.id = v.id """
    conn_ = pg_connection()
    with conn_ as connection:
        cur = connection.cursor()
        cur.execute(query_)


def masive_calc():
    df = get_data()
    for index, row in df.iterrows():
        try:
            at_id: int = int(row['id_at'])
            ca_id: int = int(row['id_ca'])
            coe_act_id: int = int(row['id_coe_acti'])
            ta: float = row['temp']
            pf: float = row['por_pasto']
            ps: float = row['por_suple']
            vp_id: int = int(row['id_pasto'])
            vs_id: int = int(row['id_suple'])
            weight: float = row['peso']
            adult_w: float = row['adult_peso']
            cp_id: int = int(row['id_cp'])
            gan: float = row['gn_peso']
            milk: float = row['leche']
            grease: float = row['grasa']
            ht: float = row['ht']
            cs_id: int = int(row['id_cs'])
            # sp_id: int = int(row['id_prod_metano'])
            # sgea_id: int = int(row['id_gestion_est1'])
            # sgra_id: int = int(row['id_gestion_res1'])
            # p_sga: float = row['por_gestion1']
            # sgeb_id: int = int(row['id_gestion_est2'])
            # sgrb_id: int = int(row['id_gestion_res2'])
            # p_sgb: float = row['por_gestion2']

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
                                                                                   sp_id=2,
                                                                                   sgea_id=5,
                                                                                   sgra_id=1,
                                                                                   p_sga=100,
                                                                                   sgeb_id=5,
                                                                                   sgrb_id=5,
                                                                                   p_sgb=0.0,
                                                                                   prnt=False)
            print(f'FE: {fe},    Ym: {ym},   GE: {fge}  ')
            df['fe_fermentacion_ent'][index] = fe
            df['ym'][index] = ym
            df['fe_gestion_est'][index] = fge
        except (IndexError, ValueError):
            print('Dato no disponible en la base de datos')
            pass
    update_db(df)
    df.to_csv('/home/alfonso/Documents/afolu/results/res.csv')


def main():
    masive_calc()
    pass


if __name__ == '__main__':
    main()
