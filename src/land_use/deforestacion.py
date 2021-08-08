import pandas as pd
import sys
import os

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_query_da(id_bioma=None, year=None):
    query = """ SELECT id_bioma
                       ,ano
                       ,ha_his
                       ,ha_pro
                       ,arbustales
                       ,plantaciones_forestales
                       ,vegetacion_secundaria
                       ,areas_agricolas_het
                       ,cultivos_permanentes
                       ,cultivos_transitorios
                       ,herbazales
                       ,pastos
                       ,superficies_agua
                       ,vegetacion_acuatica
                       ,areas_urbanizadas
                       ,otras_areas_sin_vegetacion
                FROM deforestacion_datos_actividad_todos 
            """
    if id_bioma:
        id_bioma_query = """WHERE id_bioma in {} """.format(str(id_bioma).replace('[', '(').replace(']', ')'))
        query = query + id_bioma_query
    if year:
        if len(year) == 1:
            year_query = """AND ano in {} """.format(str(year).replace('[', '(').replace(']', ')'))
            query = query + year_query
        if len(year) != 1:
            year_query = """AND ano BETWEEN {} AND {}""".format(str(min(year)).replace('[', '(').replace(']', ')'),
                                                                str(max(year)).replace('[', '(').replace(']', ')'))
            query = query + year_query
    return query


def deforestacion(id_bioma=None, year=None, id_type=1, id_report=1):
    df = pd.read_sql_query(get_query_da(id_bioma=id_bioma, year=year), con=pg_connection_str())
    if df.empty:
        raise ValueError('Esta consulta no tiene datos')
    df.rename(columns=dict([('id', 'idx')]), inplace=True)
    df_subcat = pd.read_sql('deforestacion_subcategorias_ipcc', con=pg_connection_str())
    df_subcat.rename(columns=dict([('nombre', 'nombre_id'), ('id_datos_actividad', 'nombre')]), inplace=True)
    df_fact_cab = pd.read_sql('deforestacion_factores', con=pg_connection_str())
    df = pd.melt(df.reset_index(), id_vars=['id_bioma', 'ano', 'ha_his', 'ha_pro'], var_name='nombre',
                 value_name='porc_cobert_cambio', value_vars=df.columns[4:]).reset_index()
    df = pd.merge(df, df_subcat[['nombre', 'id_subcat_ipcc', 'subcat_ipcc_number']], on=['nombre'],
                  how='left').drop(['nombre'], axis=1)
    df = df[['ano', 'id_bioma', 'subcat_ipcc_number', 'id_subcat_ipcc', 'ha_his', 'ha_pro', 'porc_cobert_cambio']]
    df = df.sort_values(by=['id_bioma', 'id_subcat_ipcc'])
    df['ha_pro'] = df['ha_pro'].fillna(df['ha_his'])
    # inner join usando el bioma o region Contenidos de CO2 equivalente del Bosque Natural (tCO2)
    df = df.merge(df_fact_cab[['id_bioma', 'biomasa_total_co2', 'mom_c02']], on=['id_bioma'], how='left')

    # inner join usando la subcategoria ipcc Contenidos de CO2 equivalente de la nueva cobertura (tCO2)
    df = df.merge(df_fact_cab[['id_subcat_ipcc', 'biomasa_total_co2', 'mom_c02']], on=['id_subcat_ipcc'], how='left')

    df['tipif_ha_hist'] = df['ha_his'] * df['porc_cobert_cambio'] / 100
    df['tipif_ha_proy'] = df['ha_pro'] * df['porc_cobert_cambio'] / 100
    df.rename(columns=dict([('biomasa_total_co2_x', 'bt_co2_equiv_bos_nat'),
                            ('biomasa_total_co2_y', 'bt_co2_equiv_nuev_cob'),
                            ('mom_c02_x', 'mom_co2_equiv_bos_nat'),
                            ('mom_c02_y', 'mom_co2_equiv_nuev_cob')]), inplace=True)

    # Emisiones brutas y netas por deforestacion del bosque natural - histórico
    df['em_bruta_bnat_hist'] = df['tipif_ha_hist'] * df['bt_co2_equiv_bos_nat']
    df['cont_rema_bnat_hist'] = df['tipif_ha_hist'] * df['bt_co2_equiv_nuev_cob']
    df['emi_neta_bnat_hist'] = df['cont_rema_bnat_hist'] - df['em_bruta_bnat_hist']

    # Emisiones brutas y netas por deforestacion de la MOM - histórico
    df['em_bruta_mom_hist'] = df['tipif_ha_hist'] * df['mom_co2_equiv_bos_nat']
    df['cont_rema_mom_hist'] = df['tipif_ha_hist'] * df['mom_co2_equiv_nuev_cob']
    df['emi_neta_mom_hist'] = df['cont_rema_mom_hist'] - df['em_bruta_mom_hist']

    # Emisiones brutas y netas por deforestacion del bosque natural - Proyectado
    df['em_bruta_bnat_nref'] = df['tipif_ha_proy'] * df['bt_co2_equiv_bos_nat']
    df['cont_rema_bnat_nref'] = df['tipif_ha_proy'] * df['bt_co2_equiv_nuev_cob']
    df['emi_neta_bnat_nref'] = df['cont_rema_bnat_nref'] - df['em_bruta_bnat_nref']

    # Emisiones brutas y netas por deforestacion de la MOM - Proyectado
    df['em_bruta_mom_nref'] = df['tipif_ha_proy'] * df['mom_co2_equiv_bos_nat']
    df['cont_rema_mom_nref'] = df['tipif_ha_proy'] * df['mom_co2_equiv_nuev_cob']
    df['emi_neta_mom_nref'] = df['cont_rema_mom_nref'] - df['em_bruta_mom_nref']

    df_res = df.groupby(by=['id_bioma', 'ano', 'subcat_ipcc_number'],
                        as_index=False)[['tipif_ha_hist', 'tipif_ha_proy',
                                         'em_bruta_bnat_hist', 'cont_rema_bnat_hist', 'emi_neta_bnat_hist',
                                         'em_bruta_mom_hist', 'cont_rema_mom_hist', 'emi_neta_mom_hist',
                                         'em_bruta_bnat_nref', 'cont_rema_bnat_nref', 'emi_neta_bnat_nref',
                                         'em_bruta_mom_nref', 'cont_rema_mom_nref', 'emi_neta_mom_nref'
                                         ]].sum().reset_index()

    df_res = df_res.sort_values(by=['id_bioma', 'subcat_ipcc_number', 'ano'])
    df_res = pd.merge(df_res, df_subcat[['subcat_ipcc_number', 'subcat_ipcc']].drop_duplicates(),
                      on=['subcat_ipcc_number'], how='left').drop(['subcat_ipcc_number', 'index'], axis=1)
    df_res = pd.pivot(df_res, index=['id_bioma', 'ano'], columns='subcat_ipcc')

    if (id_type == 1) and (id_report == 1):  # Filtro por datos de actividad
        df_res = df_res['tipif_ha_hist'].reset_index()
    elif (id_type == 1) and (id_report == 2):
        df_res = df_res['tipif_ha_proy'].reset_index()
    elif (id_type == 2) and (id_report == 3):  # Filtro por datos de biomasa
        df_res = df_res['emi_neta_bnat_hist'].reset_index()
    elif (id_type == 2) and (id_report == 4):
        df_res = df_res['em_bruta_bnat_hist'].reset_index()
    elif (id_type == 2) and (id_report == 5):
        df_res = df_res['emi_neta_bnat_nref'].reset_index()
    elif (id_type == 2) and (id_report == 6):
        df_res = df_res['em_bruta_bnat_nref'].reset_index()
    elif (id_type == 3) and (id_report == 3):  # Filtro por datos de Mom
        df_res = df_res['emi_neta_mom_hist'].reset_index()
    elif (id_type == 3) and (id_report == 4):
        df_res = df_res['em_bruta_mom_hist'].reset_index()
    elif (id_type == 3) and (id_report == 5):
        df_res = df_res['emi_neta_mom_nref'].reset_index()
    elif (id_type == 3) and (id_report == 6):
        df_res = df_res['em_bruta_mom_nref'].reset_index()
    else:  # Otros filtros por aplicar
        df_res = df_res['tipif_ha_hist'].reset_index()
        print('Estos filtros aun no han sido aplicados, datos desplegados son de Actividad')

    df_biomas = pd.read_sql('deforestacion_biomas', con=pg_connection_str())
    df_biomas.rename(columns=dict([('id', 'id_bioma')]), inplace=True)
    df_res = pd.merge(df_res, df_biomas[['nombre', 'id_bioma']], on=['id_bioma'], how='left').drop(['id_bioma'], axis=1)
    df_res.rename(columns=dict([('nombre', 'bioma')]), inplace=True)
    cols = list(df_res.columns)
    cols = [cols[-1]] + cols[:-1]
    df_res = df_res[cols]
    df_res.to_sql('deforestacion_resultado', con=pg_connection_str(), if_exists='replace', index=False,
                  method="multi", chunksize=5000)
    print('Done!')


def main():
    deforestacion(id_bioma=[1], year=[2019, 1960], id_type=2, id_report=1)


if __name__ == '__main__':
    main()
