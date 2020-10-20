import pandas as pd
import sys
import os

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.database.db_utils import pg_connection_str


def get_query_da(id_bioma=None, year=None):
    query = """ SELECT *
                FROM deforestacion_datos_actividad 
            """
    if id_bioma:
        id_bioma_query = """WHERE id_bioma in {} """.format(str(id_bioma).replace('[', '(').replace(']', ')'))
        query = query + id_bioma_query
    if year:
        if len(year) == 1:
            year_query = """AND ano in {} """.format(str(year).replace('[', '(').replace(']', ')'))
            query = query + year_query
        if len(year) != 1:
            year_query = """AND ano BETWEEN {} AND {}""".format(str(year[0]).replace('[', '(').replace(']', ')'),
                                                                str(year[-1]).replace('[', '(').replace(']', ')'))
            query = query + year_query
    return query


def get_query_fb(id_cat=None):
    query = """ SELECT *
                FROM deforestacion_categorias
            """
    if id_cat:
        id_bioma_query = """WHERE id in {} """.format(str(id_cat).replace('[', '(').replace(']', ')'))
        query = query + id_bioma_query
    return query


def deforestacion(year=None, id_bioma=None):
    df = pd.read_sql_query(get_query_da(id_bioma=id_bioma, year=year), con=pg_connection_str())
    df.rename(columns=dict([('id', 'idx'), ('id_bioma', 'id_bioma')]), inplace=True)
    df_subcat = pd.read_sql('deforestacion_subcategorias_ipcc', con=pg_connection_str())
    df_subcat.rename(dict([('nombre', 'nombre_id'), ('id_datos_actividad', 'nombre')]))
    df_fact_cab = pd.read_sql('deforestacion_factores', con=pg_connection_str())
    df = pd.melt(df.reset_index(), id_vars=['idx', 'id_bioma', 'ano', 'ha_his', 'ha_pro'], var_name='nombre',
                 value_name='porc_cob_camb', value_vars=df.columns[5:]).reset_index()
    df['ha_pro'] = df['ha_pro'].fillna(df['ha_his'])
    df = pd.merge(df, df_subcat[['nombre', 'id_subcat_ipcc']], on=['nombre'],
                  how='left').drop(['nombre'], axis=1)
    df = df.merge(df_fact_cab[['id_bioma', 'biomasa_total_co2']], on=['id_bioma'],  how='left')
    df = df.merge(df_fact_cab[['id_subcat_ipcc', 'biomasa_total_co2']], on=['id_subcat_ipcc'],  how='left')
    df['tipi_hist'] = df['ha_his'] * df['porc_cob_camb'] / 100
    df['tipi_proy'] = df['ha_pro'] * df['porc_cob_camb'] / 100
    df['em_bruta_defo_bos_nat_hist'] = df['tipi_hist'] * df['biomasa_total_co2_x']
    df['Cont_rema_nue_cobert_hist'] = df['tipi_hist'] * df['biomasa_total_co2_y']
    df['emi_neta_defo_bos_nat_hist'] = df['Cont_rema_nue_cobert_hist'] - df['em_bruta_defo_bos_nat_hist']
    df['em_bruta_defo_bos_nat_nref'] = df['tipi_proy'] * df['biomasa_total_co2_x']
    df['Cont_rema_nue_cobert_nref'] = df['tipi_proy'] * df['biomasa_total_co2_y']
    df['emi_neta_defo_bos_nat_nref'] = df['Cont_rema_nue_cobert_nref'] - df['em_bruta_defo_bos_nat_nref']
    df.to_csv('/home/alfonso/Documents/afolu/data/deforestacion_test.csv')
    print(1)
    # df.to_sql('test', con=pg_connection_str(), index=False, if_exists='replace',
    #            method="multi", chunksize=5000)


def main():
    path = '/home/alfonso/Documents/afolu/data/coberturas_def.csv'
    deforestacion()


if __name__ == '__main__':
    main()