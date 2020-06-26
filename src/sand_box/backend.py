#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from src.database.db_utils import pg_connection_str

df_act = pd.read_csv('/home/alfonso/Documents/afolu/results/act.csv')
df_gis = pd.read_csv('/home/alfonso/Documents/afolu/results/muni.csv')
df_at = pd.read_sql_table('animal_tipo', con=pg_connection_str())
df_gis['DPTO_CCDGO'] = df_gis['DPTO_CCDGO'].astype(str)
dt_year = [{'label': f'{i}', 'value': f'{i}'} for i in df_act.ano.unique()]
dt_dptos = [{'label': f'{df_gis.DPTO_CNMBR[df_gis.DPTO_CCDGO.isin([i])].values[0]}',
             'value': f'{i}'} for i in df_act.cod_depto.unique().astype(str)]
dt_mpios = [{'label': f'{df_gis.MPIO_CNMBR[df_gis.MPIO_CCNCT.isin([i])].values[0]}',
             'value': f'{i}'} for i in df_act.cod_muni.unique().astype(str)]
dt_at = [{'label': f'{df_at.tipo[df_at.id_animal_tipo.isin([i])].values[0]}',
          'value': f'{i}'} for i in df_at.id_animal_tipo]
with open('/home/alfonso/Documents/gis/Muni/mcpios.geojson') as json_file:
    mun = json.load(json_file)


def get_act_data(year=None, dpto=None, mpio=None, ipcc=None):
    year_q = ''
    dpto_q = ''
    mpio_q = ''
    ipcc_q = ''
    query = """ SELECT cod_depto, cod_muni, id_reg_ganadera, ano, id_ani_tipo_ipcc, numero, cod_pais,
                D.nombre as depto, M.nombre as muni,
                P.nombre as pais, R.nom_region as region, T.tipo as tipo
                FROM datos_act_bovinos_ipcc as A
                INNER JOIN pais             as P ON P.codigo = A.cod_pais
                INNER JOIN departamento     as D ON D.codigo = A.cod_depto
                INNER JOIN animal_tipo      as T ON T.id_animal_tipo = A.id_ani_tipo_ipcc
                INNER JOIN region_ganadera  as R ON R.id_region = A.id_reg_ganadera
                INNER JOIN municipio        as M ON M.codigo = A.cod_muni
    """
    if year:
        year_q = """WHERE ano in {0}
                 """.format(str(year).replace('[', '(').replace(']', ')'))
    if dpto:
        dpto_q = """AND codigo_depto in {0}
                 """.format(str(dpto).replace('[', '(').replace(']', ')'))
    if mpio:
        mpio_q = """AND cod_muni in {0}
                 """.format(str(mpio).replace('[', '(').replace(']', ')'))
    if ipcc:
        ipcc_q = """AND id_ani_tipo_ipcc in {0} 
                 """.format(str(ipcc).replace('[', '(').replace(']', ')'))
    query = query + year_q + dpto_q + mpio_q + ipcc_q
    df_actividad = pd.read_sql_query(query, con=pg_connection_str())
    df_actividad.cod_muni = df_actividad.cod_muni.map('{:05}'.format)
    return df_actividad


def filter_json(list_mun):
    list_filt = []
    if len(list_mun) < 1122:
        for cod in list_mun:
            for x in range(len(mun['features'])):
                if mun['features'][x]['properties']['CODIGO'] == str(f'{cod:05}'):
                    list_filt.append(mun['features'][x])

        dt_mun_filt = {"type": "FeatureCollection", "name": "mcpios",
                       "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
                       "features": list_filt}
        return dt_mun_filt
    elif len(list_mun) == 1122:
        dt_mun_filt = mun
        return dt_mun_filt


def get_mpio_by_dpto(dpto=None):
    if dpto:
        df_mpio_by_dpto = df_gis.MPIO_CCNCT[df_gis.DPTO_CCDGO.isin(dpto)]
        return df_mpio_by_dpto.values
    else:
        df_mpio_by_dpto = df_gis.MPIO_CCNCT
        return df_mpio_by_dpto.values
    

def create_map(df, j_son):
    fig_map = px.choropleth_mapbox(df, geojson=j_son, locations='cod_muni', featureidkey='properties.CODIGO',
                                   color='numero',
                                   mapbox_style="carto-positron",
                                   zoom=4, center={"lat": 4.0, "lon": -74.0},
                                   # hover_data='numero',
                                   hover_name=df.muni,
                                   opacity=0.3,
                                   height=450,
                                   # width=500
                                   )
    fig_map.update_geos(showcountries=False, showcoastlines=False, showland=False, fitbounds="locations")
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig_map


def create_table(df_res):
    fig_table = go.Figure(
        data=go.Table(
            header=dict(
                values=['Año', 'Municipio', 'Animal Tipo', 'Cantidad'],
                align='center'
            ),
            cells=dict(
                values=[df_res.ano, df_res.muni, df_res.tipo, df_res.numero],
            )
        )
    )
    return fig_table
    

def get_table_fig(df_res=None, mpio=None):
    if not df_res.empty:
        if mpio:
            json_mun = filter_json(mpio)
        else:
            json_mun = mun
        fig_map = create_map(df_res, json_mun)
        fig_table = create_table(df_res)
        return [fig_table, fig_map]
    else:
        return [None, None]


def main():
    a = get_mpio_by_dpto(['18'])
    a = [i for i in a]
    b = get_act_data(year=['1993'], mpio=a, ipcc=['2'])
    # c = filter_json(a)
    # # print(c)
    get_table_fig(b, a)
    # get_act_data_a(year=['1990'], dpto=['91'], ipcc=['1'])
    pass


if __name__ == '__main__':
    main()
