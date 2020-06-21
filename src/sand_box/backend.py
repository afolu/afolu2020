#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
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


def filter_json(list_mun):
    list_filt = []
    if len(list_mun) < 1122:
        for cod in list_mun:
            for x in range(len(mun['features'])):
                if mun['features'][x]['properties']['CODIGO'] == str(cod):
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


def get_table_fig(df_res=None, mpio=None):
    if not df_res.empty:
        fig_table = go.Figure(
            data=go.Table(
                header=dict(
                    values=list(df_res.columns),
                    align='center'
                ),
                cells=dict(
                    values=[df_res.id, df_res.cod_depto, df_res.cod_muni, df_res.id_reg_ganadera,
                            df_res.ano, df_res.id_ani_tipo_ipcc, df_res.numero, df_res.cod_pais],
                )
            )
        )
        df_res.cod_muni = df_res.cod_muni.map('{:05}'.format)
        if mpio:
            mun = filter_json(mpio)
        else:
            return [None, None]
        fig_map = px.choropleth_mapbox(df_res, geojson=mun, locations='cod_muni', featureidkey='properties.CODIGO',
                                       color='numero',
                                       mapbox_style="carto-positron",
                                       zoom=3, center={"lat": 4.0, "lon": -74.0},
                                       # hover_data='numero',
                                       # hover_name=df_res.,
                                       opacity=0.3,
                                       height=300
                                       )
        fig_map.update_geos(showcountries=False, showcoastlines=False, showland=False, fitbounds="locations")
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        return [fig_table, fig_map]
    else:
        fig_table, fig_map = None, None
        return [fig_table, fig_map]


def get_act_data(year=None, mpio=None, ipcc=None):
    if year:
        df_res = df_act[df_act.ano.astype(str).isin(year)]
    else:
        df_res = df_act
    if mpio:
        df_res = df_res[df_act.cod_muni.isin(mpio)]
    if ipcc:
        df_res = df_res[df_act.id_ani_tipo_ipcc.isin(ipcc)]
    return df_res


def others():
    conn = pg_connection_str()
    df_act = pd.read_sql_table("datos_act_bovinos_ipcc", conn)
    df_act.to_csv('/home/alfonso/Documents/afolu/results/act.csv', index=False)
    df_act['MPIO_CCDGO'] = df_act.cod_muni
    df_act['DPTO_CCDGO'] = df_act.cod_depto.astype(str)
    df_new = pd.DataFrame()
    # df_new['DPTO_CCDGO'] = df_act['DPTO_CCDGO'].unique().astype(str)
    df_new['CODIGO'] = df_act['MPIO_CCDGO'].map('{:05}'.format).unique()
    df_new['Values'] = np.random.randint(1, 6, df_new.shape[0])
    fig = px.choropleth_mapbox(df_new, geojson=mun, locations='CODIGO', featureidkey='properties.CODIGO',
                               color='Values',
                               mapbox_style="carto-positron",
                               zoom=3, center={"lat": 4.0, "lon": -74.0},
                               opacity=0.5)
    fig.update_geos(showcountries=False, showcoastlines=False, showland=False, fitbounds="locations")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def main():
    a = get_mpio_by_dpto(['18'])
    b = get_act_data(year=['1993'], mpio=None, ipcc=['2'])
    c = filter_json(a)
    print(c)
    # get_table_fig(b)
    pass


if __name__ == '__main__':
    main()
