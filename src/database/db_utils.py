#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import sys
import os
sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from configparser import ConfigParser

path_root = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../"))


def pg_config(filename='database.ini', section='pg_afolu_fe'):
    filename = f'{path_root}/config/{filename}'
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        return db
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))


def pg_connection_str(**kwargs):
    """
    Create string connection for using with pandas read_sql
    :type kwargs: basestring
    :param kwargs: set of strings for creating string connection
    :return:
    """
    # read connection parameters
    if not kwargs:
        params = pg_config(**kwargs)
    else:
        params = kwargs
    conn = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**params)
    return conn


def pg_connection(**kwargs):
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        if not kwargs:
            params = pg_config(**kwargs)
        else:
            params = kwargs
        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    return conn


def get_from_ca_table(ca_id):
    """
    Get data from categoria animal table
    :return: a1, tc
    """
    conn = pg_connection()
    with conn as connection:
        query = """
        SELECT  coe_cat_animal, temp_conf, rcms, ib
        FROM categoria_animal
        WHERE id_categoria_animal = {0}
        """.format(ca_id)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        a1 = res[0][0]
        tc = res[0][1]
        rcms = res[0][2]
        bi = res[0][3]
    conn.close()
    return a1, tc, rcms, bi


def get_from_cp_table(cp_id):
    """
    Get data from coeficiente de preñez table
    :return: cp
    """
    conn = pg_connection()
    with conn as connection:
        query = """
        SELECT  coe_prenez 
        FROM coeficiente_prenez
        WHERE id_coe_prenez = {0}
        """.format(cp_id)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        cp = res[0][0]
    conn.close()
    return cp


def get_from_grass_type(id_):
    conn = pg_connection()
    with conn as connection:
        query = """
                SELECT ed_rumiantes, energia_bruta_pasto, fdn_dieta, fda, enm_rumiantes, 
                ceniza_dieta, pc_dieta
                FROM variedad_pasto
                WHERE id_variedad = {0}
                """.format(id_)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        edr: float = res[0][0]
        ebp: float = res[0][1]
        fdn: float = res[0][2]
        fda: float = res[0][3]
        enm: float = res[0][4]
        cen: float = res[0][5]
        pcd: float = res[0][6]
    conn.close()
    return edr, ebp, fdn, fda, enm, cen, pcd


def get_from_suplement_type(id_):
    conn = pg_connection()
    with conn as connection:
        query = """
                SELECT edt_rumiantes, energia_bruta_pasto, fdn_dieta, fda, 
                enm_rumiantes, ceniza_dieta, pc_dieta
                FROM suplemento
                WHERE id_suplemento = {0}
                """.format(id_)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        edr: float = res[0][0]
        ebp: float = res[0][1]
        fdn: float = res[0][2]
        fda: float = res[0][3]
        enm: float = res[0][4]
        cen: float = res[0][5]
        pcd: float = res[0][6]
    conn.close()
    return edr, ebp, fdn, fda, enm, cen, pcd


def get_from_ac_table(ac_id):
    """
    Get data from coeficiente de actividad table
    :return: ca
    """
    conn = pg_connection()
    with conn as connection:
        query = """
                SELECT coe_actividad 
                FROM coeficiente_actividad
                WHERE id_coe_actividad = {0}
                """.format(ac_id)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        ca = res[0][0]
    conn.close()
    return ca


def get_from_cs_table(id_cs):
    """
    Get data from condición sexual table
    :return: ca
    """
    conn = pg_connection()
    with conn as connection:
        query = """
        SELECT coe_cond_sexual 
        FROM condicion_sexual
        WHERE id_cond_sexual = {0}
        """.format(id_cs)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        fcs = res[0][0]
    conn.close()
    return fcs


def get_from_pm_table(pm_id):
    """
    Get data from produccion de metano table
    :return: cp
    """
    conn = pg_connection()
    with conn as connection:
        query = """
        SELECT  bovino_alta_prod, bovino_otras
        FROM produccion_metano
        WHERE id = '{0}'
        """.format(pm_id)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        ap: float = res[0][0]
        bp: float = res[0][1]
    conn.close()
    return ap, bp


def get_from_awms_table(awms_id):
    """
    Get data from produccion de metano table
    :return: cp
    """
    conn = pg_connection()
    with conn as connection:
        query = """
        SELECT  alta_prod, otras
        FROM gestion_residuos
        WHERE id = '{0}'
        """.format(awms_id)
        cur = connection.cursor()
        cur.execute(query)
        res = cur.fetchall()
        ap = res[0][0]
        bp = res[0][1]
    conn.close()
    return ap, bp


def main():
    db_parameters = pg_config()
    db_str = pg_connection_str()


if __name__ == '__main__':
    main()

