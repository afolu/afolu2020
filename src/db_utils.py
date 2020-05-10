#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
from configparser import ConfigParser


def pg_config(filename='../config/database.ini', section='pg_afolu_fe'):
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

    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def pg_connection_str(**kwargs):
    """
    Create string connection for using with pandas read_sql
    :param kwargs:
    :return:
    """
    params = pg_config(**kwargs)
    conn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**params)

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


if __name__ == '__main__':
    db_parameters = pg_config()
    pass
