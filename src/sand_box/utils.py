#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from db_utils import pg_config, pg_connection


def columns_header_query(table='variedad_pasto'):
    query = """
    select column_name
    from information_schema.columns
    where table_name = '{table}'
    order by ordinal_position
    """.format(table=table)
    return query


def get_data(db='pg_afolu_fe'):
    try:
        db_parameters = pg_config(section=db)
        conn = pg_connection(**db_parameters)
        with conn as connection:
            cur = connection.cursor()
            header_query = columns_header_query()
            cur.execute(header_query)
            df_data = pd.DataFrame(cur.fetchall(), columns=['column_name'])
            df_model = pd.DataFrame(columns=df_data['column_name'])
            print(df_model)

    except Exception as e:
        print(f'Error {e}')
        print('Connection failed!')


def main():
    get_data()


if __name__ == '__main__':
    main()
