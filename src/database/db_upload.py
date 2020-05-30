#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from src.database.db_utils import pg_connection_str


def upload_data(df, tb_name):
    """
    https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
    :param df: data frame to upload
    :param tb_name: table name
    :return:
    """
    conn = pg_connection_str()
    df.to_sql(tb_name, conn)
    print("Done!")


def main():
    df = pd.read_excel('../data/Datos_Actividad_Homologados_IPCC_Bovinos_Serie_1990_2020_V2.xlsx', header=0)
    upload_data(df, tb_name='act_dat_bov')


if __name__ == "__main__":
    main()
