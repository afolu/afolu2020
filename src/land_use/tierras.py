#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd


def handle_df(df):
    batms = df['RENDIMIENTO DETERMINADO (vol/año)']
    print(df.head(10))


def spread_df(df_data):
    for name, sheet in df_data.items():
        print(name, sheet)
        df = pd.DataFrame(data=sheet)
        df.to_csv(f'../data/{name}.csv')


def main():
    path_data = '../data'
    df_data = pd.read_csv(f'{path_data}/Datos actividad PC.csv', header=0, index_col=[0])
    # spread_df(df_data)
    handle_df(df_data)


if __name__ == '__main__':
    main()
