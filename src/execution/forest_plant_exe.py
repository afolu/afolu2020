#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import argparse

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.land_use.forest_plant import forest_emissions


def forest_execution(**kwargs):
    forest_emissions(year=kwargs['year'], esp=kwargs['esp'], sub_reg=kwargs['sub_reg'],
                     z_upra=kwargs['z_upra'], dpto=kwargs['dpto'], muni=kwargs['muni'])


def create_parser():
    parser = argparse.ArgumentParser(description='Modulo para el cálculo de las emisiones por plataciones forestales')
    parser.add_argument('--year', nargs='+', type=int, help='Lista de años a consultar')
    parser.add_argument('--esp', nargs='+', type=int, help='Lista de especies forestales a consultar')
    parser.add_argument('--sub_reg', nargs='+', type=int, help='Lista de subregiones a consultar')
    parser.add_argument('--z_upra', nargs='+', type=int, help='Lista de zonas upra a consultar')
    parser.add_argument('--dpto', nargs='+', type=int, help='lista de departamentos a consultar')
    parser.add_argument('--muni', nargs='+', type=int, help='lista de municipios a consultar')
    parser.add_argument('--sie', nargs='+', type=int, help='lista de sistema de siembra')
    parser.add_argument('--fue', nargs='+', type=int, help='lista de fuente de informacion')
    return parser.parse_args()


def main():
    args = create_parser()
    arg = vars(args)
    forest_execution(year=arg['year'],
                     esp=arg['esp'],
                     sub_reg=arg['sub_reg'],
                     z_upra=arg['z_upra'],
                     dpto=arg['dpto'],
                     muni=arg['muni'])


if __name__ == '__main__':
    main()
