#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import argparse

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.land_use.deforestacion import deforestacion


def deforest_execution(**kwargs):
    deforestacion(year=kwargs['year'], id_bioma=kwargs['id_bioma'], id_type=kwargs['id_type'],
                  id_report=kwargs['id_report'])


def create_parser():
    parser = argparse.ArgumentParser(description='Modulo para el cálculo de las emisiones por deforestacion')
    parser.add_argument('--year', nargs='+', type=int, help='Lista de años a consultar')
    parser.add_argument('--id_bioma', nargs='+', type=int, help='Lista de biomas a consultar')
    parser.add_argument('--id_type', type=int, help='Tipo de categoria consultar')
    parser.add_argument('--id_report', type=int, help='Tipo de reporte a consultar')
    return parser.parse_args()


def main():
    args = create_parser()
    arg = vars(args)
    deforest_execution(year=arg['year'],
                       id_bioma=arg['id_bioma'],
                       id_type=arg['id_type'],
                       id_report=arg['id_report'])


if __name__ == '__main__':
    main()
