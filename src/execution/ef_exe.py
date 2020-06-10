#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import argparse

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.enteric_fermentation.emission_factor_ef import FactorEF


def ef_execution(**kwargs):
    """

    :param kwargs: arguments contained in FE script
    :return: emission factor, gross energy,
    """
    ef = FactorEF(**kwargs)
    if kwargs['at_id'] == 1:
        ceb: float = ef.tge
        fe: float = ef.gbvap_ef()
        cpms: float = ef.cpmsgbvap()
        cms: float = ef.cms
        cf: float = ef.cmcf_calc()
        cc: float = ef.cmcs_calc()
        ccms: float = cpms - cms
        dpcms: float = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 2:
        ceb: float = ef.tge
        fe: float = ef.gbvbp_ef()
        cpms: float = ef.cpmsgbvbp()
        cms: float = ef.cms
        cf: float = ef.cmcf_calc()
        cc: float = ef.cmcs_calc()
        ccms: float = cpms - cms
        dpcms: float = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 3:
        ceb = ef.tge
        fe = ef.gbvpc_ef()
        cpms = ef.cpmsgbpc()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        ccms = cpms - cms
        dpcms = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 4:
        ceb = ef.tge
        fe = ef.gbtpfr_ef()
        cpms = ef.cpmsgbtfr()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        ccms = cpms - cms
        dpcms = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 5:
        ceb = ef.tge
        fe = ef.gbtpd_ef()
        cpms = ef.cpmsgbtp()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        ccms = cpms - cms
        dpcms = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 6:
        ceb = ef.tge
        fe = ef.gbtr_ef()
        cpms = ef.cpmsgbtr()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        ccms = cpms - cms
        dpcms = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"FE={fe}")
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym
    elif kwargs['at_id'] == 7:
        ceb = ef.tge
        fe = ef.gbge_ef()
        cpms = ef.cpmsgbge()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        ccms = cpms - cms
        dpcms = ((cms * 100 / cpms) - 100) / 100
        ym: float = ef.ym
        print(f"CEB={ceb}")
        print(f"CMS={cms}")
        print(f"CF={cf}")
        print(f"CC={cc}")
        print("Punto de control")
        print(f"CPMS={cpms}")
        print(f"CCMS={ccms}")
        print(f"DPCMS={dpcms}")
        print(f'Ym={ym}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym


def create_parser():
    parser = argparse.ArgumentParser(description='Modulo para el calculo del factor de emision '
                                                 'de  fermentación entérica')
    parser.add_argument('-at_id', type=int, help='Indice de animal tipo')
    parser.add_argument('-ca_id', type=int, help='Indice categoria animal')
    parser.add_argument('-coe_act_id', type=int, help='Indice coeficiente actividad')
    parser.add_argument('-ta', type=float, help='Temperatura ambiente')
    parser.add_argument('-pf', type=float, help='Porcentaje de forraje')
    parser.add_argument('-ps', type=float, help='Porcentaje de concentrado/suplemento')
    parser.add_argument('-vp_id', type=int, help='Indice variedad de pasto')
    parser.add_argument('-vs_id', type=int, help='Indice variedad de concentrado/suplemento')
    parser.add_argument('-wg', type=float, help='Peso del animal')
    parser.add_argument('-adult_wg', type=float, help='Peso del animal adulto')
    parser.add_argument('-cp_id', type=int, help='Indice Coeficiente de preñez')
    parser.add_argument('-gn_wg', type=float, help='Ganancia de peso del animal')
    parser.add_argument('-milk', type=float, help='Cantidad de leche en litros por año (l año-1)')
    parser.add_argument('-grease', type=float, help='Porcentaje de grasa en la leche')
    parser.add_argument('-ht', type=float, help='Horas de trabajo del animal')
    parser.add_argument('-cs_id', type=int, help='Indice de condicion sexual')
    parser.add_argument('-sp_id', type=int, help='Sistema de producción. 1 -> Alta produccion. 2 -> Baja producción ')
    return parser.parse_args()


def main():
    # args = create_parser()
    # ef_execution(**vars(args))
    ef_execution(at_id=5, ca_id=2, weight=110, adult_w=577, milk=545, grease=3.5, cp_id=1, cs_id=2,
                 coe_act_id=3, pf=100, ps=0, vp_id=15, vs_id=1, ta=16, ht=0.0, gan=0.5, sp=2)
    pass


if __name__ == '__main__':
    main()
