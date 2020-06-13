#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import argparse

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.manure_management.manure_mgmt import FeGe


def ef_execution(**kwargs):
    """

    :param kwargs: arguments contained in FE and GE  script
    :return: emission factor, gross energy,
    """
    ef = FeGe(**kwargs)
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge
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
        fge: float = ef.ef_ge_calc()
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
        print(f'FGE={fge}')
        return fe, ceb, cms, cf, cc, cpms, ccms, dpcms, ym, fge


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
    parser.add_argument('-sp_id', type=int, help='Sistema de producción.')
    parser.add_argument('-sgea_id', type=int, help='Sistema de gestion de estiercol A')
    parser.add_argument('-sgra_id', type=int, help='Sistema de gestion de residuos A')
    parser.add_argument('-p_sga', type=float, help='Porcentaje de Gestion de estiercol A')
    parser.add_argument('-sgeb_id', type=int, help='Sistema de gestion de estiercol B')
    parser.add_argument('-sgrb_id', type=int, help='Sistema de gestion de residuos B')
    parser.add_argument('-p_sgb', type=float, help='Porcentaje de Gestion de estiercol B')
    return parser.parse_args()


def main():
    args = create_parser()
    arg = vars(args)
    ef_execution(at_id=arg['at_id'],
                 ca_id=arg['ca_id'],
                 coe_act_id=arg['coe_act_id'],
                 ta=arg['ta'],
                 pf=arg['pf'],
                 ps=arg['ps'],
                 vp_id=arg['vp_id'],
                 vs_id=arg['vs_id'],
                 weight=arg['wg'],
                 adult_w=arg['adult_wg'],
                 cp_id=arg['cp_id'],
                 gan=arg['gn_wg'],
                 milk=arg['milk'],
                 grease=arg['grease'],
                 ht=arg['ht'],
                 cs_id=arg['cs_id'],
                 sp_id=arg['sp_id'],
                 sgea_id=arg['sgea_id'],
                 sgra_id=arg['sgra_id'],
                 p_sga=arg['p_sga'],
                 sgeb_id=arg['sgeb_id'],
                 sgrb_id=arg['sgrb_id'],
                 p_sgb=arg['p_sgb'],
                 )

    # ef_execution(at_id=1, ca_id=1, weight=540.0, adult_w=600.0, milk=3660, grease=3.5, cp_id=2, cs_id=1,
    #              coe_act_id=2, pf=80, ps=20, vp_id=15, vs_id=1, ta=14, ht=0.0, sp_id=1, sgea_id=1,
    #              sgra_id=2, p_sga=20, sgeb_id=2, sgrb_id=3, p_sgb=80)
    pass


if __name__ == '__main__':
    main()
