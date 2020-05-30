#!/usr/bin/env python
# -*- coding: utf-8 -*-
from src.enteric_fermentation.emission_factor_ef import FactorEF


def ef_execution(**kwargs):
    """

    :param kwargs: arguments contained in FE script
    :return: emission factor, gross energy,
    """
    ef = FactorEF(**kwargs)
    if kwargs['at_id'] == 1:
        ceb = ef.ne
        fe = ef.gbvap_ef()
        cpms = ef.cpmsgbvap()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 2:
        ceb = ef.ne
        fe = ef.gbvbp_ef()
        cpms = ef.cpmsgbvbp()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 3:
        ceb = ef.ne
        fe = ef.gbvpc_ef()
        cpms = ef.cpmsgbpc()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 4:
        ceb = ef.ne
        fe = ef.gbtpfr_ef()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 5:
        ceb = ef.ne
        fe = ef.gbtpd_ef()
        cpms = ef.cpmsgbtp()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 6:
        ceb = ef.ne
        fe = ef.gbtr_ef()
        cpms = ef.cpmsgbtr()
        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms
    elif kwargs['at_id'] == 7:
        ceb = ef.ne
        fe = ef.gbge_ef()
        cpms = ef.cpmsgbge()

        cms = ef.cms
        cf = ef.cmcf_calc()
        cc = ef.cmcs_calc()
        print(f"FE = {fe}")
        print(f"CEB = {ceb}")
        print(f"CMS = {cms}")
        print(f"CF = {cf}")
        print(f"CC = {cc}")
        print("Punto de control")
        print(f"CMS = {cpms}")
        return fe, ceb, cms, cf, cc, cpms


def main():
    ef_execution(at_id=7, ca_id=1, coe_act_id=2,  ta=18, pf=100, ps=0, vp_id=1, vs_id=40, weight=400,
                 adult_w=550, cp_id=2, gan=0, milk=3660, grease=3.2, ht=50.0, cs_id=1)
    pass


if __name__ == '__main__':
    main()
