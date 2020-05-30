#!/usr/bin/env python
# -*- coding: utf-8 -*-
from enteric_fermentation.emission_factor_ef import FactorEF


def ef_execution(**kwargs):
    """

    :param kwargs:
    :return:
    """
    ef = FactorEF(**kwargs)
    if kwargs['at_id'] == 1:
        print(ef.ne)
        return 0
    elif kwargs['at_id']  == 2:
        print(ef.ne_vbp())
        return ef.ne_vbp()
    elif kwargs['at_id']  == 3:
        return ef.ne_vpc()
    elif kwargs['at_id']  == 4:
        return ef.ne_tprf()
    elif kwargs['at_id']  == 5:
        return ef.ne_tpd()
    elif kwargs['at_id'] == 6:
        return ef.ne_tr()
    elif kwargs['at_id'] == 7:
        return ef.ne_ge()


def main():
    ef_execution(at_id=2, ca_id=1, coe_act_id=2,  ta=18, pf=100, ps=0, vp_id=1, vs_id=40, weight=400,
                 adult_w=550, cp_id=2, gan=0, milk=3660, grease=3.2, ht=50.0, cs_id=1)
    pass


if __name__ == '__main__':
    main()
