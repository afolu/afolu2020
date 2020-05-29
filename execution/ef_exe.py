#!/usr/bin/env python
# -*- coding: utf-8 -*-
from enteric_fermentation.emission_factor_ef import FactorEF


def ef_execution(at_id=1, ca_id=1, coe_act_id=2,  ta=13.0, pf=100.0, ps=0, vp_id=1, vs_id=40, weight=500.0,
                 adult_w=550.0, cp_id=2, gan=0.0, milk=0.0, grease=0.0, ht=0.0, cs_id=1, **kwargs):
    """
    :param at_id: Animal tipo. Animal type. ID animal type according to the DB.
            E.g. 1. Vacas de alta producción
    :param ca_id: Indice categoria animal. Animal Category. ID Animal Category according to the DB
            E.g. 1. Bovino tipo lechero
    :param coe_act_id: Indice coeficiente actividad
    :param ta: Temperatura ambiente. Environmental temperature in °C. By default 13°C
    :param pf: Porcentaje de Forraje. Fodder Percentage. 100% by default (%)
    :param pc: Porcentaje de Concentrado. Synthetic food percentage. 0% by default (%)
    :param vp_id: Indice Variedad de Pasto. Grass variety. ID grass variety according to the DB
            E.g. 1. Brachiaria común, Amargo, Peludo (Brachiaria decumbens)
    :param vs_id: Variedad de suplemento. Grass suplement variety. ID grass variety according to the DB
            E.g. 1. Brachiaria común, Amargo, Peludo (Brachiaria decumbens)
    :param weight:  Peso del animal. Animal Weight in (kg)
    :param adult_w:  Peso del animal Adulto. Animal Weight in (kg)
    :param cp_id: Indice Coeficiente de preñez
    :param gan: Ganancia de peso del animal
    :param milk: Cantidad de leche en litros por año (l año ** -1)
    :param grease: Porcentaje de grasa en la leche (%)
    :param ht: Horas de trabajo del animal
    :param cs_id: Indice de condicion sexual
    """
    ef = FactorEF()
    if at_id == 1:
        print(ef.ne)
        return 0
    elif at_id == 2:
        return ef.ne_vbp()
    elif at_id == 3:
        return ef.ne_vpc()
    elif at_id == 4:
        return ef.ne_tprf()
    elif at_id == 5:
        return ef.ne_tpd()
    elif at_id == 6:
        return ef.ne_tr()
    elif at_id == 7:
        return ef.ne_ge()


def main():
    ef_execution(at_id=1, ca_id=1, coe_act_id=2,  ta=13, pf=100, ps=0, vp_id=1, vs_id=40, weight=500,
                 adult_w=550, cp_id=2, gan=0, milk=3660, grease=3.2, ht=0, cs_id=1,  ym_id=4)
    pass


if __name__ == '__main__':
    main()
