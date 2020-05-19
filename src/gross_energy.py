#!/usr/bin/env python
# -*- coding: utf-8 -*-
import db_utils


class GrossEnergy(object):
    def __init__(self, ca_id=1, coe_act_id=2,  ta=13, pf=100, ps=0, vp_id=1, vs_id=40, weight=500,
                 adult_w=550, cp_id=2, gan=0, milk=0, grease=0, ht=0, cs_id=1, **kwargs):
        """
        :param at: Animal tipo. Animal type. ID animal type according to the DB.
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
        self.ca_id = ca_id
        self.ta = ta
        assert -10 <= self.ta <= 50.0, "Verifique la temperatura ambiente. El rango permitido es entre -10 y 50 °C "
        self.pf = pf
        assert 0 <= self.pf <= 100, "Verifique el porcentaje de forraje. El rango permitido es entre 0 y 100%"
        self.ps = ps
        assert 0 <= self.ps <= 100, "Verifique el porcentaje de Suplemento. El rango permitido es entre 0 y 100%"
        self.vp_id = vp_id
        self.vs_id = vs_id
        self.weight = weight
        self.adult_w = adult_w
        self.cp_id = cp_id
        self.gan = gan
        self.milk = milk
        self.grease = grease
        self.ac_id = coe_act_id
        self.ht = ht
        self.id_cs = cs_id
        self.a1, self.tc, self.rcms = db_utils.get_from_ca_table(self.ca_id)
        self.fcs = db_utils.get_from_cs_table(self.id_cs)
        self.de_f, self.ebf = db_utils.get_from_grass_type(self.vp_id)
        self.de_s, self.ebs = db_utils.get_from_grass_type(self.vs_id)
        self.dep = self.d_ep()
        self.reg = self.reg_rel()
        self.rem = self.rem_rel()
        self.ca = db_utils.get_from_ac_table(self.ac_id)
        self.cp = db_utils.get_from_cp_table(self.cp_id)

    def d_ep(self):
        """
        Digestibilidad de la dieta - dep
        :return:
        """
        assert self.ps + self.pf == 100, "La suma de los porcentajes de forraje y complemento debe ser igual a 100%"
        dep = (self.de_f * 100 / self.ebf) * self.pf / 100 + (self.de_s * 100 / self.ebs) * self.ps / 100
        return dep

    def rem_rel(self):
        """
        Relación entre la energía neta disponible en una dieta para mantenimiento
        y la energía digerible consumida (rem)
        :param dep Digestibilidad de la Dieta
        :return: rem
        """
        rem = (1.123 - (4.092 * 10 ** -3 * self.dep) +
               (1.126 * 10 ** -5 * (self.dep ** 2)) - (25.4 / self.dep))
        return rem

    def reg_rel(self):
        """
        Relación entre la energía neta disponible en la dieta para crecimiento y
        la energía digerible consumida
        :return: reg
        """
        reg = (1.164 - (5.16 * 10 ** -3 * self.dep) +
               (1.308 * 10 ** -5 * (self.dep ** 2)) - (37.4 / self.dep))
        return reg

    def maintenance(self):
        """
        Cálculo del requerimiento de energía bruta para mantenimiento GEm o em
        :return: em (Mj día -1)
        """
        em = (self.weight ** 0.75) * (self.a1 + (0.0029288 * (self.tc - self.ta))) / self.rem / (self.dep / 100)
        return em

    def activity(self):
        """
        Cálculo del requerimiento de energía bruta de actividad GEa o ea
        :return: ea (Mj día -1)
        """
        em = ((self.weight ** 0.75) * (self.a1 + (0.0029288 * (self.tc - self.ta)))) * self.ca / self.rem / \
             (self.dep / 100)
        return em

    def breastfeeding(self):
        """
        Cálculo del requerimiento de energía bruta para lactancia o producción de leche GEl o el
        :return: el (Mj día -1)
        """
        el = (self.milk / 365) * (1.47 + 0.4 * self.grease) / self.rem / (self.dep / 100)
        return el

    def work(self):
        """
        Requerimiento de energía bruta para el trabajo
        :return: ew
        """
        ew = ((self.weight ** 0.75) * (self.a1 + (0.0029288 * (self.tc - self.ta)))) * 0.1 * self.ht / self.rem / \
             (self.dep / 100)
        return ew

    def pregnancy(self):
        """
        Requerimiento de energía bruta para gestación o preñez
        :return: ep (Mj día -1)
        """
        ep = ((self.weight ** 0.75) * (self.a1 + (0.0029288 * (self.tc - self.ta)))) * self.cp / self.rem / \
             (self.dep / 100)
        return ep

    def grow(self):
        """
        Requerimiento de energía bruta para ganancia de peso o crecimiento
        :return:GEg o eg (Mj día -1)
        """
        eg = (22.02 * (self.weight / (self.fcs * self.adult_w)) ** 0.75) * self.gan ** 1.097 / self.reg / \
             (self.dep / 100)
        return eg

    def milk_energy(self):
        """
        Aporte de energía bruta de la leche consumida por el ternero.
        :return: me (Mj día -1)
        """
        me = (self.milk / 365) * ((44.01 * self.grease + 163.56) * 4.184 / 0.4536) * 10 ** -3
        return me


def main():
    ge = GrossEnergy(milk=3660, grease=3.2)
    me = ge.maintenance()
    ae = ge.activity()
    preg = ge.pregnancy()
    milk = ge.breastfeeding()
    grow = ge.grow()
    breastfeeding = ge.breastfeeding()
    work = ge.work()
    print(me + ae + preg + milk + grow + breastfeeding + work)


if __name__ == '__main__':
    main()
