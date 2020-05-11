#!/usr/bin/env python
# -*- coding: utf-8 -*-
from db_utils import pg_connection


class GrossEnergy(object):
    def __init__(self, ca_id=1, coe_act_id=2,  ta=13, pf=100, pc=0, vp_id=1, vs_id=1, weight=500,
                 adult_w=550, cp_id=2, gan=0, milk=0, grease=0, ht=0, **kwargs):
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
        """
        self.ca_id = ca_id
        self.ta = ta
        assert -10 <= self.ta <= 50.0, "Verifique la temperatura ambiente. El rango permitido es entre -10 y 50 °C "
        self.pf = pf
        assert 0 <= self.pf <= 100, "Verifique el porcentaje de forraje. El rango permitido es entre 0 y 100%"
        self.pc = pc
        assert 0 <= self.pc <= 100, "Verifique el porcentaje de forraje. El rango permitido es entre 0 y 100%"
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

    def get_from_ca_table(self):
        """
        Get data from categoria animal table
        :return: a1, tc
        """
        conn = pg_connection()
        with conn as connection:
            query = """
            SELECT  coe_cat_animal, temp_conf 
            FROM categoria_animal
            WHERE id_categoria_animal = '{0}'
            """.format(self.ca_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            a1 = res[0][0]
            tc = res[0][1]
        return a1, tc

    def get_from_cp_table(self):
        """
        Get data from coeficiente de preñez table
        :return: cp
        """
        conn = pg_connection()
        with conn as connection:
            query = """
            SELECT  coe_prenez 
            FROM coeficiente_prenez
            WHERE id_coe_prenez = '{0}'
            """.format(self.cp_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            cp = res[0][0]
        return cp

    def get_from_grass_type(self):
        conn = pg_connection()
        with conn as connection:
            query = """
                    SELECT digestibilidad_dieta 
                    FROM variedad_pasto
                    WHERE id_variedad = '{0}'
                    """.format(self.vp_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            dd = res[0][0]
        return dd

    def get_from_ac_table(self):
        """
        Get data from coeficiente de actividad table
        :return: ca
        """
        conn = pg_connection()
        with conn as connection:
            query = """
                    SELECT coe_actividad 
                    FROM coeficiente_actividad
                    WHERE id_coe_actividad = '{0}'
                    """.format(self.ac_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            ca = res[0][0]
        return ca

    def get_from_cs_table(self):
        """
        Get data from condición sexual table
        :return: ca
        """
        conn = pg_connection()
        with conn as connection:
            query = """
            SELECT coe_cond_sexual 
            FROM condicion_sexual
            WHERE id_cond_sexual = '{0}'
            """.format(self.vs_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            fcs = res[0][0]
        return fcs

    def d_ep(self):
        """

        :return: Digestibilidad de la dieta - dep
        """
        assert self.pc + self.pf == 100, "La suma de los porcentajes de forraje y complemento debe ser igual a 100%"
        de_f = self.get_from_grass_type()
        de_c = self.get_from_grass_type()
        dep = de_f * self.pf / 100 + de_c * self.pc / 100
        return dep

    @staticmethod
    def rem_rel(dep):
        """
        Relación entre la energía neta disponible en una dieta para mantenimiento
        y la energía digerible consumida (rem)
        :param dep Digestibilidad de la Dieta
        :return: rem
        """
        rem = (1.123 - (4.092 * 10 ** -3 * dep) +
               (1.126 * 10 ** -5 * (dep ** 2)) - (25.4 / dep))
        return rem

    @staticmethod
    def reg_rel(dep):
        """
        Relación entre la energía neta disponible en la dieta para crecimiento y
        la energía digerible consumida
        :return: reg
        """
        reg = (1.164 - (5.16 * 10 ** -3 * dep) +
               (1.308 * 10 ** -5 * (dep ** 2)) - (37.4 / dep))
        return reg

    def maintenance(self):
        """
        Cálculo del requerimiento de energía bruta para mantenimiento GEm o em
        :return: em (Mj día -1)
        """
        a1, tc = self.get_from_ca_table()
        dep = self.d_ep()
        rem = self.rem_rel(dep)
        em = (self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta))) / rem / (dep / 100)
        return em

    def activity(self):
        """
        Cálculo del requerimiento de energía bruta de actividad GEa o ea
        :return: ea (Mj día -1)
        """
        a1, tc = self.get_from_ca_table()
        dep = self.d_ep()
        rem = self.rem_rel(dep)
        ca = self.get_from_ac_table()
        em = ((self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta)))) * ca / rem / (dep / 100)
        return em

    def pregnancy(self):
        """
        Requerimiento de energía bruta para gestación o preñez
        :return: ep (Mj día -1)
        """
        a1, tc = self.get_from_ca_table()
        dep = self.d_ep()
        rem = self.rem_rel(dep)
        cp = self.get_from_cp_table()
        ep = ((self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta)))) * cp / rem / (dep / 100)
        return ep

    def grow(self):
        """
        Requerimiento de energía bruta para ganancia de peso o crecimiento
        :return:GEg o eg (Mj día -1)
        """
        dep = self.d_ep()
        reg = self.reg_rel(dep)
        fcs = self.get_from_cs_table()
        eg = (22.02 * (self.weight / (fcs * self.adult_w)) ** 0.75) * self.gan ** 1.097 / reg / (dep / 100)
        return eg

    def breastfeeding(self):
        """
        Cálculo del requerimiento de energía bruta para lactancia o producción de leche GEl o el
        :return: el (Mj día -1)
        """
        # TODO preguntar si la leche y la grasa tienen límites, revisar si el % de leche entra completo
        dep = self.d_ep()
        rem = self.rem_rel(dep)
        el = (self.milk / 365) * (1.47 + 0.4 * self.grease) / rem / (dep / 100)
        return el

    def milk_energy(self):
        """
        Aporte de energía bruta de la leche consumida por el ternero.
        :return: me (Mj día -1)
        """
        me = (self.milk / 365) * ((44.01 * self.grease + 163.56) * 4.184 / 0.4536) * 10 ** -3
        return me

    def work(self):
        """
        Requerimiento de energía bruta para el trabajo
        :return: ew
        """
        a1, tc = self.get_from_ca_table()
        dep = self.d_ep()
        rem = self.rem_rel(dep)
        ew = ((self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta)))) * 0.1 * self.ht / rem / (dep / 100)
        return ew


def main():
    ge = GrossEnergy(milk=3660, grease=3.2)
    me = ge.maintenance()
    ae = ge.activity()
    preg = ge.pregnancy()
    milk = ge.breastfeeding()
    grow = ge.grow()
    breastfeeding = ge.breastfeeding()
    print(me + ae + preg + milk + grow + breastfeeding)


if __name__ == '__main__':
    main()
