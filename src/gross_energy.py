#!/usr/bin/env python
# -*- coding: utf-8 -*-
from db_utils import pg_connection


class GrossEnergy(object):
    def __init__(self, at=1, ca=1, ta=13, pf=100, pc=0, vp=1, vs=1, weight=500,
                 adult_w=550, **kwargs):
        """

        :param at: Animal tipo. Animal type. ID animal type according to the DB.
                E.g. 1. Vacas de alta producción
        :param ca: Categoria animal. Animal Category. ID Animal Category according to the DB
                E.g. 1. Bovino tipo lechero
        :param ta: Temperatura ambiente. Environmental temperature in °C. By default 13°C
        :param pf: Porcentaje de Forraje. Fodder Percentage. 100% by default (%)
        :param pc: Porcentaje de Concentrado. Synthetic food percentage. 0% by default (%)
        :param vp: Variedad de Pasto. Grass variety. ID grass variety according to the DB
                E.g. 1. Brachiaria común, Amargo, Peludo (Brachiaria decumbens)
        :param vs: Variedad de suplemento. Grass suplement variety. ID grass variety according to the DB
                E.g. 1. Brachiaria común, Amargo, Peludo (Brachiaria decumbens)
        :param weight:  Peso del animal. Animal Weight in (kg)
        :param adult_w:  Peso del animal Adulto. Animal Weight in (kg)
        """
        self.at = at
        self.ca_id = ca
        self.ta = ta
        assert -10 <= self.ta <= 50.0, "Verifique la temperatura ambiente. El rango permitido es entre -10 y 50 °C "
        self.pf = pf
        assert 0 <= self.pf <= 100, "Verifique el porcentaje de forraje. El rango permitido es entre 0 y 100%"
        self.pc = pc
        assert 0 <= self.pc <= 100, "Verifique el porcentaje de forraje. El rango permitido es entre 0 y 100%"
        self.vp = vp
        self.vs = vs
        self.weight = weight
        self.adult_w = adult_w

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

    @staticmethod
    def get_from_grass_type(vp_id=1):
        conn = pg_connection()
        with conn as connection:
            query = """
                    SELECT digestibilidad_dieta 
                    FROM variedad_pasto
                    WHERE id_variedad = '{0}'
                    """.format(vp_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            dd = res[0][0]
        return dd

    @staticmethod
    def get_from_ac_table(ca_id=1):
        """
        Get data from coeficiente de actividad table
        :param ca_id: indice de coeficiente de actividad
        :return: ca
        """
        conn = pg_connection()
        with conn as connection:
            query = """
                    SELECT coe_actividad 
                    FROM coeficiente_actividad
                    WHERE id_coe_actividad = '{0}'
                    """.format(ca_id)
            cur = connection.cursor()
            cur.execute(query)
            res = cur.fetchall()
            ca = res[0][0]
        return ca

    def d_ep(self):
        """

        :return: Digestibilidad de la dieta - dep
        """
        assert self.pc + self.pf == 100, "La suma de los porcentajes de " \
                                         "forraje y complemento debe ser igual a 100%"
        de_f = self.get_from_grass_type(self.vp)
        de_c = self.get_from_grass_type(self.vs)
        dep = de_f * self.pf / 100 + de_c * self.pc / 100
        return dep

    def em_rel(self):
        """
        Relación entre la energía neta disponible en una dieta para mantenimiento
        y la energía digerible consumida (rm)
        :return: rem
        """
        dep = self.d_ep()
        rem = (1.123 - (4.092 * 10 ** -3 * dep) +
               (1.126 * 10 ** -5 * (dep ** 2)) - (25.4 / dep))
        return rem

    def maintenance(self):
        """
        Cálculo del requerimiento de energía bruta para mantenimiento GEm o em
        :return: em
        """
        a1, tc = self.get_from_ca_table()
        em = (self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta))) / (self.em_rel()) / (self.d_ep() / 100)
        return em

    def activity(self, ca_id=1):
        """
        Cálculo del requerimiento de energía bruta para actividad GEa o ea
        :return: ea
        """
        a1, tc = self.get_from_ca_table()
        ca = self.get_from_ac_table(ca_id)
        ea = ((self.weight ** 0.75) * (a1 + (0.0029288 * (tc - self.ta)))) * ca / (self.em_rel()) / (self.d_ep() / 100)
        return ea

    def breastfeeding(self, milk=0, grease=0):
        """
        Cálculo del requerimiento de energía bruta para lactancia o producción de leche GEl o el
        :param grease: milk grease (%)
        :param milk: amount of milk for breastfeeding in liters per year (l)
        :return: el
        """

        el = (milk / 365) * (1.47 + 0.4 * grease / 100) / (self.em_rel()) / (self.d_ep() / 100)
        return el


def main():
    ge = GrossEnergy(ca=3, ta=13, weigth=500)
    # me = ge.maintenance()
    ae = ge.activity(ca_id=2)
    print(ae)


if __name__ == '__main__':
    main()
