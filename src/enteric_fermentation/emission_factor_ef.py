#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")
from src.enteric_fermentation.gross_energy import GrossEnergy


class FactorEF(GrossEnergy):
    def __init__(self, **kwargs):
        """
        :param kwargs: parámetros para la caractrización del animal tipo
        """
        super().__init__(**kwargs)
        self.gepd = self.gepd_calc()
        self.cms = self.cms_calc()
        self.fda = self.fda_calc()
        self.fdn = self.fdn_calc()
        self.ym = self.ym_calc()
        self.fcm = self.fcm_calc()
        if self.milk / 365 >= 11.5:
            self.ajl = 1.7
        else:
            self.ajl = 0
        self.eqsbw = self.eqsbw_calc()

    def fda_calc(self):
        """
        Fibra en detergente acido ponderada (%).
        :return: fda
        """
        fda = self.fdaf * self.pf / 100 + self.fdas * self.ps / 100
        return fda

    def fdn_calc(self):
        """
        Fibra en detergente neutro ponderada (%).
        :return: fdn
        """
        fdn = self.fdnf * self.pf / 100 + self.fdns * self.ps / 100
        return fdn

    def ym_calc(self):
        """
        Cálculo del Ym
        :return: ym
        """
        ym = ((3.41 + 0.52 * self.cms - 0.996 * (self.cms * self.fda / 100) +
               1.15 * (self.cms * self.fdn / 100)) * 100) / self.tge
        return ym

    def eqsbw_calc(self):
        """
        Peso equivalente vacío
        :return:
        """
        pev = (self.weight * 0.96) * 400 / (self.adult_w * 0.96)
        return pev

    def bfaf_calc(self):
        """
        Factor de ajuste por grasa corporal
        :return:
        """
        if self.weight > 350:
            fagc = 0.7714 + (0.00196 * (self.weight * 0.96 * self.eqsbw) / (self.adult_w * 0.96)) - \
                   (0.000000371 * (self.weight * 0.96 * self.eqsbw) / ((self.adult_w * 0.96) ** 2))
        else:
            fagc = 1
        return fagc

    def gepd_calc(self):
        """
        energía bruta ponderada de la dieta (MJ kg -1 ).
        :return:ebpd
        """
        ebpd = self.ebf * self.pf / 100 + self.ebs * self.ps / 100
        return ebpd

    def fcm_calc(self):
        """
        Leche corregida por grasa al 3.5% (kg/día)
        :return: f_cm
        """
        fcm = 0.4324 * (self.milk / 365) + 16.216 * (self.milk / 365) * (self.grease / 100)
        return fcm

    def gbvap_ef(self):
        """
        3A1ai Ganado Bovino Vacas de Alta Producción
        :return: factor de emision para la categoria 3A1ai
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def gbvbp_ef(self):
        """
        3A1aii Ganado Bovino Vacas de Baja Producción
        :return: factor de emisión para la categoria 3A1aii
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def gbvpc_ef(self):
        """
        3A1aiii Ganado Bovino Vacas para Producción de Carne
        :return: factor de emisión para la categoria 3A1aiii
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def gbtpfr_ef(self):
        """
        3A1aiv Ganado Bovino Toros utilizados con fines reproductivos
        :return: factor de emisión para la categoria 3A1aiv
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def gbtpd_ef(self):
        """
        3A1av Ganado Bovino Terneros pre-destetos
        :return: factor de emisión para la categoria 3A1av (Kg CH4 animal-1 año-1)
        """
        ef = (self.tge * (self.ym / 100) * 273.75) / 55.65
        return ef

    def gbtr_ef(self):
        """
        3A1avi Ganado Bovino Terneras de remplazo
        :return:factor de emisión para la categoria 3A1avi
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def gbge_ef(self):
        """
        3A1avii Ganado Bovino Ganado de engorde
        :return:factor de emisión para la categoria 3A1avii
        """
        ef = (self.tge * (self.ym / 100) * 365) / 55.65
        return ef

    def cms_calc(self):
        """
        Consumo de materia seca (calculado a través del consumo de energía)
        :return: cms (kg dia-1)
        """
        cms = self.tge / self.gepd
        return cms

    def cms_pv_calc(self):
        """
        Consumo de materia seca como porcentaje de peso vivo
        :return: cmspv
        """
        cmspv = self.cms / self.weight
        return cmspv

    def cmcf_calc(self):
        """
        Consumo de forraje (kg dia-1)
        :return: cf
        """
        cf = self.cms * self.pf / 100
        return cf

    def cmcs_calc(self):
        """
        Consumo de concentrado/suplemento
        :return: cf (kg día -1)
        """
        cs = self.cms * self.ps / 100
        return cs

    def cpmsgbvap(self):
        """
        Consumo potencial de materia seca para 3A1ai Ganado Bovino Vacas de Alta Producción
        :return: cpms (kg día -1 )
        """
        cpms = (0.0185 * self.weight + 0.305 * self.fcm) * (1 - (self.rcms / 100) *
                                                            (self.ta - self.tc))
        return cpms

    def cpmsgbvbp(self):
        """
        Consumo potencial de materia seca para 3A1aii Ganado Bovino Vacas de Baja Producción
        :return: cpms (kg día -1 )
        """
        cpms = ((self.weight ** 0.75) * (0.14652 * (self.enmf / 4.184)) -
                (0.0517 * (self.enmf / 4.184) ** 2) - 0.0074 + (0.305 * self.fcm) + self.ajl) * \
               (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms

    def cpmsgbpc(self):
        """
        Consumo potencial de materia seca para 3A1aiii Ganado Bovino Vacas para Producción de Carne
        :return: cpms (kg día -1 )
        """

        cpms = (((self.weight * 0.96) ** 0.75) * (0.04997 * ((self.enmf / 4.184) ** 2) + 0.04631) /
                (self.enmf / 4.184)) * (1 - (self.rcms / 100) * (self.ta - self.tc)) + \
               (0.2 * (self.milk / 365))
        return cpms

    def cpmsgbtfr(self):
        """
        Consumo potencial de materia seca para 3A1aiv Ganado Bovino Toros utilizados con fines reproductivos
        :return: cpms (kg día -1 )
        """
        cpms = (3.83 + 0.0143 * (self.weight * 0.96)) * (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms

    def cpmsgbtp(self):
        """
        Consumo potencial de materia seca para 3A1av Ganado Bovino Terneros pre-destetos
        :return: cpms (kg día -1 )
        """

        cpms = ((self.weight * 0.96) ** 0.75) * (((0.2435 * (self.enmf / 4.184)) - (0.0466 * ((self.enmf / 4.184) ** 2))
                                                  - 0.1128) / (self.enmf / 4.184)) * self.bfaf_calc() \
               * self.bi * (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms

    def cpmsgbtr(self):
        """
        Consumo potencial de materia seca para 3A1avi Ganado Bovino Terneras de remplazo
        :return: cpms (kg día -1 )
        """
        cpms = ((self.weight * 0.96) ** 0.75) * (((0.2435 * (self.enmf / 4.184)) - (0.0466 * ((self.enmf / 4.184) ** 2))
                                                  - 0.0869) / (self.enmf / 4.184)) * self.bfaf_calc() * self.bi \
               * (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms

    def cpmsgbge(self):
        """
        Consumo potencial de materia seca para 3A1avii Ganado Bovino Ganado de engorde
        :return: cpms (kg día -1 )
        """
        cpms = ((self.weight * 0.96) ** 0.75) * (((0.2435 * (self.enmf / 4.184)) - (0.0466 * ((self.enmf / 4.184) ** 2))
                                                  - 0.0869) / (self.enmf / 4.184)) * self.bfaf_calc() * self.bi \
               * (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms


def main():
    ef = FactorEF(at_id=1, ca_id=1, weight=540.0, adult_w=600.0, milk=3660, grease=3.5, cp_id=2, cs_id=1,
                  coe_act_id=2, pf=80, ps=20, vp_id=15, vs_id=40, ta=14)
    fe = ef.gbvap_ef()
    print(f"factor de emision: {round(fe, 2)}")


if __name__ == '__main__':
    main()
