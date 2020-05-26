#!/usr/bin/env python
# -*- coding: utf-8 -*-
from enteric_fermentation.gross_energy import GrossEnergy


class FactorEF(GrossEnergy):
    def __init__(self, **kwargs):
        """
        :param kwargs: parámetros para la caractrización del animal tipo
        """
        super().__init__(**kwargs)
        self.cms = self.cms_calc()
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
        fda = self.fda_calc()
        fdn = self.fdn_calc()
        ym = (3.41 + 0.52 * self.cms - 0.996 * ((self.cms * (fda / 100)) +
                                                1.15 * (self.cms * (fdn / 100))) * 100) / self.ne
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
        :return: factor de emision para la categoria 3A2ai
        """
        assert 250 <= self.weight <= 700, "El peso vivo promedio de estas razas a nivel nacional puede variar desde " \
                                          "los 250 a 700 kg tanto promedio como adulto"
        assert 10 <= self.ta <= 18, "Temperaturas promedio para climas fríos a templados que van de 10 a 18°C"
        assert 2440 <= self.milk <= 15250, "El rango de producción de leche puede ir de 2,440 a " \
                                           "15,250 litros por lactancia"
        assert 2.0 <= self.grease <= 6.0, "El contenido de grasa en la leche puede ir desde el 2.0 % hasta el 6.0%"
        assert self.ca_id == 1, "La categoria de ganado bovino vacas de alta producción solo puede estar conformado " \
                                "animales tipo Bos Taurus (1)"
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def gbvbp_ef(self):
        """
        3A1aii Ganado Bovino Vacas de Baja Producción
        :return: factor de emisión para la categoria 3A2aii
        """
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def gbvpc_ef(self):
        """
        3A1aiii Ganado Bovino Vacas para Producción de Carne
        :return:
        """
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def gbtpfr_ef(self):
        """
        3A1aiv Ganado Bovino Toros utilizados con fines reproductivos
        :return:
        """
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def gbtpd_ef(self):
        """
        3A1av Ganado Bovino Terneros pre-destetos
        :return:
        """
        ef = (self.ne * (self.ym / 100) * 273.75) / 55.65
        return ef

    def gbtr_ef(self):
        """
        3A1avi Ganado Bovino Terneras de remplazo
        :return:
        """
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def gbge_ef(self):
        """
        3A1avii Ganado Bovino Ganado de engorde
        :return:
        """
        ef = (self.ne * (self.ym / 100) * 365) / 55.65
        return ef

    def cms_calc(self):
        """
        Consumo de materia seca (calculado a través del consumo de energía)
        :return: cms (kg dia-1)
        """
        cms = self.ne / self.gepd_calc()
        return cms

    def cms_pv_calc(self, c_ms):
        """
        Consumo de materia seca como porcentaje de peso vivo
        :param c_ms: Consumo de materia seca
        :return: cmspv
        """
        cmspv = c_ms / self.weight
        return cmspv

    def cmcf_calc(self, c_ms):
        """
        Consumo de forraje (kg dia-1)
        :param c_ms: Consumo de materia seca
        :return: cf
        """
        cf = c_ms * self.pf / 100
        return cf

    def cmcs_calc(self, c_ms):
        """
        Consumo de concentrado/suplemento
        :param c_ms: Consumo de materia seca
        :return: cf (kg día -1)
        """
        cs = c_ms * self.ps / 100
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
        Consumo potencial de materia seca para 3A1ai Ganado Bovino Vacas de Baja Producción
        :return: cpms (kg día -1 )
        """
        cpms = ((self.weight ** 0.75) * (0.14652 * (self.maintenance() / 4.184)) -
                (0.0517 * (self.maintenance() / 4.184) ** 2) - 0.0074 + (0.305 * self.fcm) + self.ajl) * \
               (1 - (self.rcms / 100) * (self.ta - self.tc))
        return cpms

    def cpmsgbpc(self):
        """
        Consumo potencial de materia seca para 3A1aiii Ganado Bovino Vacas para Producción de Carne
        :return: cpms (kg día -1 )
        """

        cpms = (((self.weight * 0.96) ** 0.75) * (0.04997 * ((self.maintenance() / 4.184) ** 2) + 0.04631) /
                (self.maintenance() / 4.184)) * (1 - (self.rcms / 100) * (self.ta - self.tc)) + \
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

        cpms = (((self.weight * 0.96) ** 0.75) * (0.2435 * ((self.maintenance() / 4.184) -
                                                            (0.0466 * ((self.maintenance() / 4.184) ** 2) - 0.1128) *
                                                            self.bfaf_calc() * self.bi *
                                                            (1 - (self.rcms / 100) * (self.ta - self.tc)))))
        return cpms

    def cpmsgbtr(self):
        """
        Consumo potencial de materia seca para 3A1avi Ganado Bovino Terneras de remplazo
        :return: cpms (kg día -1 )
        """
        cpms = (((self.weight * 0.96) ** 0.75) * (0.2435 * ((self.maintenance() / 4.184) -
                                                            (0.0466 * ((self.maintenance() / 4.184) ** 2) - 0.0869) /
                                                            (self.maintenance() / 4.184)) * self.bfaf_calc() * self.bi *
                                                  (1 - (self.rcms / 100) * (self.ta - self.tc))))
        return cpms

    def cpmsgbge(self):
        """
        Consumo potencial de materia seca para 3A1avii Ganado Bovino Ganado de engorde
        :return: cpms (kg día -1 )
        """
        cpms = (((self.weight * 0.96) ** 0.75) * (0.2435 * ((self.maintenance() / 4.184) -
                                                            (0.0466 * ((self.maintenance() / 4.184) ** 2) - 0.0869) /
                                                            (self.maintenance() / 4.184)) * self.bfaf_calc() * self.bi *
                                                  (1 - (self.rcms / 100) * (self.ta - self.tc))))
        return cpms


def main():
    ef = FactorEF(at_id=1, ca_id=1, coe_act_id=2,  ta=13, pf=100, ps=0, vp_id=1, vs_id=40, weight=500,
                  adult_w=550, cp_id=2, gan=0, milk=3660, grease=3.2, ht=0, cs_id=1,  ym_id=4)
    fe = ef.gbvap_ef()
    print(f"factor de emision: {round(fe, 2)}")


if __name__ == "__main__":
    main()
