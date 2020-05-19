#!/usr/bin/env python
# -*- coding: utf-8 -*-
import db_utils
from gross_energy import GrossEnergy


class FactorEF(GrossEnergy):
    def __init__(self, ym_id=1, **kwargs):
        """
        :param ym_id: indicie del factor ym
        :param kwargs: parámetros para la caractrización del animal tipo
        """
        super().__init__(**kwargs)
        self.ym_id = ym_id
        self.ym = db_utils.get_from_ym_table(self.ym_id)
        self.fmc = self.fcm()

    def gepd(self):
        """
        energía bruta ponderada de la dieta (MJ kg -1 ).
        :return:ebpd
        """
        ebpd = self.ebf * self.pf / 100 + self.ebs * self.ps / 100
        return ebpd

    def gbvap(self):
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
        gbvap_en = self.maintenance() + self.activity() + self.breastfeeding() + \
                   self.pregnancy() + self.work()
        ef = (gbvap_en * (self.ym / 100) * 365) / 55.65
        return ef, gbvap_en

    def gbvbp(self):
        """
        3A1aii Ganado Bovino Vacas de Baja Producción
        :return: factor de emisión para la categoria 3A2aii
        """
        gbvbp_en = self.maintenance() + self.activity() + self.breastfeeding() + \
                   self.pregnancy() + self.work()
        ef = (gbvbp_en * (self.ym / 100) * 365) / 55.65
        return ef, gbvbp_en

    def gbvpc(self):
        """
        3A1aiii Ganado Bovino Vacas para Producción de Carne
        :return:
        """
        gbvpc_en = self.maintenance() + self.activity() + self.breastfeeding() + self.pregnancy() + self.work()
        ef = (gbvpc_en * (self.ym / 100) * 365) / 55.65
        return ef, gbvpc_en

    def gbtpfr(self):
        """
        3A1aiv Ganado Bovino Toros utilizados con fines reproductivos
        :return:
        """
        gbtfr_en = self.maintenance() + self.activity() + self.work()
        ef = (gbtfr_en * (self.ym / 100) * 365) / 55.65
        return ef, gbtfr_en

    def gbtpd(self):
        """
        3A1av Ganado Bovino Terneros pre-destetos
        :return:
        """
        gbtpd_en = self.maintenance() + self.activity() + self.grow() + self.work() - \
                   self.milk()
        ef = (gbtpd_en * (self.ym / 100) * 273.75) / 55.65
        return ef, gbtpd_en

    def gbtr(self):
        """
        3A1avi Ganado Bovino Terneras de remplazo
        :return:
        """
        gbtr_en = self.maintenance() + self.activity() + self.grow() + self.work()
        ef = (gbtr_en * (self.ym / 100) * 365) / 55.65
        return ef, gbtr_en

    def gbge(self):
        """
        3A1avii Ganado Bovino Ganado de engorde
        :return:
        """
        gbge_en = self.maintenance() + self.activity() + self.grow() + self.work()
        ef = (gbge_en * (self.ym / 100) * 365) / 55.65
        return ef, gbge_en

    def cms(self, ge_t):
        """
        Consumo de materia seca (calculado a través del consumo de energía)
        :param ge_t: total gross energy
        :return: cms (kg dia-1)
        """
        gedp = self.gepd()
        c_ms = ge_t / gedp
        return c_ms

    def cms_pv(self, c_ms):
        """
        Consumo de materia seca como porcentaje de peso vivo
        :param c_ms: Consumo de materia seca
        :return: cmspv
        """
        cmspv = c_ms / self.weight
        return cmspv

    def cmcf(self, c_ms):
        """
        Consumo de forraje (kg dia-1)
        :param c_ms: Consumo de materia seca
        :return: cf
        """
        cf = c_ms * self.pf / 100
        return cf

    def cmcs(self, c_ms):
        """
        Consumo de concentrado/suplemento
        :param c_ms: Consumo de materia seca
        :return: cf (kg día -1)
        """
        cs = c_ms * self.ps / 100
        return cs

    def fcm(self):
        """
        Leche corregida por grasa al 3.5% (kg/día)
        :return: f_cm
        """
        f_cm = 0.4324 * (self.milk / 365) + 16.216 * (self.milk / 365 * self.grease / 100)
        return f_cm

    def cpmsgbvap(self):
        """
        Consumo potencial de materia seca para 3A1ai Ganado Bovino Vacas de Alta Producción
        :return: cpms (kg día -1 )
        """
        cpms = (0.0185 * self.weight + 0.305 * self.fmc) * (1 - (self.rcms / 100) *
                                                            (self.tc - self.ta))
        return cpms


def main():
    ef = FactorEF(weight=500, ta=13, milk=3660, grease=3.2, ca_id=1)
    fe, eg = ef.gbvap()
    cms = ef.cms(eg)
    print(fe, eg, cms)
    print(ef.cpmsgbvap())


if __name__ == "__main__":
    main()