#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gross_energy import GrossEnergy


class FactorEF(GrossEnergy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def gbvap(self):
        """
        3A2ai Ganado Bovino Vacas de Alta Producción
        :return:
        """
        assert 250 <= self.weight <= 700, "El peso vivo promedio de estas razas a nivel nacional puede variar desde " \
                                          "los 250 a 700 kg tanto promedio como adulto"
        assert 10 <= self.ta <= 18, "Temperaturas promedio para climas fríos a templados que van de 10 a 18°C"
        assert 2440 <= self.milk <= 15250, "El rango de producción de leche puede ir de 2,440 a " \
                                           "15,250 litros por lactancia"
        assert 2.0 <= self.grease <= 6.0, "El contenido de grasa en la leche puede ir desde el 2.0 % hasta el 6.0%"

        gbvap_en = self.maintenance() + self.activity() + self.breastfeeding() + self.pregnancy()
        return gbvap_en

    def gbvbp(self):
        """
        3A2aii Ganado Bovino Vacas de Baja Producción
        :return:
        """
        gbvbp_en = self.maintenance() + self.activity() + self.breastfeeding() + self.pregnancy()
        return gbvbp_en

    def gbvpc(self):
        """
        3A2aiii Ganado Bovino Vacas para Producción de Carne
        :return:
        """
        gbvpc_en = self.maintenance() + self.activity() + self.breastfeeding() + self.pregnancy()
        return gbvpc_en

    def gbtpfr(self):
        """
        3A2aiv Ganado Bovino Toros utilizados con fines reproductivos
        :return:
        """
        gbtpfr_en = self.maintenance() + self.activity()
        return gbtpfr_en

    def gbtpd(self):
        """
        3A2av Ganado Bovino Terneros pre-destetos
        :return:
        """
        gbtpd_en = self.maintenance() + self.activity() + self.grow() + self.milk()
        return gbtpd_en

    def gbtr(self):
        """
        3A2avi Ganado Bovino Terneras de remplazo
        :return:
        """
        gbtr_en = self.maintenance() + self.activity() + self.grow()
        return gbtr_en

    def gbge(self):
        """
        3A2avii Ganado Bovino Ganado de engorde
        :return:
        """
        gbge_en = self.maintenance() + self.activity() + self.grow()
        return gbge_en


def main():
    ef = FactorEF(weight=500, ta=13, milk=3660, grease=3.2, ca_id=1)
    hp = ef.gbvap()
    print(hp)


if __name__ == "__main__":
    main()
