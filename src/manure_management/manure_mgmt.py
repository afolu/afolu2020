#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.enteric_fermentation.emission_factor_ef import FactorEF


class FeGe(FactorEF):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sp = kwargs['sp']
        self.sv: float = self.sv_calc()
        self.eu: float = self.eu_calc()
        self.cen_p: float = self.cen_p_calc()
        self.pcp: float = self.pcp_calc()
        self.bo: float = self.bo_calc()
        self.mcfp: float = self.mcfp_calc()
        self.awmsp: float = self.awmsp_calc()

    def ef_ge_calc(self):
        """
        Estimación del factor de emisión de CH4 por la gestión del estiércol bovino
        :return:
        """
        ef: float = (self.sv * 365) * (self.bo * 0.67 * (self.mcfp / 100) * self.awmsp)
        return ef

    def sv_calc(self):
        """
        Sólidos volátiles excretados por día, kg materia seca animal-1 día-1
        :return:
        """
        sv: float = (self.tge * (1 - (self.dep / 100) + self.eu)) * ((1 - self.cen_p / 100) / 18.45)
        return sv

    def cen_p_calc(self):
        """
        Ceniza ponderada (%).
        :return:
        """
        cp: float = self.cen_f * self.pf / 100 + self.cen_s * self.ps / 100
        return cp

    def eu_calc(self):
        """
        Energía urinaria
        :return:
        """
        eu: float = (-2.71 + 0.028 * (10 * self.pcp) + 0.589 * self.cms)
        return eu

    def bo_calc(self):
        """
        Capacidad máxima de producción de metano del estiércol producido por el ganado
        :return:m 3 CH4 kg -1 deVS excretados
        """
        if self.at_id == 1 and self.sp == 1:
            return 0.24
        elif self.at_id == 1 and self.sp == 2:
            return 0.13
        elif self.at_id != 1 and self.sp == 1:
            return 0.18
        else:
            return 0.13

    def mcfp_calc(self):
        """
        Factores de conversión de metano ponderado (%).
        :return:
        """
        return 1

    def pcp_calc(self):
        """
        Proteína cruda ponderada (%).
        :return:
        """
        pcp = self.pc_f * self.pf / 100 + self.pc_s * self.ps / 100
        return pcp

    def awmsp_calc(self):
        """

        :return:
        """
        return 1


def main():
    ef = FeGe(at_id=1, ca_id=1, weight=540.0, adult_w=600.0, milk=3660, grease=3.5, cp_id=2, cs_id=1,
              coe_act_id=2, pf=80, ps=20, vp_id=15, vs_id=1, ta=14, ht=0.0, sp=2)
    ge = ef.ef_ge_calc()
    print(f"gestion de estiercol: {round(ge, 2)}")


if __name__ == '__main__':
    main()
