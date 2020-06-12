#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
from numpy import exp

sys.path.insert(0, f"{os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../'))}")

from src.enteric_fermentation.emission_factor_ef import FactorEF
from src.database.db_utils import get_from_pm_table, get_from_awms_table


class FeGe(FactorEF):
    def __init__(self, sp_id=1, sgea_id=1, p_sgea=20.0, sgra_id=4, p_sgra=80.0,
                 sgeb_id=1, p_sgeb=20.0, sgrb_id=3, p_sgrb=80.0, **kwargs):
        super().__init__(**kwargs)
        self.sp_id: int = sp_id
        self.sgea_id: int = sgea_id
        self.p_sgea: float = p_sgea
        self.sgra_id: int = sgra_id
        self.p_sgra: float = p_sgra
        self.sgeb_id: int = sgeb_id
        self.p_sgeb: float = p_sgeb
        self.sgrb_id: int = sgrb_id
        self.p_sgrb: float = p_sgrb
        self.pcp: float = self.pcp_calc()
        self.eu: float = self.eu_calc()
        self.cen_p: float = self.cen_p_calc()
        self.sv: float = self.sv_calc()
        if self.at_id == 1:
            self.pm_id = 1
        else:
            self.pm_id = 2
        self.sap, self.sbp = get_from_pm_table(self.pm_id)
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
        :return:m 3 CH4 kg -1 de VS excretados
        """
        if self.sp_id == 1:
            return self.sap
        else:
            return self.sbp

    def mcf_calc(self, sge_id):
        """
        Factores de conversión de metano para sistemas de gestión de estiércol (MCF)
        :param sge_id:
        :return: MCF
        """
        if sge_id == 1:
            mcf: float = 80.38 * 1 * (1 - exp(-0.17 * self.ta))
            return mcf
        elif sge_id == 2:
            if self.ta <= 27.0:
                mcf: float = 7.09 * exp(0.089 * self.ta)
                return mcf
            else:
                return 80.0
        elif sge_id == 3:
            if self.ta > 0.0:
                mcf: float = 5.03 / (1 + 140.65 * exp(-0.33 * self.ta))
                return mcf
            else:
                return 0.0
        elif sge_id == 4:
            if 0.0 < self.ta < 30.0:
                mcf: float = -0.82 + (0.19 * self.ta) + (-0.0032 * (self.ta ** 2))
                return mcf
            elif self.ta >= 30:
                return 2.0
            else:
                return 0.0
        elif sge_id == 5:
            return 0.47
        elif sge_id == 6:
            if 9.0 < self.ta < 30.0:
                mcf: float = -1.023 + (0.134 * self.ta) + (-0.0022 * (self.ta ** 2))
                return mcf
            elif self.ta >= 30.0:
                return 1.0
            else:
                return 0.0
        else:
            return 10.0

    def mcfp_calc(self):
        """
        Factores de conversión de metano ponderado (%).
        :return:
        """
        mcfp: float = self.mcf_calc(self.sgea_id) * self.p_sgea / 100 + self.mcf_calc(self.sgeb_id) * self.p_sgeb / 100
        return mcfp

    def pcp_calc(self):
        """
        Proteína cruda ponderada (%).
        :return:
        """
        pcp = self.pc_f * self.pf / 100 + self.pc_s * self.ps / 100
        return pcp

    def awms_sel(self, awms_id):
        """
        Seleccion de promedios para América latina de la fracción del estiércol del ganado manejado usando
        un determinado sistema de manejo de los desechos animales (AWMS), (IPCC, 2019)
        :return: awms por sistema de gestion de estiercol
        """
        awms_ap, awms_bp = get_from_awms_table(awms_id)
        if self.at_id == 1:
            return awms_ap
        elif self.at_id != 1:
            return awms_bp

    def awmsp_calc(self):
        """
        Fracción del estiércol del ganado manejado usando un determinado sistema de manejo de los
        desechos animales ponderado.
        :return:
        """
        awmsp = self.awms_sel(self.sgra_id) * self.p_sgra / 100 + self.awms_sel(self.sgrb_id) * self.p_sgrb
        return awmsp


def main():
    ef = FeGe(at_id=1, ca_id=1, weight=540.0, adult_w=600.0, milk=3660, grease=3.5, cp_id=2, cs_id=1,
              coe_act_id=2, pf=80, ps=20, vp_id=15, vs_id=1, ta=14, ht=0.0, sp_id=1, sgea_id=1, p_sgea=20.0,
              sgra_id=4, p_sgra=80.0, sgeb_id=1, p_sgeb=20.0, sgrb_id=3, p_sgrb=80.0)
    ge = ef.ef_ge_calc()
    print(f"gestion de estiercol: {round(ge, 2)}")


if __name__ == '__main__':
    main()
