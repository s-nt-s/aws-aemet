#!/usr/bin/env python3

from core.aemet import Aemet
from core.bucket import Bucket
from core.glue import Glue
from core.util import YEAR_UPDATE, YEAR, mkArg
import os
from datetime import datetime
from functools import lru_cache
import re

import logging

re_year = re.compile(r".*/year=(\d+).*")

class Scrap:
    def __init__(self, bucket, glue):
        self.bucket = bucket
        self.api = Aemet()
        self.glue = glue

    @property
    @lru_cache(maxsize=None)
    def bases(self):
        if not self.api.bases:
            raise Exception("Bases no encontradas")
        self.bucket.up_jsgz(self.api.bases, "raw/AEMET/BASES/", commet=self.api.last_url)
        return self.api.bases

    def get_years(self, table, base):
        years = set()
        for fl in self.bucket.s3glob("raw/AEMET/{}/base={}/year=*".format(table, base)):
            m = re_year.search(fl)
            years.add(int(m.group(1)))
        for y in YEAR_UPDATE:
            if y in years:
                years.remove(y)
        return years

    def do_dia(self):
        logging.info("AEMET DIA")
        for b in self.bases:
            visto = self.get_years("DIA", b['indicativo'])
            for y in range(Aemet.YEAR_ZERO, YEAR+1):
                if y not in visto:
                    year_dias = self.api.get_dia_estacion(
                        b['indicativo'], y, expand=True)
                    if year_dias is not None:
                        for year, dias in year_dias.items():
                            visto.add(year)
                            if dias is None:
                                continue
                            target = "raw/AEMET/DIA/base={}/year={}/".format(
                                b['indicativo'], year)
                            if len(dias) == 0:
                                target = "raw/AEMET/DIA/base={}/year={}.txt".format(
                                    b['indicativo'], year)
                            self.bucket.up_jsgz(
                                dias,
                                target,
                                commet=self.api.last_url
                            )
    def do_mes(self):
        logging.info("AEMET MES")
        for b in self.bases:
            visto = self.get_years("MES", b['indicativo'])
            for year in range(Aemet.YEAR_ZERO, YEAR+1):
                if year in visto:
                    continue
                meses = self.api.get_mes_estacion(b['indicativo'], year)
                if meses is None:
                    continue
                target = "raw/AEMET/MES/base={}/year={}/".format(
                    b['indicativo'], year)
                if len(meses) == 0:
                    target = "raw/AEMET/MES/base={}/year={}.txt".format(
                        b['indicativo'], year)
                self.bucket.up_jsgz(
                    meses,
                    target,
                    commet=self.api.last_url
                )

    def do_prediccion(self):
        for prov in self.api.get_provincias():
            for mun in self.api.get_municipios(prov):
                data = self.api.get_prediccion(mun)
                if data.dias:
                    target = "raw/AEMET/PREDICCION/elaborado={}/provincia={}/municipio={}/".format(data.elaborado, prov, mun)
                    self.bucket.up_jsgz(
                        data.dias,
                        target,
                        commet=self.api.last_url+"\n\n"+self.api.last_response.text,
                        overwrite=False
                    )


    def update(self):
        uploaded = [i.rsplit("/", 1)[0] for i in self.bucket.uploaded if ".txt." not in i]
        if uploaded:
            logging.info("{} ficheros actualizados".format(len(uploaded)))
            self.glue.start()  # *uploaded)
            return True
        return False

if __name__ == "__main__":
    args = mkArg(
        "Scraping de la AEMET",
        mes="Hace scraping de los datos mensuales",
        dia="Hace scraping de los datos diarios",
        pre="Hace scraping de los datos de prediccion"
    )
    sc = Scrap(Bucket(os.environ['S3_TARGET']), Glue(os.environ['GLUE_TARGET']))
    if args.dia:
        sc.do_dia()
    if args.mes:
        sc.do_mes()
    if args.pre:
        sc.do_prediccion()
    if sc.update():
        sc.glue.raise_if_error()
