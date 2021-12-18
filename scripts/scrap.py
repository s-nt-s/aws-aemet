#!/usr/bin/env python3

import logging
import os
import re
from functools import lru_cache

from core.aemet import Aemet
from core.bucket import Bucket
from core.glue import Glue
from core.threadme import ThreadMe
from core.util import YEAR_UPDATE, YEAR, mkArg

re_year = re.compile(r".*/year=(\d+).*")

logger = logging.getLogger(__name__)


class Scrap:
    def __init__(self, bucket: Bucket):
        """
        Extrae información de la Aemet y la guarda en s3

        :param bucket: bucket donde se guardaran los datos extraídos
        """
        self.bucket = bucket
        self.api = Aemet()

    @property
    @lru_cache(maxsize=None)
    def bases(self) -> list:
        """
        Devuelve y guarda las bases de la AEMET
        """
        if not self.api.bases:
            raise Exception("Bases no encontradas")
        self.bucket.up_gz(self.api.bases, "raw/AEMET/BASES/", comment=self.api.last_url)
        return self.api.bases

    def get_years(self, table: str, base: str) -> set:
        """
        Devuelve la lista de años de los que ya tenemos datos consolidados para una tabla y una base
        Para ello obtiene los años para los que ya existe un fichero en s3 y de ellos descarta
        aquellos que aún pueden haber cambiado desde la última ejecución
        """
        years = set()
        for fl in self.bucket.s3glob("raw/AEMET/{}/base={}/year=*".format(table, base)):
            m = re_year.search(fl)
            years.add(int(m.group(1)))
        for y in YEAR_UPDATE:
            if y in years:
                years.remove(y)
        return years

    def do_dia(self):
        """
        Recupera datos históricos diarios y los guarda en s3
        """
        logger.info("AEMET DIA")
        for b in self.bases:
            visto = self.get_years("DIA", b['indicativo'])
            for y in range(Aemet.YEAR_ZERO, YEAR + 1):
                if y not in visto:
                    year_dias = self.api.get_dia_estacion(
                        b['indicativo'], y, expand=True)
                    if year_dias is not None:
                        for year, dias in year_dias.items():
                            visto.add(year)
                            if dias is None:
                                continue
                            target = "raw/AEMET/DIA/base={}/year={}/".format(b['indicativo'], year)
                            if len(dias) == 0:
                                # Creamos un txt para que la entrada no se quede vacía
                                # impidiéndonos detectar que este año ya se ha tratado
                                target = "raw/AEMET/DIA/base={}/year={}.txt".format(b['indicativo'], year)
                            self.bucket.up_gz(
                                dias,
                                target,
                                comment=self.api.last_url
                            )

    def do_mes(self):
        """
        Recupera datos históricos mensuales y los guarda en s3
        """
        logger.info("AEMET MES")
        for b in self.bases:
            visto = self.get_years("MES", b['indicativo'])
            for year in range(Aemet.YEAR_ZERO, YEAR + 1):
                if year in visto:
                    continue
                meses = self.api.get_mes_estacion(b['indicativo'], year)
                if meses is None:
                    continue
                target = "raw/AEMET/MES/base={}/year={}/".format(b['indicativo'], year)
                if len(meses) == 0:
                    # Creamos un txt para que la entrada no se quede vacía
                    # impidiéndonos detectar que este año ya se ha tratado
                    target = "raw/AEMET/MES/base={}/year={}.txt".format(b['indicativo'], year)
                self.bucket.up_gz(
                    meses,
                    target,
                    comment=self.api.last_url
                )

    def do_prediccion(self):
        """
        Recupera datos de predicciones y los guarda en s3
        """
        tm = ThreadMe(fix_param=self.api, max_thread=30)

        def do_work(api, mun):
            num_data = api.get_prediccion(mun)
            if num_data.dias:
                return num_data

        for prov in self.api.get_provincias():
            datas = list(tm.run(do_work, self.api.get_municipios(prov)))
            elab_dias = {}
            for data in datas:
                if data.elaborado not in elab_dias:
                    elab_dias[data.elaborado] = []
                dias = []
                for dia in data.dias:
                    dia = {**{"municipio": data.municipio}, **dia}
                    dias.append(dia)
                elab_dias[data.elaborado].extend(dias)

            for elaborado, dias in elab_dias.items():
                prov_target = "raw/AEMET/PREDICCION/elaborado={}/provincia={}/".format(elaborado, prov)
                dias = sorted(dias, key=lambda d: (d["municipio"], d["fecha"]))
                self.bucket.up_gz(
                    dias,
                    prov_target
                )

            for data in datas:
                target = "raw/AEMET/PREDICCION/elaborado={}/provincia={}/{}.xml".format(data.elaborado, prov,
                                                                                        data.municipio)
                self.bucket.up_gz(
                    data.source.rstrip() + "\n<!-- " + data.url + " -->",
                    target,
                    overwrite=False
                )

    def need_update(self) -> list:
        """
        Devuelve la lista de ficheros nuevos o modificados tras las tareas
        de extracción de datos
        """
        return [i.rsplit("/", 1)[0] for i in self.bucket.uploaded if i.endswith("/data.json.gz")]


if __name__ == "__main__":
    arg = mkArg(
        "Scraping de la AEMET",
        mes="Hace scraping de los datos mensuales",
        dia="Hace scraping de los datos diarios",
        pre="Hace scraping de los datos de predicción",
        glue="Ejecutar Glue"
    )
    sc = Scrap(Bucket(os.environ['S3_TARGET']))
    if arg.dia:
        sc.do_dia()
    if arg.mes:
        sc.do_mes()
    if arg.pre:
        sc.do_prediccion()
    if arg.glue and sc.need_update():
        glue = Glue(os.environ['GLUE_TARGET'])
        glue.start()
