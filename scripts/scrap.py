#!/usr/bin/env python3

from core.aemet import Aemet
from core.bucket import Bucket
from core.glue import Glue
from core.util import YEAR_UPDATE, YEAR, mkArg
import os
from datetime import datetime
import re

import logging

args = mkArg(
    "Scraping de la AEMET",
    mes="Hace scraping de los datos mensuales",
    dia="Hace scraping de los datos diarios"
)

api = Aemet()

if not api.bases:
    raise Exception("Bases no encontradas")

re_year = re.compile(r".*/year=(\d+).*")
YEAR = datetime.now().year
bucket = Bucket(os.environ['S3_TARGET'])

bucket.up_jsgz(api.bases, "raw/AEMET/BASES/", commet=api.last_url)


def get_years(table, base):
    years = set()
    for fl in bucket.s3glob("raw/AEMET/{}/base={}/year=*".format(table, base)):
        m = re_year.search(fl)
        years.add(int(m.group(1)))
    for y in YEAR_UPDATE:
        if y in years:
            years.remove(y)
    return years


if args.dia:
    logging.info("AEMET DIA")
    for b in api.bases:
        visto = get_years("DIA", b['indicativo'])
        for y in range(Aemet.YEAR_ZERO, YEAR+1):
            if y not in visto:
                year_dias = api.get_dia_estacion(
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
                        bucket.up_jsgz(
                            dias,
                            target,
                            commet=api.last_url
                        )
if args.mes:
    logging.info("AEMET MES")
    for b in api.bases:
        visto = get_years("MES", b['indicativo'])
        for year in range(Aemet.YEAR_ZERO, YEAR+1):
            if year in visto:
                continue
            meses = api.get_mes_estacion(b['indicativo'], year)
            if meses is None:
                continue
            target = "raw/AEMET/MES/base={}/year={}/".format(
                b['indicativo'], year)
            if len(meses) == 0:
                target = "raw/AEMET/MES/base={}/year={}.txt".format(
                    b['indicativo'], year)
            bucket.up_jsgz(
                meses,
                target,
                commet=api.last_url
            )

uploaded = [i.rsplit("/", 1)[0] for i in bucket.uploaded if ".txt." not in i]
if uploaded:
    logging.info("{} ficheros actualizados".format(len(uploaded)))
    logging.info("Se ejecutara el crawler de AWS Glue")
    Glue(os.environ['GLUE_TARGET']).start()  # *uploaded)