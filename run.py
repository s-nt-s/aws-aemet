#!/usr/bin/env python3

import logging
import os
import sys

from core.athena import Athena
from core.bucket import Bucket
from core.db import DB
from core.glue import Glue
from core.util import mkArg
from scripts.scrap import Scrap
from scripts.update import Update

arg = mkArg(
    "Scraping de la AEMET y actualización de la base de datos",
    mes="Trata los datos mensuales",
    dia="Trata los datos diarios",
    pre="Trata los datos de predicción",
    glue="Ejecutar Glue"
)

logger = logging.getLogger(__name__)

if not (arg.mes or arg.dia or arg.pre):
    sys.exit("No se ha pasado ningún parámetro")

bucket = Bucket(os.environ['S3_TARGET'])

sc = Scrap(bucket)

if arg.dia:
    sc.do_dia()
if arg.mes:
    sc.do_mes()
if arg.pre:
    sc.do_prediccion()

if not sc.need_update():
    logger.info("No hay nada que actualizar")
    sys.exit()

if arg.glue:
    glue = Glue(os.environ['GLUE_TARGET'])
    glue.start()
    glue.raise_if_error()

up = Update(
    DB(os.environ['DB_TARGET'], schema="aemet"),
    bucket,
    Athena(os.environ['ATHENA_TARGET'], "s3://{}/tmp/".format(os.environ['S3_TARGET']))
)

if arg.dia or arg.mes:
    up.do_bases()

if arg.dia:
    up.do_dia()
if arg.mes:
    up.do_mes()

if arg.dia or arg.mes:
    up.db.refresh("PROV_DIAS", "PROV_SEMANAS")

if arg.pre:
    up.do_prediccion()
    up.db.refresh("MUN_PREDICCION")

up.db.close()
