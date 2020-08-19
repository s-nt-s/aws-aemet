#!/usr/bin/env python3

from scripts.scrap import Scrap
from scripts.update import Update
from core.aemet import Aemet
from core.bucket import Bucket
from core.glue import Glue
from core.athena import Athena
from core.db import DB
from core.util import mkArg
import os
import sys

import logging


args = mkArg(
    "Scraping de la AEMET y actualización de la base de datos",
    mes="Trata los datos mensuales",
    dia="Trata los datos diarios",
    pre="Trata los datos de prediccion"
)

if not(args.mes or args.dia or args.pre):
    sys.exit("No se ha pasado ningun parametro")

bucket = Bucket(os.environ['S3_TARGET'])

sc = Scrap(bucket, Glue(os.environ['GLUE_TARGET']))

if args.dia:
    sc.do_dia()
if args.mes:
    sc.do_mes()
if args.pre:
    sc.do_prediccion()

if not (args.dia or args.pre):
    logging.info("Se actualizara la base de datos mas tarde, cuando se traten los datos diarios")
    sys.exit()

if not sc.update():
    logging.info("No hay nada que actualizar")
    sys.exit()

sc.glue.raise_if_error()

up = Update(
    DB(os.environ['DB_TARGET']),
    bucket,
    Athena(os.environ['ATHENA_TARGET'], "s3://{}/tmp/".format(os.environ['S3_TARGET']))
)
if args.dia or args.mes:
    up.do_bases()

if args.dia:
    up.do_dia()
if args.mes:
    up.do_mes()

if args.dia or args.mes:
    up.db.refresh("PROV_DIAS", "PROV_SEMANAS")

if args.pre:
    up.do_prediccion()
    up.db.refresh("MUN_PREDICCION")

up.db.close()
