import boto3

from datetime import datetime
from core.bucket import Bucket
from core.athena import Athena
from core.db import DB
from core.util import YEAR_UPDATE, YEAR, mkArg, read_file
from core.aemet import Aemet
import psycopg2
import os
import argparse
import sys

args = mkArg("Actualiza la base de datos",
    mes="Actualiza los datos mensuales",
    dia="Actualiza los datos diarios"
)

db = DB(os.environ['DB_TARGET'])
bucket = Bucket(os.environ['S3_TARGET'])
athena = Athena(os.environ['ATHENA_TARGET'], "s3://{}/tmp/".format(os.environ['S3_TARGET']))

def get_years(sql):
    visto = [i[0] for i in db.select(sql)]
    years = []
    for y in range(AEMET.YEAR_ZERO, YEAR+1):
        if y not in visto or y in YEAR_UPDATE:
            years.append(y)
    return tuple(years)

if args.dia:
    years = get_years("select distinct EXTRACT(year FROM fecha) from dias")
    years = [str(y) for y in years]
    sql = read_file("sql/athena/dia.sql") + "\n where "+Athena.gWhere("year", years)
    athena.query(sql)
    output = athena.wait()
    print(output)
    #db.copy(output, "dias", key="base, fecha")
    #bucket.delete(output)

if args.mes:
    years = get_years("select distinct EXTRACT(year FROM fecha) from meses")
    years = [str(y) for y in years]
    sql = read_file("sql/athena/mes.sql") + "\n where "+Athena.gWhere("year", years)
    athena.query(sql)
    output = athena.wait()
    print(output)
    #db.copy(output, "meses", key="base, fecha")
    #bucket.delete(output)


db.close()
