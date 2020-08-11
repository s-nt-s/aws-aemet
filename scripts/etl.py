#!/usr/bin/env python3

from core.bucket import Bucket
from core.athena import Athena
from core.glu import Glu
from core.util import read_file
from core.provincias import get_provincias
from core.util import YEAR_UPDATE, YEAR
import sys
import os
from datetime import datetime
import re
import time

import argparse
import logging
import sys

parser = argparse.ArgumentParser("Crea tablas PARQUET con datos agregados")
parser.add_argument('--verbose', '-v', action='count',
                    help="Nivel de depuración", default=int(os.environ.get("DEBUG_LEVEL", 0)))
args = parser.parse_args()

levels = [logging.WARNING, logging.INFO, logging.DEBUG]
level = levels[min(len(levels)-1, args.verbose)]

logging.basicConfig(
    level=level, format='%(asctime)s - %(levelname)s - %(message)s')

athena = Athena(os.environ['ATHENA_TARGET'])
bucket = Bucket(os.environ['S3_TARGET'])

def get_years(table, base):
    re_year = re.compile(r".*/year=(\d+).*")
    years = set()
    for fl in bucket.s3glob("/raw/{}/base={}/year=*".format(table, base)):
        m = re_year.search(fl)
        years.add(int(m.group(1)))
    for y in YEAR_UPDATE:
        if y in years:
            years.remove(y)
    return years

sql = read_file("sql/athena/dia_prov.sql")
create, insert = (i.strip() for i in sql.split(";")[:2])

if not athena.exist("dia_prov"):
    bucket.delete("/prq/dia_prov/")
    create = create.replace("group by", "where provincia='28'\ngroup by")
    athena.query(create)
    if not athena.wait(max_execution=-1):
        sys.exit()

for p in get_provincias():
    if bucket.exists("/prq/dia_prov/provincia={}/*".format(p)):
        logging.info("particion provincia={} ya existe".format(p))
        continue
    insr = insert.replace("group by", "where provincia='{}'\ngroup by".format(p))
    athena.query(insr)

logging.info("Athena queri status")
while athena.queriesid:
    for q in list(athena.queriesid):
        state, response = athena.state(q)
        if state is not None:
            logging.info(str(q)+" "+state)
            if state in ('FAILED', 'SUCCEEDED'):
                if state == "FAILED":
                    print(response)
                athena.queriesid.remove(q)
    if athena.queriesid:
        time.sleep(5)
