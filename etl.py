#!/usr/bin/env python3

from core.bucket import Bucket
from core.athena import Athena
from core.glu import Glu
import sys
import os
from datetime import datetime
import re

import argparse
import logging

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
    if cYear in years:
        years.remove(cYear)
    if datetime.now().month < 4 and (cYear-1) in years:
        years.remove(cYear-1)
    return years
