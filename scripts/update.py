import boto3

from core.bucket import Bucket
from core.athena import Athena
from core.db import DB
from core.util import YEAR_UPDATE, YEAR, mkArg, read_file
from core.aemet import Aemet
import psycopg2
import os

class Update:
    def __init__(self, db, bucket, athena):
        self.db = db
        self.bucket = bucket
        self.athena = athena

    def get_years(self, sql):
        visto = [i[0] for i in self.db.select(sql)]
        years = []
        for y in range(Aemet.YEAR_ZERO, YEAR+1):
            if y not in visto or y in YEAR_UPDATE:
                years.append(y)
        return tuple(years)

    def copy(self, sql, *args, **kargv):
        self.athena.query(sql)
        output = self.athena.wait()
        self.db.copy(output, *args, **kargv)
        self.bucket.delete(output.split("/", 3)[-1])

    def do_bases(self):
        sql = read_file("sql/athena/bases.sql").strip()
        self.copy(sql, "bases", key="id")

    def do_dia(self):
        years = self.get_years("select distinct EXTRACT(year FROM fecha) from aemet.dias")
        sql = read_file("sql/athena/dia.sql").strip()
        if years != tuple(range(Aemet.YEAR_ZERO, YEAR+1)):
            years = [str(y) for y in years]
            sql = sql.rstrip() + " and\n  "+Athena.gWhere("year", years)
        self.copy(sql,  "dias", key="base, fecha")

    def do_mes(self):
        years = self.get_years("select distinct EXTRACT(year FROM fecha) from aemet.meses")
        sql = read_file("sql/athena/mes.sql").strip()
        if years != tuple(range(Aemet.YEAR_ZERO, YEAR+1)):
            years = [str(y) for y in years]
            sql = sql.rstrip() + " and\n  "+Athena.gWhere("year", years)
        self.copy(sql, "meses", key="base, fecha")

    def do_prediccion(self):
        sql = read_file("sql/athena/prediccion.sql").strip()
        min_ela = self.db.one('''
            select elaborado from (
                select
                    cast(elaborado as date) elaborado, count(*)
                from
                    aemet.prediccion
                group by
                    cast(elaborado as date)
                order by
                    count(*) desc, cast(elaborado as date) desc
            ) T
        ''')
        if min_ela is not None:
            sql = sql + " and\n  elaborado>'{:%Y-%m-%dT00:00:00}'".format(min_ela)
        self.copy(sql,  "prediccion", key="elaborado, municipio, fecha" , overwrite=False)


if __name__ == "__main__":
    args = mkArg(
        "Actualiza la base de datos",
        mes="Actualiza los datos mensuales",
        dia="Actualiza los datos diarios",
        pre="Actualiza los datos de prediccion",
        glue="Ejecutar Glue"
    )

    up = Update(
        DB(os.environ['DB_TARGET'], schema="aemet"),
        Bucket(os.environ['S3_TARGET']),
        Athena(os.environ['ATHENA_TARGET'], "s3://{}/tmp/".format(os.environ['S3_TARGET']))
    )

    if args.glue:
        glue = Glue(os.environ['GLUE_TARGET'])
        glue.start()
        glue.raise_if_error()

    if args.dia or args.mes:
        up.do_bases()
    if args.dia:
        up.do_dia()
    if args.mes:
        up.do_mes()
    if args.pre:
        up.do_prediccion()

    up.db.close()
