import os
from datetime import timedelta

from core.aemet import Aemet
from core.athena import Athena
from core.bucket import Bucket
from core.db import DB
from core.util import YEAR_UPDATE, YEAR, mkArg, read_file


class Update:
    def __init__(self, db: DB, bucket: Bucket, athena: Athena):
        """
        Actualiza la base de datos a partir de ficheros en s3 usando consultas Athena

        :param db: Base de datos a actualizar
        :param bucket: Bucket que usa de workspace Athena
        :param athena: Base de datos Athema
        """
        self.db = db
        self.bucket = bucket
        self.athena = athena

    def get_years(self, sql: str) -> tuple:
        """
        Obtiene los de una sql que coinciden con años a actualizar

        :param sql: sql que consulta los años de una tabla
        """
        visto = [i[0] for i in self.db.select(sql)]
        years = []
        for y in range(Aemet.YEAR_ZERO, YEAR + 1):
            if y not in visto or y in YEAR_UPDATE:
                years.append(y)
        return tuple(years)

    def copy(self, sql: str, *args, **kwargs):
        """
        Copia el resultado de una consulta de Athena a la base de datos
        """
        self.athena.query(sql)
        output = self.athena.wait()
        self.db.copy(output, *args, **kwargs)
        self.bucket.delete(output.split("/", 3)[-1])

    def do_bases(self):
        """
        Copia a la base de datos las bases de la AEMET guardadas en Athena
        """
        sql = read_file("sql/athena/bases.sql").strip()
        self.copy(sql, "bases", key="id")

    def do_dia(self):
        """
        Copia a la base de datos el histórico diario de la AEMET guardado en Athena
        """
        years = self.get_years("select distinct EXTRACT(year FROM fecha) from aemet.dias")
        sql = read_file("sql/athena/dia.sql").strip()
        if years != tuple(range(Aemet.YEAR_ZERO, YEAR + 1)):
            years = [str(y) for y in years]
            sql = sql.rstrip() + " and\n  " + Athena.gWhere("year", years)
        self.copy(sql, "dias", key="base, fecha")

    def do_mes(self):
        """
        Copia a la base de datos el histórico mensual de la AEMET guardado en Athena
        """
        years = self.get_years("select distinct EXTRACT(year FROM fecha) from aemet.meses")
        sql = read_file("sql/athena/mes.sql").strip()
        if years != tuple(range(Aemet.YEAR_ZERO, YEAR + 1)):
            years = [str(y) for y in years]
            sql = sql.rstrip() + " and\n  " + Athena.gWhere("year", years)
        self.copy(sql, "meses", key="base, fecha")

    def do_prediccion(self):
        """
        Copia a la base de datos las predicciones de la AEMET guardadas en Athena
        """
        sql = read_file("sql/athena/prediccion.sql").strip()
        min_ela = self.db.one("select max(cast(elaborado as date)) from aemet.prediccion")
        if min_ela is not None:
            min_ela = min_ela - timedelta(days=10)
            sql = sql + " and\n  elaborado>'{:%Y-%m-%dT00:00:00}'".format(min_ela)
        self.copy(sql, "prediccion", key="elaborado, municipio, fecha", overwrite=False)


if __name__ == "__main__":
    from core.glue import Glue

    arg = mkArg(
        "Actualiza la base de datos",
        mes="Actualiza los datos mensuales",
        dia="Actualiza los datos diarios",
        pre="Actualiza los datos de predicción",
        glue="Ejecutar Glue"
    )

    up = Update(
        DB(os.environ['DB_TARGET'], schema="aemet"),
        Bucket(os.environ['S3_TARGET']),
        Athena(os.environ['ATHENA_TARGET'], "s3://{}/tmp/".format(os.environ['S3_TARGET']))
    )

    if arg.glue:
        glue = Glue(os.environ['GLUE_TARGET'])
        glue.start()
        # glue.raise_if_error()

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
