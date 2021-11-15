import logging
from textwrap import dedent

import boto3
import psycopg2
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DB:
    def __init__(self, config: str, schema: str = None):
        """
        Abre una conexión a una base de datos PostgreSQL
        :param config: cadena de conexión en formato: host database usuario contraseña
        :param schema: esquema de la base de datos
        """
        host, db, user, psw = config.split()
        self.con = psycopg2.connect(
            host=host, database=db, user=user, password=psw)
        self.schema = schema
        if schema:
            db = db + "." + schema
        self.db = db
        logger.info("{} conectada en {}".format(db, host))

    def execute(self, sql: str, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logger.debug(self.db + ": " + dedent(sql).rstrip())
        c.execute(sql)
        if new_c:
            c.close()

    def close(self):
        """
        Cierra la conexión a la base de datos, previo commit
        """
        self.con.commit()
        self.con.close()

    def select(self, sql: str, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logger.debug(self.db + ": " + dedent(sql).rstrip())
        c.execute(sql)
        for r in c.fetchall():
            yield r
        if new_c:
            c.close()

    def one(self, sql: str, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logger.debug(self.db + ": " + dedent(sql).rstrip())
        c.execute(sql)
        r = c.fetchone()
        if r is not None and len(r) == 1:
            r = r[0]
        if new_c:
            c.close()
        return r

    def _table(self, s: str):
        """
        Pasa el nombre de una tabla a esquema.tabla
        """
        if not self.schema or "." in s:
            return s
        return self.schema + "." + s

    def version(self):
        return self.one("SELECT version()")

    def refresh(self, *tables):
        """
        Refresca vistas materializadas
        """
        c = self.con.cursor()
        for table in tables:
            table = self._table(table)
            logger.info("REFRESH MATERIALIZED VIEW " + table)
            c.execute("REFRESH MATERIALIZED VIEW " + table)
            self.con.commit()
        c.close()

    def copy_expert(self, sql: str, file, c=None):
        """
        Realiza una sentencia COPY
        Ver [sql-copy](https://www.postgresql.org/docs/current/sql-copy.html) y
        [cursor.copy_expert](https://www.psycopg.org/docs/cursor.html#cursor.copy_expert)

        :param sql: sentencia COPY que determina a que tabla copiar los datos
        :param file: objeto file-like desde el que copiar el contenido
        :param c: cursor a reutilizar
        """
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logger.debug(self.db + ": " + dedent(sql).rstrip())
        c.copy_expert(sql=sql, file=file)
        if new_c:
            c.close()

    def _get_file(self, s3path: str):
        """
        Devuelve el objeto alojado en una ruta s3
        """
        s3 = boto3.client('s3')
        bucket, file = s3path.split("/", 3)[2:]
        obj = s3.get_object(Bucket=bucket, Key=file)
        return obj

    def copy(self, s3path: str, table: str, key: str = None, delimiter: str = ",", overwrite: bool = True) -> bool:
        """
        Copia el conenido de un csv alojado en s3 a una tabla en PostgreSQL

        :param s3path: ruta al fichero csv en s3
        :param table: tabla en la que se cargaran los datos
        :param key: campos que forman la clave en la tabla destino
        :param delimiter: carácter delimitador del csv (por defecto ,)
        :param overwrite: indica si se debe sobreescribir los datos preexistentes
        :return:
            False si no existe el fichero de origen
            True si se completa la operación con éxito
        """
        table = self._table(table)
        tmp_table = "tmp_" + table.replace(".", "_")
        logger.info("COPY in {} from {}".format(table, s3path))
        obj = None
        try:
            obj = self._get_file(s3path)
        except ClientError:
            return False
        header = obj['Body']._raw_stream.readline()
        header = header.decode().replace('"', '')
        header = header.strip()
        obj = self._get_file(s3path)
        try:
            if self.one("select count(*) from " + table) == 0:
                key = None
            c = self.con.cursor()
            if key:
                # Si hay datos y la tabla tiene una clave,
                # creamos una tabla temporal para evitar que
                # la carga del fichero duplique registros
                self.execute('''
                    CREATE TEMP TABLE {1}
                    ON COMMIT DROP
                    AS
                    SELECT * FROM {0}
                    WITH NO DATA;
                '''.format(table, tmp_table), c=c)
            # Volcamos el fichero a la tabla:
            # - directamente si esta esta vacía o no tiene clave
            # - a la tabla temporal en caso contrario
            self.copy_expert('''
                COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER '{}'
            '''.format(
                tmp_table if key else table,
                header,
                delimiter
            ), obj['Body'], c=c)
            if key:
                # Volvamos la tabla temporal a la tabla destino
                if overwrite:
                    # Si overwrite = True
                    # 1º se eliminan los registros viejos con misma clave que los nuevos
                    # 2º se cargan todos los registros nuevos
                    self.execute("SET CONSTRAINTS ALL DEFERRED", c=c)
                    self.execute('''
                        delete from {0}
                        where ({1}) in (
                            select {1}
                            from {2}
                        );
                    '''.format(table, key, tmp_table), c=c)
                    self.execute('''
                        insert  into {0}
                        select  *
                        from    {1};
                    '''.format(table, tmp_table), c=c)
                else:
                    # Si overwrite = False
                    # se cargan solo los registros nuevos que no generen conflictos con la calve
                    self.execute('''
                        insert  into {0}
                        select  *
                        from    {1}
                        ON CONFLICT({2}) DO NOTHING;
                    '''.format(table, tmp_table, key), c=c)
            self.con.commit()
            c.close()
        except Exception as e:
            self.con.rollback()
            raise e from None

        return True
