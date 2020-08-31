import psycopg2
import boto3
from botocore.exceptions import ClientError
import logging


class DB:
    def __init__(self, config, schema=None):
        host, db, user, psw = config.split()
        self.con = psycopg2.connect(
            host=host, database=db, user=user, password=psw)
        self.schema = None

    def close(self):
        self.con.commit()
        self.con.close()

    def select(self, sql):
        c = self.con.cursor()
        c.execute(sql)
        for r in c.fetchall():
            yield r
        c.close()

    def one(self, sql):
        c = self.con.cursor()
        c.execute(sql)
        r = c.fetchone()
        if len(r) == 1:
            r = r[0]
        c.close()
        return r

    def _table(self, s):
        if not self.schema or "." in s:
            return s
        return self.schema+"."+s

    def version(self):
        return self.one("SELECT version()")

    def refresh(self, *tables):
        c = self.con.cursor()
        for table in tables:
            table = self._table(table)
            logging.info("REFRESH MATERIALIZED VIEW "+table)
            c.execute("REFRESH MATERIALIZED VIEW "+table)
            self.con.commit()
        c.close()

    def copy(self, url, table, key=None, delimiter=",", overwrite=True):
        table = self._table(table)
        logging.info("COPY in {} from {}".format(table, url))
        obj = None
        s3 = boto3.client('s3')
        try:
            bucket, file = url.split("/", 3)[2:]
            obj = s3.get_object(Bucket=bucket, Key=file)
        except ClientError:
            return False
        try:
            if self.one("select count(*) from "+table) == 0:
                key = None
            c = self.con.cursor()
            if key:
                c.execute('''
                    CREATE TEMP TABLE {0}_TMP
                    ON COMMIT DROP
                    AS
                    SELECT * FROM {0}
                    WITH NO DATA;
                '''.format(table))
            c.copy_expert(sql='''
                COPY {} FROM stdin WITH CSV HEADER DELIMITER as '{}'
            '''.format(
                (table+"_TMP") if key else table,
                delimiter
            ), file=obj['Body'])
            if key:
                if overwrite:
                    c.execute("SET CONSTRAINTS ALL DEFERRED")
                    c.execute('''
                        delete from {0}
                        where ({1}) in (
                            select {1}
                            from {0}_TMP
                        );
                    '''.format(table, key))
                else:
                    c.execute('''
                        delete from {0}_TMP
                        where ({1}) in (
                            select {1}
                            from {0}
                        );
                    '''.format(table, key))
                c.execute('''
                    insert  into {0}
                    select  *
                    from    {0}_TMP;
                '''.format(table))
            self.con.commit()
            c.close()
        except Exception as e:
            self.con.rollback()
            raise e from None
