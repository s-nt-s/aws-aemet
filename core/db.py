import psycopg2
import boto3
from botocore.exceptions import ClientError
import logging


class DB:
    def __init__(self, config, schema=None):
        host, db, user, psw = config.split()
        self.con = psycopg2.connect(
            host=host, database=db, user=user, password=psw)
        self.schema = schema
        if schema:
            db = db + "." + schema
        logging.info("{} conectada en {}".format(db, host))

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
        if r is not None and len(r) == 1:
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

    def _get_file(self, s3path):
        s3 = boto3.client('s3')
        bucket, file = s3path.split("/", 3)[2:]
        obj = s3.get_object(Bucket=bucket, Key=file)
        return obj

    def copy(self, url, table, key=None, delimiter=",", overwrite=True):
        table = self._table(table)
        tmp_table = "tmp_"+table.replace(".", "_")
        logging.info("COPY in {} from {}".format(table, url))
        obj = None
        try:
            obj = self._get_file(url)
        except ClientError:
            return False
        header = obj['Body']._raw_stream.readline()
        header = header.decode().replace('"', '')
        header = header.strip()
        obj = self._get_file(url)
        try:
            if self.one("select count(*) from "+table) == 0:
                key = None
            c = self.con.cursor()
            if key:
                c.execute('''
                    CREATE TEMP TABLE {1}
                    ON COMMIT DROP
                    AS
                    SELECT * FROM {0}
                    WITH NO DATA;
                '''.format(table, tmp_table))
            c.copy_expert(sql='''
                COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER '{}'
            '''.format(
                tmp_table if key else table,
                header,
                delimiter
            ), file=obj['Body'])
            if key:
                if overwrite:
                    c.execute("SET CONSTRAINTS ALL DEFERRED")
                    c.execute('''
                        delete from {0}
                        where ({1}) in (
                            select {1}
                            from {2}
                        );
                    '''.format(table, key, tmp_table))
                else:
                    c.execute('''
                        delete from {2}
                        where ({1}) in (
                            select {1}
                            from {0}
                        );
                    '''.format(table, key, tmp_table))
                c.execute('''
                    insert  into {0}
                    select  *
                    from    {1};
                '''.format(table, tmp_table))
            self.con.commit()
            c.close()
        except Exception as e:
            self.con.rollback()
            raise e from None
