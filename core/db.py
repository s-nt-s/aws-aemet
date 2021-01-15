import psycopg2
import boto3
from botocore.exceptions import ClientError
import logging
from textwrap import dedent


class DB:
    def __init__(self, config, schema=None):
        host, db, user, psw = config.split()
        self.con = psycopg2.connect(
            host=host, database=db, user=user, password=psw)
        self.schema = schema
        if schema:
            db = db + "." + schema
        self.db = db
        logging.info("{} conectada en {}".format(db, host))

    def execute(self, sql, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logging.debug(self.db+": "+dedent(sql).rstrip())
        c.execute(sql)
        if new_c:
            c.close()

    def close(self):
        self.con.commit()
        self.con.close()

    def select(self, sql, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logging.debug(self.db+": "+dedent(sql).rstrip())
        c.execute(sql)
        for r in c.fetchall():
            yield r
        if new_c:
            c.close()

    def one(self, sql, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logging.debug(self.db+": "+dedent(sql).rstrip())
        c.execute(sql)
        r = c.fetchone()
        if r is not None and len(r) == 1:
            r = r[0]
        if new_c:
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

    def copy_expert(self, sql, file, c=None):
        new_c = c is None
        if new_c:
            c = self.con.cursor()
        logging.debug(self.db+": "+dedent(sql).rstrip())
        c.copy_expert(sql=sql, file=file)
        if new_c:
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
                self.execute('''
                    CREATE TEMP TABLE {1}
                    ON COMMIT DROP
                    AS
                    SELECT * FROM {0}
                    WITH NO DATA;
                '''.format(table, tmp_table), c=c)
            self.copy_expert('''
                COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER '{}'
            '''.format(
                tmp_table if key else table,
                header,
                delimiter
            ), obj['Body'], c=c)
            if key:
                if overwrite:
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
