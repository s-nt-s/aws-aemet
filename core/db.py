import psycopg2
import boto3

class DB:
    def __init__(self, config):
        host, db, user, psw = config.split()
        self.con = psycopg2.connect(host=host, database=db, user=user, password=psw)

    def close(self):
        self.con.close()

    def select(self, sql):
        c = self.con.cursor()
        c.execute(sql)
        for r in c.fetchall():
            yield r
        c.close()

    def version(self):
        for i in self.select("SELECT version()"):
            return i[0]

    def copy(self, url, table, key=None, delimiter="\t"):
        obj = None
        s3 = boto3.client('s3')
        try:
            bucket, file = url.split("/",3)[2:]
            obj = s3.get_object(Bucket=bucket, Key=file)
        except ClientError:
            return False
        try:
            c = self.con.cursor()
            if key:
                c.execute('''
                    CREATE TEMP TABLE tmp_{0}
                    ON COMMIT DROP
                    AS
                    SELECT * FROM {0}
                    WITH NO DATA;
                '''.format(table))
            c.copy_expert(sql='''
                COPY {} FROM stdin WITH CSV HEADER DELIMITER as '{}'
            '''.format(
                ("tmp_"+table) if key else table,
                delimiter
            ), file=obj['Body'], table)
            if key:
                c.execute('''
                    delete from {0}
                    where {1} in (
                        select {1}
                        from tmp_{0}
                    );
                '''.fromat(table, key))
                c.execute('''
                    insert  into {}
                    select  *
                    from    tmp_{};
                '''.format(table))
            self.con.commit()
            c.close()
        except Exception as e:
            self.con.rollback()
            raise e from None
