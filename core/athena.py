import boto3
import time
import logging
import botocore
from .util import sizeof_fmt

params = {
    'region': 'eu-central-1',
    'database': 'databasename',
    'bucket': 'your-bucket-name',
    'path': 'temp/athena/output',
    'query': 'SELECT * FROM tablename LIMIT 100'
}

session = boto3.Session()

def file_size(s3path):
    s3 = boto3.client('s3')
    bucket, file = s3path.split("/", 3)[2:]
    response = s3.head_object(Bucket=bucket, Key=file)
    size = response['ContentLength']
    return size

class Athena:
    def __init__(self, database, workspace):
        self.database = database
        self.client = boto3.client('athena')
        self.last_queryid = None
        self.workspace = workspace
        self.queriesid = []

    @staticmethod
    def gWhere(field, values):
        if len(values) == 1:
            v = values[0]
            if isinstance(v, str):
                return field+" = '"+v+"'"
            return field+" = "+str(v)
        return field+" in {}".format(tuple(values))

    def _query(self, sql):
        logging.info(self.database+":\n"+sql)
        response = self.client.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': self.workspace
            },
            QueryExecutionContext={
                'Database': self.database
            }
        )
        self.last_queryid = response['QueryExecutionId']
        self.queriesid.append(self.last_queryid)
        return response

    def query(self, *args, **kargv):
        try:
            return self._query(*args, **kargv)
        except botocore.exceptions.ClientError as e:
            r = e.response or {}
            if not(r.get("Error", {}).get("Code") == "TooManyRequestsException"):
                raise e from None
            logging.info("AWS Athena TooManyRequestsException. waiting...")
            time.sleep(10)
            return self._query(*args, **kargv)

    def state(self, queryid):
        response = self.client.get_query_execution(QueryExecutionId=queryid)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            return state, response
        return None, None

    def wait(self, queryid=None, wait=3, max_execution=-1, no_raise=False):
        if queryid is None:
            queryid = self.last_queryid
        if queryid is None:
            return None

        logging.info("AWS Athena {} waiting...".format(queryid))
        state = 'RUNNING'
        while (max_execution != 0 and state in ('RUNNING', 'QUEUED')):
            max_execution = max_execution - 1
            state, response = self.state(queryid)
            if state == 'FAILED':
                msg = response['QueryExecution']['Status']['StateChangeReason']
                logging.critical(msg)
                if no_raise:
                    return False
                raise Exception(msg)
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                size = file_size(s3_path)
                size = sizeof_fmt(size)
                logging.info(state+": "+s3_path+" ["+size+"]")
                return s3_path
            time.sleep(wait)
        msg = "AWS Athena {} ({}) wait timeout".format(queryid, state)
        logging.critical(msg)
        if no_raise:
            return False
        raise Exception(logging)

    def exist(self, table):
        for t in self.client.list_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=self.database,
            Expression=table
        )['TableMetadataList']:
            if t['Name'] == table:
                return True
        return False
