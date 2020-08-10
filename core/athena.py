import boto3
import pandas as pd
import io
import re
import time

params = {
    'region': 'eu-central-1',
    'database': 'databasename',
    'bucket': 'your-bucket-name',
    'path': 'temp/athena/output',
    'query': 'SELECT * FROM tablename LIMIT 100'
}

session = boto3.Session()



class Athena:
    def __init__(self, database):
        self.database = database
        self.client = boto3.client('athena')
        self.last_queryid = None

    def query(self, sql):
        response = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                'Database': self.database
            }
        )
        self.last_queryid = response['QueryExecutionId']
        return response

    def wait(self, queryid=None, wait=3, max_execution = 10):
        if queryid is None:
            queryid = self.last_queryid
        if queryid is None:
            return None

        state = 'RUNNING'
        while (max_execution!=0 and state in ['RUNNING', 'QUEUED']):
            max_execution = max_execution - 1
            response = self.client.get_query_execution(QueryExecutionId = queryid)

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    return False
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall('.*\/(.*)', s3_path)[0]
                    return filename
            time.sleep(wait)

        return False

    def exist(self, table):
        return self.client.list_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=self.database,
            Expression=table
        )
