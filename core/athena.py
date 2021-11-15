import logging
import time

import boto3
import botocore

from .util import sizeof_fmt

session = boto3.Session()

logger = logging.getLogger(__name__)


def file_size(s3path: str) -> int:
    """
    Obtiene el tamaño de un fichero alojado en s3
    """
    s3 = boto3.client('s3')
    bucket, file = s3path.split("/", 3)[2:]
    response = s3.head_object(Bucket=bucket, Key=file)
    size = response['ContentLength']
    return size


class Athena:
    def __init__(self, database: str, workspace: str):
        """
        :param database: Base de datos Athena
        :param workspace: ruta s3 donde guardar resultados de consultas Athena
        """
        self.database = database
        self.client = boto3.client('athena')
        self.last_queryid = None
        self.workspace = workspace
        self.queriesid = []

    @staticmethod
    def gWhere(field: str, values: list):
        if len(values) == 1:
            v = values[0]
            if isinstance(v, str):
                return field + " = '" + v + "'"
            return field + " = " + str(v)
        return field + " in {}".format(tuple(values))

    def _query(self, sql: str) -> dict:
        """
        Lanza una consulta sql sobre Athena
        Ver [Athena.Client.start_query_execution](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.start_query_execution)
        """
        logger.info(self.database + ":\n" + sql)
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

    def query(self, *args, **kwargs) -> dict:
        """
        Lanza una consulta sql sobre Athena realizando reintentos si es necesarios
        Ver self._query
        """
        try:
            return self._query(*args, **kwargs)
        except botocore.exceptions.ClientError as e:
            r = e.response or {}
            if not (r.get("Error", {}).get("Code") == "TooManyRequestsException"):
                raise e from None
            logger.info("AWS Athena TooManyRequestsException. waiting...")
            time.sleep(10)
            return self._query(*args, **kwargs)

    def state(self, queryid: str) -> tuple:
        """
        Consulta el estado de una query previamente lanzada con Athena
        Ver [Athena.Client.get_query_execution](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution)

        :return: estado y response de la consulta
        """
        response = self.client.get_query_execution(QueryExecutionId=queryid)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            return state, response
        return None, None

    def wait(self, queryid: str = None, wait: int = 3, max_execution: int = -1, no_raise: bool = False):
        """
        Espera hasta que Athena ha terminado de ejecutar una consulta

        :param queryid: Identificador de la query por la que estamos esperando (por defecto es la última ejecutada)
        :param wait: Segundos que esperaremos entre consultas al estado de la query
        :param max_execution: Número maximo de consultas al estado de la query antes de abandonar (-1 = infinito)
        :param no_raise: Evita lanzar una excepción en caso de que la query haya fallado
        :return:
            False si la query ha fallado y no_raise = True
            ruta s3 donde se encuentra el resultado
        """
        if queryid is None:
            queryid = self.last_queryid
        if queryid is None:
            return None

        logger.info("AWS Athena {} waiting...".format(queryid))
        state = 'RUNNING'
        while (max_execution != 0 and state in ('RUNNING', 'QUEUED')):
            max_execution = max_execution - 1
            state, response = self.state(queryid)
            if state == 'FAILED':
                msg = response['QueryExecution']['Status']['StateChangeReason']
                logger.critical(msg)
                if no_raise:
                    return False
                raise Exception(msg)
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                size = file_size(s3_path)
                size = sizeof_fmt(size)
                logger.info(state + ": " + s3_path + " [" + size + "]")
                return s3_path
            time.sleep(wait)
        msg = "AWS Athena {} ({}) wait timeout".format(queryid, state)
        logger.critical(msg)
        if no_raise:
            return False
        raise Exception(msg)

    def exist(self, table: str) -> bool:
        """
        Comprueba si existe una tabla en Athena
        """
        for t in self.client.list_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=self.database,
                Expression=table
        )['TableMetadataList']:
            if t['Name'] == table:
                return True
        return False
