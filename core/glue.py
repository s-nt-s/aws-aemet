import logging
import time

import boto3

logger = logging.getLogger(__name__)


class Glue:
    def __init__(self, name: str):
        """
        Gestiona un crawler de Glue
        :param name: nombre del crawler de Glue
        """
        self.name = name
        self.client = boto3.client('glue')

    def start(self, *update):
        """
        Arranca el trabajo glue
        :param update: En caso de ser una actualización, lista de rutas s3 con los targets a actualizar

        Ver [Glue.Client.start_crawler](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_crawler) y
        [Glue.Client.update_crawler](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.update_crawler)
        """
        if update:
            logger.info(
                "AWS Glue {} se lanzara para: {}".format(self.name, update))
            targets = [{'Path': i} for i in update]
            return self.client.update_crawler(Name=self.name, Targets={'S3Targets': targets})
        logger.info("AWS Glue {} se lanzara por completo".format(self.name))
        return self.client.start_crawler(Name=self.name)

    def info(self) -> dict:
        """
        Devuelve información sobre el crawler

        Ver [Glue.Client.get_crawler](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_crawler)
        """
        r = self.client.get_crawler(Name=self.name)
        return r["Crawler"]

    def raise_if_error(self):
        """
        Espera a la finalización del crawler y lanza una excepción si termina con error
        """
        r = self.info()
        while r["State"] in ('RUNNING', 'STOPPING'):
            time.sleep(30)
            r = self.info()
        l = r.get("LastCrawl")
        if r is None:
            logger.info("AWS Glue {} aún no se ha ejecutado".format(self.name))
            return
        status = l["Status"]
        if status == 'SUCCEEDED':
            logger.info("AWS Glue {} ha tenido éxito".format(self.name))
            return
        error = l['ErrorMessage']
        msg = "AWS Glue {}: {}".format(self.name, status, error)
        logger.critical(msg)
        raise Exception(msg)
