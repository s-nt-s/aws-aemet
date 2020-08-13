import boto3

import logging
import time


class Glue:
    def __init__(self, name):
        self.name = name
        self.client = boto3.client('glue')

    def start(self, *update):
        if update:
            logging.info(
                "AWS Glue {} se lanzara para: {}".format(self.name, update))
            targets = [{'Path': i} for i in update]
            return self.client.update_crawler(Name=self.name, Targets={'S3Targets': targets})
        logging.info("AWS Glue {} se lanzara por completo".format(self.name))
        return self.client.start_crawler(Name=self.name)


    def info(self):
        r = self.client.get_crawler(Name=self.name)
        return r["Crawler"]

    def raise_if_error(self):
        r = self.info()
        while r["State"] in ('RUNNING', 'STOPPING'):
            time.sleep(30)
            r = self.info()
        l = r.get("LastCrawl")
        if r is None:
            logging.info("AWS Glue {} aún no se ha ejecutado".format(self.name))
            return
        status = l["Status"]
        if status == 'SUCCEEDED':
            logging.info("AWS Glue {} ha tenido exito".format(self.name))
            return
        error = l['ErrorMessage']
        msg = "AWS Glue {}: {}".format(self.name, status, error)
        logging.critical(msg)
        raise Exception(msg)
