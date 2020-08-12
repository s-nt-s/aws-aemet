import boto3

import logging


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

    def wait(self, job):
        raise Expection("Not implemented yet")
