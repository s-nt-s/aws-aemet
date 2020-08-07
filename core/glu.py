import boto3

import logging

class Glu:
    def __init__(self, name):
        self.name = name
        self.client = boto3.client('glue')

    def start(self, *update):
        if not update:
            logging.info("AWS Glu {} se lanzara por completo".format(self.name))
            return self.client.start_crawler(Name=self.name)
        logging.info("AWS Glu {} se lanzara para: {}".format(self.name, update))
        targets = [{'Path':i} for i in update]
        return self.client.update_crawler(Name=self.name, Targets={'S3Targets': targets})
