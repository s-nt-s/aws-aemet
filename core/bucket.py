import json
import logging
import os

import yaml
from bunch import Bunch
from io import StringIO, BytesIO
import gzip
import shutil
import json
import boto3

class Bucket:
    def __init__(self, name):
        self.name = name
        self.bucket = boto3.resource('s3').Bucket(name)
        self.new_files = []
        self.uploaded = []

    def up_jsgz(self, data, target, commet=None):
        if data is None:
            return
        if target.endswith("/"):
            target = target + "data"
        target = target + ".json.gz"

        if not self.exist(target):
            self.new_files.append(target)
        compressed_fp = BytesIO()
        with gzip.GzipFile(fileobj=compressed_fp, mode='w') as gz:
            if not data:
                gz.write((json.dumps(data)+"\n").encode())
            for i in data:
                gz.write((json.dumps(i)+"\n").encode())
            if commet is not None:
                gz.write("/* {} */".format(commet).encode())
        compressed_fp.seek(0)
        self.uploaded.append(target)
        r = self.bucket.upload_fileobj(
            compressed_fp,
            target,
            {'ContentType': 'application/json', 'ContentEncoding': 'gzip'}
        )

    def exist(self, target):
        objs = list(self.bucket.objects.filter(Prefix=target))
        if any([w.key == target for w in objs]):
            return True
        return False

    def get_matching_s3_objects(self, prefix="", suffix=""):
        """
        Generate objects in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
            this suffix (optional).
        """

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        kwargs = {'Bucket': self.name}

        # We can pass the prefix directly to the S3 API.  If the user has passed
        # a tuple or list of prefixes, we go through them one by one.
        if isinstance(prefix, str):
            prefixes = (prefix, )
        else:
            prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix
            for page in paginator.paginate(**kwargs):
                try:
                    contents = page["Contents"]
                except KeyError:
                    break

                for obj in contents:
                    key = obj["Key"]
                    if key.endswith(suffix):
                        yield obj


    def get_matching_s3_keys(self, prefix="", suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(prefix, suffix):
            yield obj["Key"]

    def s3glob(self, path):
        if "*" in path:
            prefix, suffix = path.split("*")
        else:
            prefix = path
            suffix = ""
        return self.get_matching_s3_keys(prefix, suffix)

    def exists(self, path):
        for i in self.s3glob(path):
            if path[-1] in ("*", "/") or path == i:
                return True
        return False

    def delete(self, prefix):
        for item in self.bucket.objects.filter(Prefix=prefix):
            item.delete()
