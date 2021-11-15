import gzip
import json
from io import BytesIO

import boto3


class Bucket:
    def __init__(self, name: str):
        self.name = name
        self.bucket = boto3.resource('s3').Bucket(name)
        self.new_files = []
        self.uploaded = []

    def up_gz(self, data, target: str, commet: str = None, overwrite: bool = True):
        """
        Sube un fichero a s3 y lo comprime con gzip

        :param data: datos que se van a almacenar en el fichero:
            str -> se almacena como .txt
            object -> se almacena como .json
        :param target: ruta del fichero a crear, o directorio donde debe crearse el fichero
        :param commet: comentario a incluir en el fichero (para depuración)
        :param overwrite: indica si se debe sobreescribir el fichero si este ya existe
        :return:
            True si se guarda el fichero
            False si no se guarda (ya existe y overwrite = False)
            None si no hay datos que guardar
        """
        if data is None:
            return
        content_type = None
        if target.endswith("/"):
            if isinstance(data, str):
                target = target + "data.txt"
            else:
                target = target + "data.json"

        ext = target.split(".")[-1].lower()
        if ext == "xml":
            content_type = "text/xml"
        elif ext == "json":
            content_type = "application/json"
        if ext == "txt" or isinstance(data, str):
            content_type = "text/plain"
        if content_type is None:
            raise Exception("No se ha podido determinar el ContentType")

        target = target + ".gz"

        if not self.exist(target):
            self.new_files.append(target)
        elif not overwrite:
            return False
        compressed_fp = BytesIO()
        with gzip.GzipFile(fileobj=compressed_fp, mode='w') as gz:
            if isinstance(data, str):
                gz.write(data.encode())
            else:
                if not data:
                    gz.write((json.dumps(data) + "\n").encode())
                for i in data:
                    gz.write((json.dumps(i) + "\n").encode())
            if commet is not None:
                gz.write("/* {} */".format(commet).encode())
        compressed_fp.seek(0)
        self.uploaded.append(target)
        r = self.bucket.upload_fileobj(
            compressed_fp,
            target,
            {'ContentType': content_type, 'ContentEncoding': 'gzip'}
        )
        return True

    def exist(self, target: str) -> bool:
        """
        Comprueba si existe algún objeto bajo el prefix traget
        """
        objs = list(self.bucket.objects.filter(Prefix=target))
        if any([w.key == target for w in objs]):
            return True
        return False

    def get_matching_s3_objects(self, prefix: str = "", suffix: str = ""):
        """
        Busca objetos en s3 que coincida con el prefijo y sufijo pasado por parámetro

        :param prefix: prefijo de la los objetos a devolver
        :param suffix: sufijo de los objetos a devolver
        :return: Generador con los objetos encontrados
        """

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        kwargs = {'Bucket': self.name}

        if isinstance(prefix, str):
            prefixes = (prefix,)
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

    def get_matching_s3_keys(self, prefix: str = "", suffix: str = ""):
        """
        Busca objetos en s3 que coincida con el prefijo y sufijo pasado por parámetro

        :param prefix: prefijo de la los objetos a devolver
        :param suffix: sufijo de los objetos a devolver
        :return: Generador con las claves de los objetos encontrados
        """
        for obj in self.get_matching_s3_objects(prefix, suffix):
            yield obj["Key"]

    def s3glob(self, path: str):
        """
        Busca objetos que coincidan con la ruta pasada por parámetro (admite el comodín *)
        :return: Generador con las claves de los objetos encontrados
        """
        if "*" in path:
            prefix, suffix = path.split("*")
        else:
            prefix = path
            suffix = ""
        return self.get_matching_s3_keys(prefix, suffix)

    def exists(self, path: str) -> bool:
        """
        Comprueba si existe algún objeto que coincida con la ruta pasada por parámetro (admite el comodín *)
        """
        for i in self.s3glob(path):
            if path[-1] in ("*", "/") or path == i:
                return True
        return False

    def delete(self, prefix: str):
        """
        Borra todos los objetos bajo el prefijo pasado por parámetro
        """
        for item in self.bucket.objects.filter(Prefix=prefix):
            item.delete()
