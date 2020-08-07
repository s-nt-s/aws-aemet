import json
import logging
import os

import yaml
from bunch import Bunch
from io import BytesIO
import gzip
import shutil


def save_js(file, *datas, indent=2, **kargv):
    separators = (',', ':') if indent is None else None
    with open(file, "w") as f:
        for data in datas:
            json.dump(data, f, indent=indent, separators=separators)
        for k, v in kargv.items():
            f.write("var "+k+"=")
            json.dump(v, f, indent=indent, separators=separators)
            f.write(";\n")


def mkBunchParse(obj):
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = mkBunchParse(v)
        return obj
    if isinstance(obj, dict):
        data = []
        # Si la clave es un año lo pasamos a entero
        flag = True
        for k in obj.keys():
            if not isinstance(k, str):
                return {k: mkBunchParse(v) for k, v in obj.items()}
            if not(k.isdigit() and len(k) == 4 and int(k[0]) in (1, 2)):
                flag = False
        if flag:
            return {int(k): mkBunchParse(v) for k, v in obj.items()}
        obj = Bunch(**{k: mkBunchParse(v) for k, v in obj.items()})
        return obj
    return obj


def mkBunch(file):
    if not os.path.isfile(file):
        return None
    ext = file.rsplit(".", 1)[-1]
    with open(file, "r") as f:
        if ext == "json":
            data = json.load(f)
        elif ext == "yml":
            data = list(yaml.load_all(f, Loader=yaml.FullLoader))
            if len(data) == 1:
                data = data[0]
    data = mkBunchParse(data)
    return data


def safe_number(s, label=None, coma=False, nan=None):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    if coma:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    try:
        s = float(s)
    except Exception as e:
        if label:
            logging.critical(label+" = "+str(s) +
                             " no es un float", exc_info=True)
        return nan
    if s == int(s):
        return int(s)
    return s


def sexa_to_dec(i):
    g = i[0:2]
    m = i[2:4]
    s = i[4:6]
    o = i[-1]
    d = int(g) + (int(m) / 60) + (int(s) / 3600)
    if o in ("S", "W"):
        return -d
    return d
