import json
import logging
import os
from datetime import datetime

import yaml
from bunch import Bunch
import argparse
import logging


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


def read_file(fl):
    with open(fl, "r") as f:
        return f.read()


def mkArg(main, **kargv):
    parser = argparse.ArgumentParser(main)
    parser.add_argument('--verbose', '-v', action='count',
                        help="Nivel de depuración", default=int(os.environ.get("DEBUG_LEVEL", 0)))
    for k, v in kargv.items():
        parser.add_argument('--'+k, action='store_true', help=v)
    args = parser.parse_args()

    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    args.verbose = levels[min(len(levels)-1, args.verbose)]

    logging.basicConfig(
        level=args.verbose, format='%(asctime)s - %(levelname)s - %(message)s')

    return args

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


now = datetime.now()
YEAR = now.year
YEAR_UPDATE = []
if now.month < 4:
    YEAR_UPDATE.append(YEAR-1)
YEAR_UPDATE.append(YEAR)
YEAR_UPDATE = tuple(YEAR_UPDATE)
