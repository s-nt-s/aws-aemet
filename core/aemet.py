import logging
import os
import re
import time
from datetime import date, datetime
from functools import lru_cache
from statistics import mean, stdev
import xmltodict
import json

import bs4
import requests
from bunch import Bunch

from .provincias import prov_to_cod
from .util import mkBunch, safe_number, sexa_to_dec, YEAR, flatten_json

def get_txt(soup, slc):
    n = soup.select_one(slc)
    if n is None:
        return None
    n = n.get_text().strip()
    if n == "":
        return None
    return n


class Aemet:
    YEAR_ZERO = 1972

    def __init__(self, key=os.environ.get("AEMET_KEY")):
        self.key = key
        if self.key is None:
            logging.warning(
                "No se ha facilitado api key, por lo tanto solo estaran disponibles los endpoints xml")
        self.now = datetime.now()
        self.sleep_time = 60
        self.requests_verify = not(os.environ.get(
            "AVOID_REQUEST_VERIFY") == "true")
        logging.info("requests_verify = " + str(self.requests_verify))
        self.url = mkBunch("aemet.yml")
        self.last_url = None
        self.last_response = None

    @lru_cache(maxsize=None)
    def get_provincias(self):
        j = self.get_xml(self.url.provincias)
        provincias = set(i.get_text().strip()
                         for i in j.select("provincia id"))
        return tuple(sorted(i for i in provincias if i != ""))

    @lru_cache(maxsize=None)
    def get_municipios(self, provincia):
        url = self.url.municipios.format(loc=int(provincia))
        r = self._get(url)
        if r is None:
            return None
        provs = re.findall(r"<ID>\s*id(.+?)\s*</ID>",
                           r.text, flags=re.IGNORECASE)
        if len(provs) == 0:
            logging.critical("GET "+url+" > "+str(r.text))
        return tuple(sorted(set(provs)))

    @property
    @lru_cache(maxsize=None)
    def bases(self):
        bases = self.get_json(self.url.estaciones)
        for b in bases:
            b["latitud"] = sexa_to_dec(b["latitud"])
            b["longitud"] = sexa_to_dec(b["longitud"])
            b["provincia"] = prov_to_cod(b["provincia"])
            b["altitud"] = safe_number(b.get("altitud"), label="altitud")
        return bases

    def addkey(self, url):
        if self.key in url:
            return url
        url = url.replace("api_key=", "api_key="+self.key)
        if self.key in url:
            return url
        if "?" in url:
            return url+"&api_key="+self.key
        return url+"?api_key="+self.key

    def _get(self, url, url_debug=None, intentos=0):
        try:
            self.last_url = url_debug or url
            self.last_response = requests.get(url, verify=self.requests_verify)
            return self.last_response
        except Exception as e:
            if intentos < 4:
                logging.info("sleep:{} en {}".format(
                    self.sleep_time, url_debug))
                time.sleep(self.sleep_time)
                return self._get(url, url_debug=url_debug, intentos=intentos+1)
            logging.critical("GET "+(url_debug or url) +
                             " > "+str(e), exc_info=True)
            return None

    def get_json(self, url, no_data=None):
        r = self._get(self.addkey(url), url_debug=url)
        if r is None:
            return None
        try:
            j = r.json()
        except Exception as e:
            if "429 Too Many Requests" in r.text:
                logging.info(
                    "sleep:{} por error url_api:429:Too Many Requests".format(self.sleep_time))
                time.sleep(self.sleep_time)
                return self.get_json(url, no_data=no_data)
            logging.critical("GET "+url+" > "+str(r.text) +
                             " > "+str(e), exc_info=True)
            return None
        url_datos = j.get('datos')
        if url_datos is None:
            estado = j.get("estado")
            if estado == 404:
                # No hay datos que satisfagan esos criterios
                return no_data
            if estado == 429:
                # Too Many Requests
                logging.info(
                    "sleep:{} por error url_api:429:Too Many Requests".format(self.sleep_time))
                time.sleep(self.sleep_time)
                return self.get_json(url, no_data=no_data)
            logging.critical("GET "+url+" > "+str(j), exc_info=True)
            return None
        try:
            r = requests.get(url_datos, verify=self.requests_verify)
        except Exception as e:
            logging.critical("GET "+url_datos+" > "+str(e), exc_info=True)
            return None
        try:
            j = r.json()
        except Exception as e:
            if "429 Too Many Requests" in r.text:
                logging.info(
                    "sleep:{} por error url_datos:429:Too Many Requests".format(self.sleep_time))
                time.sleep(self.sleep_time)
                return self.get_json(url, no_data=no_data)
            logging.critical("GET "+url_datos+" > " +
                             str(r.text)+" > "+str(e), exc_info=True)
            return None
        if isinstance(j, dict) and j.get("estado") == 429:
            # Too Many Requests
            logging.info(
                "sleep:{} por error url_datos:429:Too Many Requests".format(self.sleep_time))
            time.sleep(self.sleep_time)
            return self.get_json(url, no_data=no_data)
        return j

    def get_xml(self, url):
        r = self._get(url)
        if r is None:
            return None
        try:
            soup = bs4.BeautifulSoup(r.text, 'lxml')
        except Exception as e:
            logging.critical("GET "+url+" > "+str(r.text) +
                             " > "+str(e), exc_info=True)
            return None
        return soup

    def get_prediccion(self, municipio):
        logging.info("PREDICCION "+municipio)
        url = self.url.localidad.format(municipio=municipio)
        xml = self.get_xml(url)
        if xml is None:
            return None
        arr = []
        elaborado = get_txt(xml, "elaborado")
        for dia in xml.select("prediccion > dia"):
            d = {
                "fecha": dia.attrs["fecha"].strip()
            }
            for slc in (
                "prob_precipitacion",
                "cota_nieve_prov",
                "racha_max",
                "viento velocidad"
            ):
                vals = set()
                for i, n in enumerate(dia.select(slc)):
                    n = n.get_text()
                    n = n.strip()
                    n = safe_number(n, coma=False)
                    if n is not None:
                        vals.add(n)
                        if i == 0:
                            break
                v = max(vals) if vals else None
                d[slc.replace(" ", "_")] = v
            for slc in (
                "temperatura maxima",
                "temperatura minima",
                "humedad_relativa maxima",
                "humedad_relativa minima",
                "estado_cielo",
                "sens_termica maxima",
                "sens_termica minima",
                "uv_max"
            ):
                v = get_txt(dia, slc)
                d[slc.replace(" ", "_")] = safe_number(v, coma=False, nan=v)
            d = {k:v for k,v in d.items() if v is not None}
            arr.append(d)
        return Bunch(
            elaborado=elaborado,
            dias=arr
        )

    def get_dia_estacion(self, id, year, expand=True):
        if year < Aemet.YEAR_ZERO:
            return []
        if year > YEAR:
            return None
        fin = year
        if expand:
            fin = min(year+4, YEAR)
            a = date(year, 1, 1)
            b = date(fin, 12, 31)
            if fin > year:
                bi = (a-b).days % 365
                if bi > 1:
                    fin = fin - 1
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        logging.info("DIARIO %s [%s ,%s]", id, year, fin)
        url = self.url.estacion.diario.format(id=id, ini=year, fin=fin)
        data = self.get_json(url, no_data=[])
        if data is None or not isinstance(data, list):
            return None
        for d in data:
            for k, v in list(d.items()):
                if k in del_key or v is None or (isinstance(v, str) and v.strip() == ""):
                    del d[k]
                d[k] = safe_number(v, coma=True, nan=v)
        if expand:
            expand_data = {}
            for y in range(year, fin+1):
                expand_data[y] = []
            for d in data:
                y = d["fecha"]
                y = int(y.split("-")[0])
                expand_data[y].append(d)
            return expand_data
        return data

    def get_mes_estacion(self, id, year):
        if year < Aemet.YEAR_ZERO:
            return []
        if year > YEAR:
            return None
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        logging.info("MENSUAL %s %s", id, year)
        url = self.url.estacion.mensual.format(id=id, ini=year)
        data = self.get_json(url, no_data=[])
        if data is None or not isinstance(data, list):
            return None
        for d in data:
            #year, month = map(int, d["fecha"].split("-"))
            for k, v in list(d.items()):
                if k in del_key or v is None or (isinstance(v, str) and v.strip() == ""):
                    del d[k]
                else:
                    d[k] = safe_number(v, coma=False, nan=v)
            #d["year"] = year
            #d["month"] = month
        return data


if __name__ == "__main__":
    a = Aemet()
    pre = a.get_prediccion_semanal()
    save_js("dataset/aemet/prediccion_semanal.json", pre)
