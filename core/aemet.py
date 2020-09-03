import logging
import os
import re
import time
from datetime import date, datetime
from functools import lru_cache
from statistics import mean, stdev
import json

import bs4
import requests
from bunch import Bunch

from .provincias import prov_to_cod
from .util import mkBunch, safe_number, sexa_to_dec, YEAR, flatten_json

re_status = re.compile(r"<h1>\s*HTTP\s*Status\s*(\d+)\s*(.*?)</h1>", re.IGNORECASE)

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
        self.count_requests = 0
        if not self.requests_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @lru_cache(maxsize=None)
    def get_provincias(self, source="html"):
        if source=="html":
            j = self.get_xml(self.url.provincias.html)
            provincias = set(i.attrs.get("value") for i in j.select("#provincia_selector option"))
        elif source == "xml":
            j = self.get_xml(self.url.provincias.xml)
            provincias = set(i.get_text().strip()
                             for i in j.select("provincia id"))
        return tuple(sorted(i for i in provincias if i not in ("", None)))

    @lru_cache(maxsize=None)
    def get_municipios(self, provincia, source="html"):
        if source=="html":
            url = self.url.municipios.html.format(loc=int(provincia))
            r = self.get_xml(url)
            muns = set()
            for i in r.select("#localidades_selector option"):
                i = i.attrs.get("value")
                if i is not None and "-id" in i:
                    muns.add(i[-5:])
        elif source == "xml":
            url = self.url.municipios.xml.format(loc=int(provincia))
            r = self._get(url)
            if r is None:
                return None
            muns = re.findall(r"<ID>\s*id(.+?)\s*</ID>",
                               r.text, flags=re.IGNORECASE)
        if len(muns) == 0:
            logging.critical("GET "+url+" > "+str(self.last_response.text))
        return tuple(sorted(set(muns)))

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

    def sleep(self, log=None):
        if log:
            logging.info("request:{} sleep:{} ".format(self.count_requests, self.sleep_time)+log)
        time.sleep(self.sleep_time)
        self.count_requests = 0

    def _get(self, url, url_debug=None, intentos=4, count_requests=True):
        log_url = (url_debug or url)
        try:
            if count_requests:
                self.count_requests = self.count_requests + 1
                self.last_url = log_url
                self.last_response = None
            r = requests.get(url, verify=self.requests_verify)
            if count_requests:
                self.last_response = r
            m = re_status.search(r.text)
            if m and intentos > 0:
                status, error = m.groups()
                error = error.lstrip("-").lstrip()
                self.sleep("status:{} en {} - {}".format(status, log_url, error))
                return self._get(url, url_debug=url_debug, intentos=intentos-1)
            return r
        except Exception as e:
            if intentos > 0:
                self.sleep("{} en {}".format(str(e), log_url))
                return self._get(url, url_debug=url_debug, intentos=intentos-1)
            logging.critical("GET "+log_url+ " > "+str(e), exc_info=True)
            return None

    def _json(self, url, label):
        if label == "url_datos":
            r = self._get(self.addkey(url), url_debug=url, count_requests=False, intentos=0)
        else:
            r = self._get(self.addkey(url), url_debug=url)
        if r is None:
            return None
        try:
            j = r.json()
        except Exception as e:
            if "429 Too Many Requests" in r.text:
                self.sleep("por error {}:429:Too Many Requests".format(label))
                return self._json(url, label)
            logging.critical("GET "+url+" > "+str(r.text) + " > "+str(e), exc_info=True)
            return None
        if isinstance(j, dict) and j.get("estado") == 429:
            # Too Many Requests
            self.sleep("por error {}:429:Too Many Requests".format(label))
            return self._json(url, label)
        return j


    def get_json(self, url, no_data=None):
        j = self._json(url, "url_api")
        if j is None:
            return None
        url_datos = j.get('datos')
        if url_datos is None:
            estado = j.get("estado")
            if estado == 404:
                # No hay datos que satisfagan esos criterios
                return no_data
            logging.critical("GET "+url+" > "+str(j), exc_info=True)
            return None
        j = self._json(url_datos, "url_datos")
        return j

    def get_xml(self, url, with_source=False):
        r = self._get(url)
        if r is None:
            return None
        try:
            soup = bs4.BeautifulSoup(r.text, 'lxml')
        except Exception as e:
            logging.critical("GET "+url+" > "+str(r.text) +
                             " > "+str(e), exc_info=True)
            return None
        if with_source:
            return soup, r.text
        return soup

    def get_prediccion(self, municipio):
        logging.info("PREDICCION "+municipio)
        url = self.url.localidad.format(municipio=municipio)
        xml = self.get_xml(url, with_source=True)
        if xml is None:
            return None
        xml, source = xml
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
            dias=arr,
            url=url,
            source=source,
            municipio=municipio
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
