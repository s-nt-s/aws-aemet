import logging
import os
import re
import time
from datetime import date, datetime
from functools import lru_cache
from statistics import mean, stdev

import bs4
import requests
from bunch import Bunch

from .provincias import prov_to_cod
from .util import mkBunch, safe_number, sexa_to_dec

cYear = datetime.now().year

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
        self.sleep_time = 61
        self.requests_verify = not(os.environ.get(
            "AVOID_REQUEST_VERIFY") == "true")
        logging.info("requests_verify = " + str(self.requests_verify))
        self.url = mkBunch("aemet.yml")
        self.last_url = None

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

    def _meanDict(self, keys, arr, desviacion=None):
        d = {}
        vls = {}
        for k in keys:
            values = tuple(safe_number(i[k], label=k) for i in arr)
            values = tuple(i for i in values if i is not None)
            vls[k] = values
            if len(values) == 0:
                d[k] = None
                continue
            if k.endswith("min"):
                d[k] = min(values)
            elif k.endswith("max"):
                d[k] = max(values)
            else:
                d[k] = mean(values)
        if desviacion:
            for k, new_k in desviacion.items():
                values = vls.get(k)
                if values is None or len(values) < 2:
                    d[new_k] = None
                else:
                    d[new_k] = stdev(values)
        return d

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
            r = requests.get(url, verify=self.requests_verify)
            return r
        except Exception as e:
            if intentos < 4:
                logging.info("sleep:{} en {}".format(self.sleep_time, url_debug))
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
                logging.info("sleep:{} por error url_api:429:Too Many Requests".format(self.sleep_time))
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
                logging.info("sleep:{} por error url_api:429:Too Many Requests".format(self.sleep_time))
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
                logging.info("sleep:{} por error url_datos:429:Too Many Requests".format(self.sleep_time))
                time.sleep(self.sleep_time)
                return self.get_json(url, no_data=no_data)
            logging.critical("GET "+url_datos+" > " +
                             str(r.text)+" > "+str(e), exc_info=True)
            return None
        if isinstance(j, dict) and j.get("estado")==429:
            # Too Many Requests
            logging.info("sleep:{} por error url_datos:429:Too Many Requests".format(self.sleep_time))
            time.sleep(self.sleep_time)
            return self.get_json(url, no_data=no_data)
        return j

    def get_xml(self, url):
        r = self._get(url)
        if r is None:
            return None
        try:
            soup = bs4.BeautifulSoup(r.text, 'lxml')
            '''
            name = url.split("/")[-1]
            if name.startswith("localidad_"):
                name = name[:12]+"/"+name[12:]
            name = self.now.strftime("%Y.%m.%d_%H.%M")+"/"+name
            name = "fuentes/aemet/prevision/"+name
            dir = os.path.dirname(name)
            os.makedirs(dir, exist_ok=True)
            with open(name, "w") as f:
                f.write(r.text)
            '''
        except Exception as e:
            logging.critical("GET "+url+" > "+str(r.text) +
                             " > "+str(e), exc_info=True)
            return None
        return soup

    def get_prediccion_semanal(self, *provincias, key_total=None):
        if len(provincias) == 0:
            provincias = self.get_provincias()
        keys = (
            "prec_medi",
            "vien_velm",
            "vien_rach",
            "temp_maxi",
            "temp_mini",
        )
        desviacion = {"temp_mini": "tmin_vari"}
        prediccion = {}
        for provincia in provincias:
            dt_prov = []
            for mun in self.get_municipios(provincia):
                url = self.url.localidad.format(municipio=mun)
                j = self.get_xml(url)
                if j is None:
                    continue
                dt_mun = []
                for dia in j.select("prediccion > dia")[:7]:
                    d = Bunch(
                        prec_medi=get_txt(dia, "prob_precipitacion"),
                        vien_velm=get_txt(dia, "viento velocidad"),
                        vien_rach=get_txt(dia, "racha_max"),
                        temp_maxi=get_txt(dia, "temperatura maxima"),
                        temp_mini=get_txt(dia, "temperatura minima"),
                        hume_maxi=get_txt(dia, "humedad_relativa minima"),
                        hume_mini=get_txt(dia, "humedad_relativa minima"),
                        nieve=get_txt(dia, "cota_nieve_prov"),
                        cielo=get_txt(dia, "estado_cielo"),
                        stmax=get_txt(dia, "sens_termica minima"),
                        stmin=get_txt(dia, "sens_termica minima"),
                        uvmax=get_txt(dia, "uv_max"),
                    )
                    d = {k: v for k, v in dict(d).items() if k in keys}
                    dt_mun.append(d)
                if len(dt_mun) == 0:
                    logging.critical("GET "+url+" > "+str(j))
                    continue
                dt_prov.append(self._meanDict(
                    keys, dt_mun, desviacion=desviacion))
            if len(dt_prov) == 0:
                logging.debug("get_prediccion_provincia : len(dt_prov)==0")
                continue
            if len(dt_prov) == 1:
                data = dt_prov[0]
            else:
                data = self._meanDict(keys, dt_prov, desviacion=desviacion)
            prediccion[provincia] = data
        if key_total:
            data = self._meanDict(keys, prediccion.values())
            prediccion[key_total] = data
        prediccion["__timestamp__"] = time.time()
        return prediccion

    def get_dia_estacion(self, id, year, expand=True):
        if year < Aemet.YEAR_ZERO:
            return []
        if year > cYear:
            return None
        fin = year
        if expand:
            fin = min(year+4, cYear)
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
        if year > cYear:
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
