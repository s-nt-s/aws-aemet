import logging
import os
import re
import time
from datetime import date, datetime
from functools import lru_cache

import bs4
import requests
from requests.models import Response
from munch import Munch

from .provincias import prov_to_cod
from .util import readMunch, safe_number, sexa_to_dec, YEAR

re_status = re.compile(r"<h1>\s*HTTP\s*Status\s*(\d+)\s*(.*?)</h1>", re.IGNORECASE)

logger = logging.getLogger(__name__)


def get_txt(soup, slc):
    n = soup.select_one(slc)
    if n is None:
        return None
    n = n.get_text().strip()
    if n == "":
        return None
    return n


class Aemet:
    """
    Api AEMET

    Atributos:
        YEAR_ZERO: primer año del que se tienen datos
    """
    YEAR_ZERO = 1972

    def __init__(self, key: str = os.environ.get("AEMET_KEY"), sleep_time: int = int(os.environ.get("SLEEP_TIME", 60))):
        if key in (None, ""):
            logger.warning("No se ha facilitado api key, por lo tanto solo estarán disponibles los endpoints xml")
        self.key = key
        self.now = datetime.now()
        self.sleep_time = sleep_time
        self.requests_verify = not(os.environ.get("AVOID_REQUEST_VERIFY") == "true")
        logger.info("requests_verify = " + str(self.requests_verify))
        self.url = readMunch("aemet.yml")
        self.last_url = None
        self.last_response = None
        self.count_requests = 0
        if not self.requests_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @lru_cache(maxsize=None)
    def get_provincias(self, source: str = "html") -> tuple:
        """
        Obtiene el listado de provincias
        """
        if source == "html":
            j = self.get_xml(self.url.provincias.html)
            provincias = set(i.attrs.get("value") for i in j.select("#provincia_selector option"))
        elif source == "xml":
            j = self.get_xml(self.url.provincias.xml)
            provincias = set(i.get_text().strip()
                             for i in j.select("provincia id"))
        else:
            raise Exception("Parámetro source incorrecto %s" % source)
        return tuple(sorted(i for i in provincias if i not in ("", None)))

    @lru_cache(maxsize=None)
    def get_municipios(self, provincia: str, source: str = "html") -> tuple:
        """
        Obtiene los municipios de una provincia
        """
        if source == "html":
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
        else:
            raise Exception("Parámetro source incorrecto %s" % source)
        if len(muns) == 0:
            logger.critical("GET " + url + " > " + str(self.last_response.text))
        return tuple(sorted(set(muns)))

    @property
    @lru_cache(maxsize=None)
    def bases(self) -> list:
        """
        Obtiene las bases meteorológicas y su información asociada
        """
        bases = self.get_json(self.url.estaciones)
        for b in bases:
            b["latitud"] = sexa_to_dec(b["latitud"])
            b["longitud"] = sexa_to_dec(b["longitud"])
            b["provincia"] = prov_to_cod(b["provincia"])
            b["altitud"] = safe_number(b.get("altitud"), label="altitud")
        return bases

    def addkey(self, url: str) -> str:
        """
        Añade a una url la api key
        """
        if self.key in url:
            return url
        url = url.replace("api_key=", "api_key=" + self.key)
        if self.key in url:
            return url
        if "?" in url:
            return url + "&api_key=" + self.key
        return url + "?api_key=" + self.key

    def sleep(self, log=None):
        """
        Realiza una parada y resetea el contador de requests
        """
        if log:
            logger.info("request:{} sleep:{} ".format(self.count_requests, self.sleep_time) + log)
        time.sleep(self.sleep_time)
        self.count_requests = 0

    def _get(self, url: str, url_debug: str = None, intentos: int = 4, count_requests: bool = True) -> Response:
        """
        Realiza una llamada GET a una url

        :param url: dirección a consulta
        :param url_debug: dirección a mostrar en el log
        :param intentos: número de intentos antes de reportar un error
        :param count_requests: indica si la consulta ha de incrementar el contador de requests o no
        :return: Response de la llamada GET
        """
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
                return self._get(url, url_debug=url_debug, intentos=intentos - 1)
            return r
        except Exception as e:
            if intentos > 0:
                self.sleep("{} en {}".format(str(e), log_url))
                return self._get(url, url_debug=url_debug, intentos=intentos - 1)
            logger.critical("GET " + log_url + " > " + str(e), exc_info=True)
            return None

    def _json(self, url: str, label: str):
        """
        Obtiene los datos json de una consulta a la Aemet

        :param url: dirección a consultar
        :param label: etiqueta que identifica el tipo de consulta (url_api o url_datos)
        :return: dict o list
        """
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
            logger.critical("GET " + url + " > " + str(r.text) + " > " + str(e), exc_info=True)
            return None
        if isinstance(j, dict) and j.get("estado") == 429:
            # Too Many Requests
            self.sleep("por error {}:429:Too Many Requests".format(label))
            return self._json(url, label)
        return j

    def get_json(self, url: str, no_data=None):
        """
        Obtiene los datos json de un endpoint de la api de la Aemet

        :param url: endpoint de la api Aemet
        :param no_data: Objeto a devolver en caso de no existir los datos
        :return: dict o list
        """
        j = self._json(url, "url_api")
        if j is None:
            return None
        url_datos = j.get('datos')
        if url_datos is None:
            estado = j.get("estado")
            if estado == 404:
                # No hay datos que satisfagan esos criterios
                return no_data
            logger.critical("GET " + url + " > " + str(j), exc_info=True)
            return None
        j = self._json(url_datos, "url_datos")
        return j

    def get_xml(self, url: str, with_source: bool = False):
        """
        Obtiene un xml de datos de la Aemet

        :param url: dirección a consultar
        :param with_source: indica si que quiere obtener también el texto de texto del response
        :return: bs4.BeautifulSoup y (opcionalmente) str
        """
        r = self._get(url)
        if r is None:
            return None
        try:
            soup = bs4.BeautifulSoup(r.text, 'lxml')
        except Exception as e:
            logger.critical("GET " + url + " > " + str(r.text) + " > " + str(e), exc_info=True)
            return None
        if with_source:
            return soup, r.text
        return soup

    def get_prediccion(self, municipio: str) -> Munch:
        """
        Obtiene la predicción meteorológica de un municipio

        :return: Objeto Munch con:
            elaborado: fecha de la elaboración de la predicción
            dias: listado de predicciones por día
            url: url que retorna la predicción
            source: xml original
            municipio: municipio al que corresponde la predicción
        """
        logger.info("PREDICCION " + municipio)
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
            d = {k: v for k, v in d.items() if v is not None}
            arr.append(d)
        return Munch(
            elaborado=elaborado,
            dias=arr,
            url=url,
            source=source,
            municipio=municipio
        )

    def get_dia_estacion(self, id: str, year: int, expand: bool = True):
        """
        Obtiene el histórico diario de una estación y año

        :param id: Identificador de la estación
        :param year: Año que se desea consultar
        :param expand: Indica que se obtenga todos los años posibles a partir del solicitado

        :return:
            Si expand = False: histórico (lista de días) del año solicitado
            Si expand = True: diccionario con clave año y valor histórico del año clave
        """
        if year < Aemet.YEAR_ZERO:
            return []
        if year > YEAR:
            return None
        fin = year
        if expand:
            fin = min(year + 4, YEAR)
            a = date(year, 1, 1)
            b = date(fin, 12, 31)
            if fin > year:
                bi = (a - b).days % 365
                if bi > 1:
                    fin = fin - 1
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        logger.info("DIARIO %s [%s, %s]", id, year, fin)
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
            for y in range(year, fin + 1):
                expand_data[y] = []
            for d in data:
                y = d["fecha"]
                y = int(y.split("-")[0])
                expand_data[y].append(d)
            return expand_data
        return data

    def get_mes_estacion(self, id: str, year: int) -> list:
        """
        Obtiene el histórico mensual de una estación y año

        :param id: Identificador de la estación
        :param year: Año que se desea consultar
        """
        if year < Aemet.YEAR_ZERO:
            return []
        if year > YEAR:
            return None
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        logger.info("MENSUAL %s %s", id, year)
        url = self.url.estacion.mensual.format(id=id, ini=year)
        data = self.get_json(url, no_data=[])
        if data is None or not isinstance(data, list):
            return None
        for d in data:
            # year, month = map(int, d["fecha"].split("-"))
            for k, v in list(d.items()):
                if k in del_key or v is None or (isinstance(v, str) and v.strip() == ""):
                    del d[k]
                else:
                    d[k] = safe_number(v, coma=False, nan=v)
            # d["year"] = year
            # d["month"] = month
        return data
