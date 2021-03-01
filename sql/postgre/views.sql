DROP INDEX IF exists aemet.prov_dias_pk;
DROP INDEX IF exists aemet.mun_prediccion_pk;
DROP INDEX IF exists aemet.mun_prediccion_prov;
DROP INDEX IF exists aemet.prov_semanas_pk1;
DROP INDEX IF exists aemet.prov_semanas_pk2;
drop VIEW IF exists aemet.PROV_SEMANA_PREDICCION;
DROP VIEW IF EXISTS aemet.PROV_PREDICCION;
DROP MATERIALIZED VIEW IF EXISTS aemet.MUN_PREDICCION;
DROP MATERIALIZED VIEW IF EXISTS aemet.PROV_SEMANAS;
DROP MATERIALIZED VIEW IF EXISTS aemet.PROV_DIAS;

-- Agrupa por provincia valores de las bases
CREATE MATERIALIZED VIEW aemet.PROV_DIAS AS
select
  PD.provincia,
  PD.fecha,
  e,
  hr,
  prec,
  presmax,
  presmin,
  q_mar,
  racha,
  sol,
  tmax,
  tmed,
  tmin,
  velmedia
from
(
  select
    provincia,
    fecha,
    avg(prec) prec,
    max(presmax) presmax,
    min(presmin) presmin,
    max(racha) racha,
    max(sol) sol,
    max(tmax) tmax,
    avg(tmed) tmed,
    min(tmin) tmin,
    avg(velmedia) velmedia
  from aemet.dias D join aemet.bases B on D.base = B.id
  group by B.provincia, D.fecha
) PD
left join
(
  select
    provincia,
    fecha,
    avg(e) e,
    avg(hr) hr,
    avg(q_mar) q_mar
  from aemet.meses M join aemet.bases B on M.base = B.id
  group by B.provincia, M.FECHA
) PM
on PD.provincia=PM.provincia and PM.fecha=date_trunc('month', PD.fecha)
;
CREATE UNIQUE INDEX prov_dias_pk
ON aemet.PROV_DIAS (provincia, fecha);

-- Agrupa por semana valores de las provincias
CREATE MATERIALIZED VIEW aemet.PROV_SEMANAS AS
select
  provincia,
  EXTRACT(ISOYEAR FROM fecha) anio,
  EXTRACT(week FROM fecha) semana,
  TO_DATE(TO_CHAR(fecha, 'IYYYIW'), 'IYYYIW') lunes,
  avg(e) e,
  avg(hr) hr,
  avg(prec) prec,
  max(presmax) presmax,
  min(presmin) presmin,
  avg(q_mar) q_mar,
  max(racha) racha,
  avg(sol) sol,
  max(tmax) tmax,
  avg(tmed) tmed,
  min(tmin) tmin,
  avg(velmedia) velmedia,
  STDDEV_POP(tmed) tmed_desviacion,
  STDDEV_POP(tmax) tmax_desviacion,
  STDDEV_POP(tmin) tmin_desviacion
from
  aemet.PROV_DIAS
group by
  provincia, EXTRACT(ISOYEAR FROM fecha), EXTRACT(week FROM fecha), TO_DATE(TO_CHAR(fecha, 'IYYYIW'), 'IYYYIW')
;
CREATE UNIQUE INDEX prov_semanas_pk1
ON aemet.PROV_SEMANAS (provincia, anio, semana);
CREATE UNIQUE INDEX prov_semanas_pk2
ON aemet.PROV_SEMANAS (provincia, lunes);

-- Prediccion de los proximos dias donde para cada
-- municipio, fecha y valor se usa el ultimo elaborado
-- que no es null.
-- Con esto se busca que si en la prediccion elaborada
-- el martes no viene la racha de viento del miercoles
-- pero si venia en la predicion elaborada el lunes
-- se use el ultimo dato disponible (el elaborado el lunes)
-- en vez de dejarlo a null
CREATE MATERIALIZED VIEW aemet.MUN_PREDICCION AS
select
  substring(p.municipio, 1, 2) provincia,
  p.municipio,
  p.fecha,
  (ARRAY_AGG(p.prob_precipitacion) FILTER (WHERE p.prob_precipitacion IS NOT NULL))[1] prob_precipitacion,
  (ARRAY_AGG(p.viento_velocidad) FILTER (WHERE p.viento_velocidad IS NOT NULL))[1] viento_velocidad,
  (ARRAY_AGG(p.temperatura_maxima) FILTER (WHERE p.temperatura_maxima IS NOT NULL))[1] temperatura_maxima,
  (ARRAY_AGG(p.temperatura_minima) FILTER (WHERE p.temperatura_minima IS NOT NULL))[1] temperatura_minima,
  (ARRAY_AGG(p.humedad_relativa_maxima) FILTER (WHERE p.humedad_relativa_maxima IS NOT NULL))[1] humedad_relativa_maxima,
  (ARRAY_AGG(p.humedad_relativa_minima) FILTER (WHERE p.humedad_relativa_minima IS NOT NULL))[1] humedad_relativa_minima,
  (ARRAY_AGG(p.estado_cielo) FILTER (WHERE p.estado_cielo IS NOT NULL))[1] estado_cielo,
  (ARRAY_AGG(p.sens_termica_maxima) FILTER (WHERE p.sens_termica_maxima IS NOT NULL))[1] sens_termica_maxima,
  (ARRAY_AGG(p.sens_termica_minima) FILTER (WHERE p.sens_termica_minima IS NOT NULL))[1] sens_termica_minima,
  (ARRAY_AGG(p.racha_max) FILTER (WHERE p.racha_max IS NOT NULL))[1] racha_max,
  (ARRAY_AGG(p.uv_max) FILTER (WHERE p.uv_max IS NOT NULL))[1] uv_max,
  (ARRAY_AGG(p.cota_nieve_prov) FILTER (WHERE p.cota_nieve_prov IS NOT NULL))[1] cota_nieve_prov
from (
	select * from aemet.prediccion T
  where
  	--T.fecha>=current_date and (
  		T.prob_precipitacion is not null or
  		T.viento_velocidad is not null or
  		T.temperatura_maxima is not null or
  		T.temperatura_minima is not null or
  		T.humedad_relativa_maxima is not null or
  		T.humedad_relativa_minima is not null or
  		T.estado_cielo is not null or
  		T.sens_termica_maxima is not null or
  		T.sens_termica_minima is not null or
  		T.racha_max is not null or
  		T.uv_max is not null or
  		T.cota_nieve_prov is not null
    --)
	order by elaborado desc, fecha desc
) p
group by
  p.municipio, p.fecha
;

CREATE UNIQUE INDEX mun_prediccion_pk
ON aemet.MUN_PREDICCION (municipio, fecha);

CREATE INDEX mun_prediccion_prov
ON aemet.MUN_PREDICCION (provincia);

CREATE VIEW aemet.PROV_PREDICCION AS
select
  provincia,
  fecha,
	avg(prob_precipitacion) prob_precipitacion,
	avg(viento_velocidad) viento_velocidad,
	max(temperatura_maxima) temperatura_maxima,
	min(temperatura_minima) temperatura_minima,
	max(humedad_relativa_maxima) humedad_relativa_maxima,
	min(humedad_relativa_minima) humedad_relativa_minima,
	max(sens_termica_maxima) sens_termica_maxima,
	min(sens_termica_minima) sens_termica_minima,
	max(racha_max) racha_max,
	max(uv_max) uv_max,
	max(cota_nieve_prov) cota_nieve_prov
from
  aemet.MUN_PREDICCION
group by
  provincia, fecha
;

CREATE VIEW aemet.PROV_SEMANA_PREDICCION AS
select
  provincia,
	avg(prob_precipitacion) prob_precipitacion,
	avg(viento_velocidad) viento_velocidad,
	max(temperatura_maxima) temperatura_maxima,
	min(temperatura_minima) temperatura_minima,
	max(humedad_relativa_maxima) humedad_relativa_maxima,
	min(humedad_relativa_minima) humedad_relativa_minima,
	max(sens_termica_maxima) sens_termica_maxima,
	min(sens_termica_minima) sens_termica_minima,
	max(racha_max) racha_max,
	max(uv_max) uv_max,
	max(cota_nieve_prov) cota_nieve_prov,
  STDDEV_POP(temperatura_minima) tmin_desviacion
from
  aemet.PROV_PREDICCION
where
  fecha>=current_date and fecha<(current_date + interval '7' day)
group by
  provincia
;

commit;
