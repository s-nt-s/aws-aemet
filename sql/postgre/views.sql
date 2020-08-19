DROP MATERIALIZED VIEW IF EXISTS MUN_PREDICCION;
DROP MATERIALIZED VIEW IF EXISTS PROV_SEMANAS;
DROP MATERIALIZED VIEW IF EXISTS PROV_DIAS;

-- Agrupa por provincia valores de las bases
CREATE MATERIALIZED VIEW PROV_DIAS AS
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
    sum(prec) prec,
    max(presmax) presmax,
    min(presmin) presmin,
    max(racha) racha,
    max(sol) sol,
    max(tmax) tmax,
    avg(tmed) tmed,
    min(tmin) tmin,
    avg(velmedia) velmedia
  from dias D join bases B on D.base = B.id
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
  from meses M join bases B on M.base = B.id
  group by B.provincia, M.FECHA
) PM
on PD.provincia=PM.provincia and PM.fecha=date_trunc('month', PD.fecha)
;
CREATE UNIQUE INDEX prov_dias_pk
ON PROV_DIAS (provincia, fecha);

-- Agrupa por semana valores de las provincias
CREATE MATERIALIZED VIEW PROV_SEMANAS AS
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
  PROV_DIAS
group by
  provincia, EXTRACT(ISOYEAR FROM fecha), EXTRACT(week FROM fecha), TO_DATE(TO_CHAR(fecha, 'IYYYIW'), 'IYYYIW')
;
CREATE UNIQUE INDEX prov_semanas_pk1
ON PROV_SEMANAS (provincia, anio, semana);
CREATE UNIQUE INDEX prov_semanas_pk2
ON PROV_SEMANAS (provincia, lunes);

-- Prediccion de los proximos dias donde para cada
-- municipio, fecha y valor se usa el ultimo elaborado
-- que no es null.
-- Con esto se busca que si en la prediccion elaborada
-- el martes no viene la racha de viento del miercoles
-- pero si venia en la predicion elaborada el lunes
-- se use el ultimo dato disponible (el elaborado el lunes)
-- en vez de dejarlo a null
CREATE MATERIALIZED VIEW MUN_PREDICCION AS
select
	P.municipio,
	P.fecha,
	CASE
		when P.prob_precipitacion is not null then P.prob_precipitacion
		else (
			select prob_precipitacion from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			prob_precipitacion is not null
			order by elaborado desc
			limit 1
		)
	end prob_precipitacion,
	CASE
		when P.viento_velocidad is not null then P.viento_velocidad
		else (
			select viento_velocidad from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			viento_velocidad is not null
			order by elaborado desc
			limit 1
		)
	end viento_velocidad,
	CASE
		when P.temperatura_maxima is not null then P.temperatura_maxima
		else (
			select temperatura_maxima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			temperatura_maxima is not null
			order by elaborado desc
			limit 1
		)
	end temperatura_maxima,
	CASE
		when P.temperatura_minima is not null then P.temperatura_minima
		else (
			select temperatura_minima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			temperatura_minima is not null
			order by elaborado desc
			limit 1
		)
	end temperatura_minima,
	CASE
		when P.humedad_relativa_maxima is not null then P.humedad_relativa_maxima
		else (
			select humedad_relativa_maxima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			humedad_relativa_maxima is not null
			order by elaborado desc
			limit 1
		)
	end humedad_relativa_maxima,
	CASE
		when P.humedad_relativa_minima is not null then P.humedad_relativa_minima
		else (
			select humedad_relativa_minima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			humedad_relativa_minima is not null
			order by elaborado desc
			limit 1
		)
	end humedad_relativa_minima,
	CASE
		when P.estado_cielo is not null then P.estado_cielo
		else (
			select estado_cielo from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			estado_cielo is not null
			order by elaborado desc
			limit 1
		)
	end estado_cielo,
	CASE
		when P.sens_termica_maxima is not null then P.sens_termica_maxima
		else (
			select sens_termica_maxima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			sens_termica_maxima is not null
			order by elaborado desc
			limit 1
		)
	end sens_termica_maxima,
	CASE
		when P.sens_termica_minima is not null then P.sens_termica_minima
		else (
			select sens_termica_minima from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			sens_termica_minima is not null
			order by elaborado desc
			limit 1
		)
	end sens_termica_minima,
	CASE
		when P.racha_max is not null then P.racha_max
		else (
			select racha_max from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			racha_max is not null
			order by elaborado desc
			limit 1
		)
	end racha_max,
	CASE
		when P.uv_max is not null then P.uv_max
		else (
			select uv_max from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			uv_max is not null
			order by elaborado desc
			limit 1
		)
	end uv_max,
	CASE
		when P.cota_nieve_prov is not null then P.cota_nieve_prov
		else (
			select cota_nieve_prov from prediccion where
			fecha = P.fecha and municipio = P.municipio and
			cota_nieve_prov is not null
			order by elaborado desc
			limit 1
		)
	end cota_nieve_prov
from prediccion P
where (elaborado, municipio, fecha) in (
select
	max(T.elaborado) elaborado, T.municipio, T.fecha
from
	prediccion T
where
	T.fecha>=current_date and (
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
	)
group by
	T.municipio, T.fecha
)
;

CREATE UNIQUE INDEX mun_prediccion_pk
ON MUN_PREDICCION (municipio, fecha);

commit;
