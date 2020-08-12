DROP MATERIALIZED VIEW IF EXISTS PROV_SEMANAS;
DROP MATERIALIZED VIEW IF EXISTS PROV_DIAS;

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

commit;
