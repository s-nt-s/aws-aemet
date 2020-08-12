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
