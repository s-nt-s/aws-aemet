CREATE OR REPLACE VIEW DIA_PROV AS
select
  provincia,
  cast(year as integer) year,
  cast(fecha as date) fecha,
  sum(
    case
      when prec='Ip' then 0.09
      when prec='Acum' then null
      else cast(prec as decimal)
    end
  ) prec,
  max(presmax) presmax,
  min(presmin) presmin,
  max(racha) racha,
  max(sol) sol,
  max(tmax) tmax,
  avg(tmed) tmed,
  min(tmin) tmin,
  avg(velmedia) velmedia
from
  dia D join bases B on D.base=B.indicativo
group by B.provincia, D.year, D.fecha;

CREATE OR REPLACE VIEW MES_PROV AS
select
  provincia,
  cast(year as integer) year,
  extract(month from cast(fecha || '-01' as date)) mes,
  avg(e) e,
  avg(hr) hr,
  avg(q_mar) q_mar
from
  mes M join bases B on M.base = B.indicativo
where
  M.fecha not like '%-13'
group by B.provincia, M.year, M.fecha;
