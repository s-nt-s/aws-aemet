CREATE OR REPLACE VIEW DIA_PROV AS
select
  B.provincia,
  cast(D.year as integer) year,
  cast(D.fecha as date) fecha,
  sum(
    case
      when D.prec='Ip' then 0.09
      when D.prec='Acum' then null
      else cast(D.prec as decimal)
    end
  ) prec,
  max(D.presmax) presmax,
  min(D.presmin) presmin,
  max(D.racha) racha,
  max(D.sol) sol,
  max(D.tmax) tmax,
  avg(D.tmed) tmed,
  min(D.tmin) tmin,
  avg(D.velmedia) velmedia
from
  dia D join bases B on D.base=B.indicativo
group by B.provincia, D.year, D.fecha;

CREATE OR REPLACE VIEW MES_PROV AS
select
  B.provincia,
  cast(M.year as integer) year,
  extract(month from cast(M.fecha || '-01' as date)) mes,
  avg(M.e) e,
  avg(M.hr) hr,
  avg(M.q_mar) q_mar
from
  mes M join bases B on M.base = B.indicativo
where
  M.fecha not like '%-13'
group by B.provincia, M.year, M.fecha;
