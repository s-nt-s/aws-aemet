CREATE TABLE dia_prov
WITH (
  format='PARQUET',
  external_location='s3://aemet-db/prq/',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['provincia', 'year']
) AS
select
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
  avg(velmedia) velmedia,
  provincia,
  cast(year as integer) year
from
  dia D join bases B on D.base=B.indicativo
group by B.provincia, D.year, D.fecha;

INSERT INTO dia_prov
select
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
  avg(velmedia) velmedia,
  provincia,
  cast(year as integer) year
from
  dia D join bases B on D.base=B.indicativo
group by B.provincia, D.year, D.fecha;
