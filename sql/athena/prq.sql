CREATE TABLE aemet-db.dia_prq
WITH (
  format='PARQUET',
  external_location='s3://ament-db/prq/',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['provincia', 'year']
) AS
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
