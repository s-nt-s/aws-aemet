select
  base,
  cast(year as integer) year,
  cast(fecha as date) fecha,
  case
    when prec='Ip' then 0.09
    when prec='Acum' then null
    else cast(prec as decimal)
  end prec,
  presmax presmax,
  presmin presmin,
  racha racha,
  sol sol,
  tmax tmax,
  tmed tmed,
  tmin tmin,
  velmedia velmedia
from
  dia
