select
  base,
  -- cast(year as integer) year,
  cast(fecha as date) fecha,
  dir,
  case
    when lower(horapresmax)='varias' then -1
    when horapresmax='' or horapresmax is null then null
    else cast(REPLACE(horapresmax, ':', '.') as decimal(4,2))
  end horapresmax,
  case
    when lower(horapresmin)='varias' then -1
    when horapresmin='' or horapresmin is null then null
    else cast(REPLACE(horapresmin, ':', '.') as decimal(4,2))
  end horapresmin,
  case
    when lower(horaracha)='varias' then -1
    when horaracha='' or horaracha is null then null
    else cast(REPLACE(horaracha, ':', '.') as decimal(4,2))
  end horaracha,
  case
    when lower(horatmax)='varias' then -1
    when horatmax='' or horatmax is null then null
    else cast(REPLACE(horatmax, ':', '.') as decimal(4,2))
  end horatmax,
  case
    when lower(horatmin)='varias' then -1
    when horatmin='' or horatmin is null then null
    else cast(REPLACE(horatmin, ':', '.') as decimal(4,2))
  end horatmin,
  case
    when prec='Ip' then 0.09
    when prec='Acum' then null
    else cast(prec as decimal(5,2))
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
where
  base is not null and
  fecha is not null
