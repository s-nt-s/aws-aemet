select
  base,
  cast(fecha || '-01' as date) fecha,
  e,
  evap,
  glo,
  hr,
  inso,
  n_cub,
  n_des,
  n_fog,
  n_gra,
  n_llu,
  n_nie,
  n_nub,
  n_tor,
  np_001,
  np_010,
  np_100,
  np_300,
  nt_00,
  nt_30,
  nv_0050,
  nv_0100,
  nv_1000,
  nw_55,
  nw_91,
  -- p_max,
  p_mes,
  p_sol,
  q_mar,
  -- q_max varchar,
  q_med,
  -- q_min varchar,
  -- ta_max varchar,
  -- ta_min varchar,
  ti_max,
  tm_max,
  tm_mes,
  tm_min,
  ts_10,
  ts_20,
  ts_50,
  ts_min,
  w_med,
  -- w_racha varchar,
  w_rec
from
  mes
where
  base is not null and
  fecha is not null and
  fecha not like '%-13' and not(
    e is null and
    evap is null and
    glo is null and
    hr is null and
    inso is null and
    n_cub is null and
    n_des is null and
    n_fog is null and
    n_gra is null and
    n_llu is null and
    n_nie is null and
    n_nub is null and
    n_tor is null and
    np_001 is null and
    np_010 is null and
    np_100 is null and
    np_300 is null and
    nt_00 is null and
    nt_30 is null and
    nv_0050 is null and
    nv_0100 is null and
    nv_1000 is null and
    nw_55 is null and
    nw_91 is null and
    -- p_max is null and
    p_mes is null and
    p_sol is null and
    q_mar is null and
    -- q_max is null and
    q_med is null and
    -- q_min is null and
    -- ta_max is null and
    -- ta_min is null and
    ti_max is null and
    tm_max is null and
    tm_mes is null and
    tm_min is null and
    ts_10 is null and
    ts_20 is null and
    ts_50 is null and
    ts_min is null and
    w_med is null and
    -- w_racha is null and
    w_rec is null
  )
