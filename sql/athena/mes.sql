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
  fecha not like '%-13'
