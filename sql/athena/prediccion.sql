select
  date_parse(elaborado, '%Y-%m-%dT%H:%i:%S') elaborado,
  cast(fecha as date) fecha,
  municipio,
  prob_precipitacion,
  viento_velocidad,
  temperatura_maxima,
  temperatura_minima,
  humedad_relativa_maxima,
  humedad_relativa_minima,
  estado_cielo,
  sens_termica_maxima,
  sens_termica_minima,
  racha_max,
  uv_max,
  cota_nieve_prov
from
  prediccion
where
  fecha is not null
