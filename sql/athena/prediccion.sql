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
  fecha is not null and
  municipio is not null and not(
    prob_precipitacion is null and
    viento_velocidad is null and
    temperatura_maxima is null and
    temperatura_minima is null and
    humedad_relativa_maxima is null and
    humedad_relativa_minima is null and
    estado_cielo is null and
    sens_termica_maxima is null and
    sens_termica_minima is null and
    racha_max is null and
    uv_max is null and
    cota_nieve_prov is null
  )
