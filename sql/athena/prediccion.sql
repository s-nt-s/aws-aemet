select
  date_parse(elaborado, '%Y-%m-%dT%H:%i:%S') elaborado,
  cast(fecha as date) fecha,
  municipio,
  IF(CAST(prob_precipitacion AS VARCHAR)='nodato', null, prob_precipitacion) prob_precipitacion,
  IF(CAST(temperatura_maxima AS VARCHAR)='nodato', null, temperatura_maxima) temperatura_maxima,
  IF(CAST(temperatura_minima AS VARCHAR)='nodato', null, temperatura_minima) temperatura_minima,
  IF(CAST(humedad_relativa_maxima AS VARCHAR)='nodato', null, humedad_relativa_maxima) humedad_relativa_maxima,
  IF(CAST(humedad_relativa_minima AS VARCHAR)='nodato', null, humedad_relativa_minima) humedad_relativa_minima,
  IF(CAST(estado_cielo AS VARCHAR)='nodato', null, estado_cielo) estado_cielo,
  IF(CAST(sens_termica_maxima AS VARCHAR)='nodato', null, sens_termica_maxima) sens_termica_maxima,
  IF(CAST(sens_termica_minima AS VARCHAR)='nodato', null, sens_termica_minima) sens_termica_minima,
  IF(CAST(uv_max AS VARCHAR)='nodato', null, uv_max) uv_max,
  IF(CAST(cota_nieve_prov AS VARCHAR)='nodato', null, cota_nieve_prov) cota_nieve_prov,
  -- La aemet da viento_velocidad y racha_max en km/h, pero
  -- lo transformamos a m/s para que sea comparable con el historico
  IF(CAST(viento_velocidad AS VARCHAR)='nodato', null, cast(viento_velocidad as decimal(8,4))*10/36) viento_velocidad,
  IF(CAST(racha_max AS VARCHAR)='nodato', null, cast(racha_max as decimal(8,4))*10/36) racha_max
from
  prediccion
where
  fecha is not null and
  municipio is not null and not(
    (prob_precipitacion is null or CAST(prob_precipitacion AS VARCHAR)='nodato') and
    (viento_velocidad is null or CAST(viento_velocidad AS VARCHAR)='nodato') and
    (temperatura_maxima is null or CAST(temperatura_maxima AS VARCHAR)='nodato') and
    (temperatura_minima is null or CAST(temperatura_minima AS VARCHAR)='nodato') and
    (humedad_relativa_maxima is null or CAST(humedad_relativa_maxima AS VARCHAR)='nodato') and
    (humedad_relativa_minima is null or CAST(humedad_relativa_minima AS VARCHAR)='nodato') and
    (estado_cielo is null or CAST(estado_cielo AS VARCHAR)='nodato') and
    (sens_termica_maxima is null or CAST(sens_termica_maxima AS VARCHAR)='nodato') and
    (sens_termica_minima is null or CAST(sens_termica_minima AS VARCHAR)='nodato') and
    (racha_max is null or CAST(racha_max AS VARCHAR)='nodato') and
    (uv_max is null or CAST(uv_max AS VARCHAR)='nodato') and
    (cota_nieve_prov is null or CAST(cota_nieve_prov AS VARCHAR)='nodato')
  )
