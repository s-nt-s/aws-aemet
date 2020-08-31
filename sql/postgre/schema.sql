-- CREATE database municipios;

DROP TABLE IF EXISTS aemet.meses;
DROP TABLE IF EXISTS aemet.dias;
DROP TABLE IF EXISTS aemet.bases;
DROP TABLE IF EXISTS aemet.prediccion;

CREATE TABLE aemet.bases (
  id varchar NOT NULL,
  provincia varchar(2) NOT NULL,
  nombre varchar NULL,
  indsinop varchar NULL,
  latitud float8 NULL,
  longitud float8 NULL,
  altitud float8 NULL,
  CONSTRAINT bases_pk PRIMARY KEY (id)
);
COMMENT ON TABLE aemet.bases IS 'Bases de la AEMET';
COMMENT ON COLUMN aemet.bases.id IS 'indicativo';

CREATE TABLE aemet.dias (
  base varchar NOT NULL,
  fecha date NOT NULL,
  dir int4 NULL,
  horapresmax float4 NULL,
  horapresmin float4 NULL,
  horaracha float4 NULL,
  horatmax float4 NULL,
  horatmin float4 NULL,
  prec float4 NULL,
  presmax float4 NULL,
  presmin float4 NULL,
  racha float4 NULL,
  sol float4 NULL,
  tmax float4 NULL,
  tmed float4 NULL,
  tmin float4 NULL,
  velmedia float4 NULL,
  CONSTRAINT dia_pk PRIMARY KEY (base, fecha),
  CONSTRAINT dia_fk FOREIGN KEY (base) REFERENCES aemet.bases(id) DEFERRABLE
);

COMMENT ON TABLE aemet.dias IS 'Historico diario';
COMMENT ON COLUMN aemet.dias.dir IS 'Dirección de la racha máxima en decenas de grado';
COMMENT ON COLUMN aemet.dias.horapresmax IS 'Hora de la presión máxima (redondeada a la hora entera más próxima). Formato HH.MM (-1 = varias)';
COMMENT ON COLUMN aemet.dias.horapresmin IS 'Hora de la presión mínima (redondeada a la hora entera más próxima). Formato HH.MM (-1 = varias)';
COMMENT ON COLUMN aemet.dias.horaracha IS 'Hora y minuto de la racha máxima. Formato HH.MM (-1 = varias)';
COMMENT ON COLUMN aemet.dias.horatmax IS 'hora de la tmin. Formato HH.MM (-1 = varias)';
COMMENT ON COLUMN aemet.dias.horatmin IS 'hora de la tmax. Formato HH.MM (-1 = varias)';
COMMENT ON COLUMN aemet.dias.prec IS 'Precipitación diaria de 07 a 07 en mm o 0.09 (‘Ip’) si es mejor que 0,1 mm';
COMMENT ON COLUMN aemet.dias.presmax IS 'Prexión máxima (hPa) al nivel de referencia de la estación';
COMMENT ON COLUMN aemet.dias.presmin IS 'Prexión mínima (hPa) al nivel de referencia de la estación';
COMMENT ON COLUMN aemet.dias.racha IS 'Racha máxima del viento en m/s';
COMMENT ON COLUMN aemet.dias.sol IS 'horas de sol';
COMMENT ON COLUMN aemet.dias.tmax IS 'Temperatura máxima (grados Celsius)';
COMMENT ON COLUMN aemet.dias.tmed IS 'Temperatura média (grados Celsius)';
COMMENT ON COLUMN aemet.dias.tmin IS 'Temperatura mínima (grados Celsius)';
COMMENT ON COLUMN aemet.dias.velmedia IS 'Velocidad media del viento en m/s';


CREATE TABLE aemet.meses (
  base varchar NOT NULL,
  fecha date NOT NULL,
  e int4 NULL,
  evap int4 NULL,
  glo int4 NULL,
  hr int4 NULL,
  inso float4 NULL,
  n_cub int4 NULL,
  n_des int4 NULL,
  n_fog int4 NULL,
  n_gra int4 NULL,
  n_llu int4 NULL,
  n_nie int4 NULL,
  n_nub int4 NULL,
  n_tor int4 NULL,
  np_001 int4 NULL,
  np_010 int4 NULL,
  np_100 int4 NULL,
  np_300 int4 NULL,
  nt_00 int4 NULL,
  nt_30 int4 NULL,
  nv_0050 int4 NULL,
  nv_0100 int4 NULL,
  nv_1000 int4 NULL,
  nw_55 int4 NULL,
  nw_91 int4 NULL,
  -- p_max varchar NULL,
  p_mes float4 NULL,
  p_sol int4 NULL,
  q_mar float4 NULL,
  -- q_max varchar NULL,
  q_med float4 NULL,
  -- q_min varchar NULL,
  -- ta_max varchar NULL,
  -- ta_min varchar NULL,
  ti_max float4 NULL,
  tm_max float4 NULL,
  tm_mes float4 NULL,
  tm_min float4 NULL,
  ts_10 float4 NULL,
  ts_20 float4 NULL,
  ts_50 float4 NULL,
  ts_min float4 NULL,
  w_med int4 NULL,
  -- w_racha varchar NULL,
  w_rec int4 NULL,
  CONSTRAINT mes_pk PRIMARY KEY (base, fecha),
  CONSTRAINT mes_fk FOREIGN KEY (base) REFERENCES aemet.bases(id) DEFERRABLE
);

COMMENT ON TABLE aemet.meses IS 'Historico mensual';
COMMENT ON COLUMN aemet.meses.fecha IS 'Solo importa el año y el mes';
COMMENT ON COLUMN aemet.meses.e IS 'Tensión de vapor media en décimas hPa';
COMMENT ON COLUMN aemet.meses.evap IS 'Evaporación total en décimas de mm';
COMMENT ON COLUMN aemet.meses.glo IS 'Radiación global en decenas de Kj*m-2';
COMMENT ON COLUMN aemet.meses.hr IS 'Humedad relativa media en %';
COMMENT ON COLUMN aemet.meses.inso IS 'Media de la insolación diaria en horas';
COMMENT ON COLUMN aemet.meses.n_cub IS 'Nº de días cubiertos';
COMMENT ON COLUMN aemet.meses.n_des IS 'Nº de días despejados';
COMMENT ON COLUMN aemet.meses.n_fog IS 'Nº de días de niebla ';
COMMENT ON COLUMN aemet.meses.n_gra IS 'Nº de días de granizo';
COMMENT ON COLUMN aemet.meses.n_llu IS 'Nº de días de lluvia';
COMMENT ON COLUMN aemet.meses.n_nie IS 'Nº de días de nieve';
COMMENT ON COLUMN aemet.meses.n_nub IS 'Nº de días nubosos';
COMMENT ON COLUMN aemet.meses.n_tor IS 'Nº de días de tormenta';
COMMENT ON COLUMN aemet.meses.np_001 IS 'Nº de días de precipitación >= 0,1 mm';
COMMENT ON COLUMN aemet.meses.np_010 IS 'Nº de días de precipitación >= 1mm';
COMMENT ON COLUMN aemet.meses.np_100 IS 'Nº de días de precipitación >= 100mm';
COMMENT ON COLUMN aemet.meses.np_300 IS 'Nº de días de precipitación >= 30mm';
COMMENT ON COLUMN aemet.meses.nt_00 IS 'Nº de días de temperatura mínima <= 0°';
COMMENT ON COLUMN aemet.meses.nt_30 IS 'Nº de días de temperatura máxima >= 30°';
COMMENT ON COLUMN aemet.meses.nv_0050 IS 'Nº de días con visibilidad < 50m';
COMMENT ON COLUMN aemet.meses.nv_0100 IS 'Nº de días con visibilidad >=50m y <100m';
COMMENT ON COLUMN aemet.meses.nv_1000 IS 'Nº de días con visibilidad >=100, y <1km';
COMMENT ON COLUMN aemet.meses.nw_55 IS 'Nº de días de velocidad del viento >= 55 Km/h';
COMMENT ON COLUMN aemet.meses.nw_91 IS 'Nº de días de velocidad del viento >= 91 Km/h';
-- COMMENT ON COLUMN aemet.meses.p_max IS 'Precipitaciones (mm)máxima diaria';
COMMENT ON COLUMN aemet.meses.p_mes IS 'Precipitaciones (mm) total';
COMMENT ON COLUMN aemet.meses.p_sol IS 'Porcentaje medio mensual de la insolación diaria frente a la insolación teórica';
COMMENT ON COLUMN aemet.meses.q_mar IS 'Presión (hPa) media al nivel del mar';
-- COMMENT ON COLUMN aemet.meses.q_max IS 'Presión (hPa) máxima absoluta';
COMMENT ON COLUMN aemet.meses.q_med IS 'Presión (hPa) media al nivel de la estación';
-- COMMENT ON COLUMN aemet.meses.q_min IS 'Presión (hPa) mínima absoluta';
-- COMMENT ON COLUMN aemet.meses.ta_max IS 'Temperatura (grados Celsius) máxima absoluta';
-- COMMENT ON COLUMN aemet.meses.ta_min IS 'Temperatura (grados Celsius) mínima absoluta';
COMMENT ON COLUMN aemet.meses.ti_max IS 'Temperatura (grados Celsius) máxima más baja';
COMMENT ON COLUMN aemet.meses.tm_max IS 'Temperatura (grados Celsius) media de las máxima';
COMMENT ON COLUMN aemet.meses.tm_mes IS 'Temperatura (grados Celsius) media';
COMMENT ON COLUMN aemet.meses.tm_min IS 'Temperatura (grados Celsius) media de las mínima';
COMMENT ON COLUMN aemet.meses.ts_10 IS 'Temperatura (grados Celsius) media a 10 cm de profundidad';
COMMENT ON COLUMN aemet.meses.ts_20 IS 'Temperatura (grados Celsius) media a 20 cm de profundidad';
COMMENT ON COLUMN aemet.meses.ts_50 IS 'Temperatura (grados Celsius) media a 50 cm de profundidad';
COMMENT ON COLUMN aemet.meses.ts_min IS 'Temperatura (grados Celsius) mínima más alta';
COMMENT ON COLUMN aemet.meses.w_med IS 'Velocidad media en km/h elaborada a partir de las observaciones de 07, 13 y 18 UTC';
-- COMMENT ON COLUMN aemet.meses.w_racha IS 'Dirección en decenas de grado, velocidad en m/sg y fecha de la racha máxima';
COMMENT ON COLUMN aemet.meses.w_rec IS 'Recorrido medio diario (de 07 a 07 UTC) en Km';

CREATE TABLE aemet.prediccion (
  elaborado timestamp,
  fecha date,
  municipio varchar(5),
  prob_precipitacion int4 NULL,
  viento_velocidad int4 NULL,
  temperatura_maxima int4 NULL,
  temperatura_minima int4 NULL,
  humedad_relativa_maxima int4 NULL,
  humedad_relativa_minima int4 NULL,
  estado_cielo int4 NULL,
  sens_termica_maxima int4 NULL,
  sens_termica_minima int4 NULL,
  racha_max int4 NULL,
  uv_max int4 NULL,
  cota_nieve_prov int4 NULL,
  CONSTRAINT prediccion_pk PRIMARY KEY (elaborado, fecha, municipio)
);

COMMENT ON COLUMN aemet.prediccion.elaborado IS 'Fecha y hora de elaboración';
COMMENT ON COLUMN aemet.prediccion.fecha IS 'Período de validez de la predicción';
COMMENT ON COLUMN aemet.prediccion.municipio IS 'ID del municipio';
COMMENT ON COLUMN aemet.prediccion.prob_precipitacion IS 'Probabilidad de precipitación (%)';
COMMENT ON COLUMN aemet.prediccion.viento_velocidad IS 'Velocidad del viento (km/h)';
COMMENT ON COLUMN aemet.prediccion.temperatura_maxima IS 'Temperatura máxima (grados Celsius)';
COMMENT ON COLUMN aemet.prediccion.temperatura_minima IS 'Temperatura mínima (grados Celsius)';
COMMENT ON COLUMN aemet.prediccion.humedad_relativa_maxima IS 'Humedad relativa máxima (%)';
COMMENT ON COLUMN aemet.prediccion.humedad_relativa_minima IS 'Humedad relativa mínima (%)';
COMMENT ON COLUMN aemet.prediccion.estado_cielo IS 'Código del estado del Cielo';
COMMENT ON COLUMN aemet.prediccion.sens_termica_maxima IS 'Sensación térmica máxima (grados Celsius)';
COMMENT ON COLUMN aemet.prediccion.sens_termica_minima IS 'Sensación térmica mínima (grados Celsius)';
COMMENT ON COLUMN aemet.prediccion.racha_max IS 'Racha máxima (km/h)';
COMMENT ON COLUMN aemet.prediccion.uv_max IS 'Índice ultravioleta máximo';
COMMENT ON COLUMN aemet.prediccion.cota_nieve_prov IS 'Cota de nieve (metros)';

commit;
