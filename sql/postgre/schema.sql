-- CREATE database municipios;

DROP TABLE IF EXISTS public.meses;
DROP TABLE IF EXISTS public.dias;
DROP TABLE IF EXISTS public.bases;

CREATE TABLE public.bases (
  id varchar NOT NULL,
  provincia varchar(2) NOT NULL,
  nombre varchar NULL,
  indsinop varchar NULL,
  latitud float8 NULL,
  longitud float8 NULL,
  altitud float8 NULL,
  CONSTRAINT bases_pk PRIMARY KEY (id)
);
COMMENT ON TABLE public.bases IS 'Bases de la AEMET';
COMMENT ON COLUMN public.bases.id IS 'indicativo';

-- DROP TABLE public.dias;

CREATE TABLE public.dias (
  base varchar NOT NULL,
  fecha date NOT NULL,
  dir int4 NULL,
  horapresmax time NULL,
  horapresmin time NULL,
  horaracha time NULL,
  horatmax time NULL,
  horatmin time NULL,
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
  CONSTRAINT dia_fk FOREIGN KEY (base) REFERENCES bases(id)
);

COMMENT ON TABLE public.dias IS 'Historico diario';
COMMENT ON COLUMN public.dias.dir IS 'Dirección de la racha máxima en decenas de grado';
COMMENT ON COLUMN public.dias.horapresmax IS 'Hora de la presión máxima (redondeada a la hora entera más próxima)';
COMMENT ON COLUMN public.dias.horapresmin IS 'Hora de la presión mínima (redondeada a la hora entera más próxima)';
COMMENT ON COLUMN public.dias.horaracha IS 'Hora y minuto de la racha máxima';
COMMENT ON COLUMN public.dias.horatmax IS 'hora de la tmin';
COMMENT ON COLUMN public.dias.horatmin IS 'hora de la tmax';
COMMENT ON COLUMN public.dias.prec IS 'Precipitación diaria de 07 a 07 en mm o 0.09 (‘Ip’) si es mejor que 0,1 mm';
COMMENT ON COLUMN public.dias.presmax IS 'Prexión máxima (hPa) al nivel de referencia de la estación';
COMMENT ON COLUMN public.dias.presmin IS 'Prexión mínima (hPa) al nivel de referencia de la estación';
COMMENT ON COLUMN public.dias.racha IS 'Racha máxima del viento en m/s';
COMMENT ON COLUMN public.dias.sol IS 'horas de sol';
COMMENT ON COLUMN public.dias.tmax IS 'Temperatura máxima (grados Celsius)';
COMMENT ON COLUMN public.dias.tmed IS 'Temperatura média (grados Celsius)';
COMMENT ON COLUMN public.dias.tmin IS 'Temperatura mínima (grados Celsius)';
COMMENT ON COLUMN public.dias.velmedia IS 'Velocidad media del viento en m/s';


CREATE TABLE public.meses (
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
  p_max varchar NULL,
  p_mes float4 NULL,
  p_sol int4 NULL,
  q_mar float4 NULL,
  q_max varchar NULL,
  q_med float4 NULL,
  q_min varchar NULL,
  ta_max varchar NULL,
  ta_min varchar NULL,
  ti_max float4 NULL,
  tm_max float4 NULL,
  tm_mes float4 NULL,
  tm_min float4 NULL,
  ts_10 float4 NULL,
  ts_20 float4 NULL,
  ts_50 float4 NULL,
  ts_min float4 NULL,
  w_med int4 NULL,
  w_racha varchar NULL,
  w_rec int4 NULL,
  CONSTRAINT mes_pk PRIMARY KEY (base, fecha),
  CONSTRAINT mes_fk FOREIGN KEY (base) REFERENCES bases(id)
);

COMMENT ON TABLE public.meses IS 'Historico mensual';
COMMENT ON COLUMN public.meses.fecha IS 'Solo importa el año y el mes';
COMMENT ON COLUMN public.meses.e IS 'Tensión de vapor media en décimas hPa';
COMMENT ON COLUMN public.meses.evap IS 'Evaporación total en décimas de mm';
COMMENT ON COLUMN public.meses.glo IS 'Radiación global en decenas de Kj*m-2';
COMMENT ON COLUMN public.meses.hr IS 'Humedad relativa media en %';
COMMENT ON COLUMN public.meses.inso IS 'Media de la insolación diaria en horas';
COMMENT ON COLUMN public.meses.n_cub IS 'Nº de días cubiertos';
COMMENT ON COLUMN public.meses.n_des IS 'Nº de días despejados';
COMMENT ON COLUMN public.meses.n_fog IS 'Nº de días de niebla ';
COMMENT ON COLUMN public.meses.n_gra IS 'Nº de días de granizo';
COMMENT ON COLUMN public.meses.n_llu IS 'Nº de días de lluvia';
COMMENT ON COLUMN public.meses.n_nie IS 'Nº de días de nieve';
COMMENT ON COLUMN public.meses.n_nub IS 'Nº de días nubosos';
COMMENT ON COLUMN public.meses.n_tor IS 'Nº de días de tormenta';
COMMENT ON COLUMN public.meses.np_001 IS 'Nº de días de precipitación >= 0,1 mm';
COMMENT ON COLUMN public.meses.np_010 IS 'Nº de días de precipitación >= 1mm';
COMMENT ON COLUMN public.meses.np_100 IS 'Nº de días de precipitación >= 100mm';
COMMENT ON COLUMN public.meses.np_300 IS 'Nº de días de precipitación >= 30mm';
COMMENT ON COLUMN public.meses.nt_00 IS 'Nº de días de temperatura mínima <= 0°';
COMMENT ON COLUMN public.meses.nt_30 IS 'Nº de días de temperatura máxima >= 30°';
COMMENT ON COLUMN public.meses.nv_0050 IS 'Nº de días con visibilidad < 50m';
COMMENT ON COLUMN public.meses.nv_0100 IS 'Nº de días con visibilidad >=50m y <100m';
COMMENT ON COLUMN public.meses.nv_1000 IS 'Nº de días con visibilidad >=100, y <1km';
COMMENT ON COLUMN public.meses.nw_55 IS 'Nº de días de velocidad del viento >= 55 Km/h';
COMMENT ON COLUMN public.meses.nw_91 IS 'Nº de días de velocidad del viento >= 91 Km/h';
COMMENT ON COLUMN public.meses.p_max IS 'Precipitaciones (mm)máxima diaria';
COMMENT ON COLUMN public.meses.p_mes IS 'Precipitaciones (mm) total';
COMMENT ON COLUMN public.meses.p_sol IS 'Porcentaje medio mensual de la insolación diaria frente a la insolación teórica';
COMMENT ON COLUMN public.meses.q_mar IS 'Presión (hPa) media al nivel del mar';
COMMENT ON COLUMN public.meses.q_max IS 'Presión (hPa) máxima absoluta';
COMMENT ON COLUMN public.meses.q_med IS 'Presión (hPa) media al nivel de la estación';
COMMENT ON COLUMN public.meses.q_min IS 'Presión (hPa) mínima absoluta';
COMMENT ON COLUMN public.meses.ta_max IS 'Temperatura (grados Celsius) máxima absoluta';
COMMENT ON COLUMN public.meses.ta_min IS 'Temperatura (grados Celsius) mínima absoluta';
COMMENT ON COLUMN public.meses.ti_max IS 'Temperatura (grados Celsius) máxima más baja';
COMMENT ON COLUMN public.meses.tm_max IS 'Temperatura (grados Celsius) media de las máxima';
COMMENT ON COLUMN public.meses.tm_mes IS 'Temperatura (grados Celsius) media';
COMMENT ON COLUMN public.meses.tm_min IS 'Temperatura (grados Celsius) media de las mínima';
COMMENT ON COLUMN public.meses.ts_10 IS 'Temperatura (grados Celsius) media a 10 cm de profundidad';
COMMENT ON COLUMN public.meses.ts_20 IS 'Temperatura (grados Celsius) media a 20 cm de profundidad';
COMMENT ON COLUMN public.meses.ts_50 IS 'Temperatura (grados Celsius) media a 50 cm de profundidad';
COMMENT ON COLUMN public.meses.ts_min IS 'Temperatura (grados Celsius) mínima más alta';
COMMENT ON COLUMN public.meses.w_med IS 'Velocidad media en km/h elaborada a partir de las observaciones de 07, 13 y 18 UTC';
COMMENT ON COLUMN public.meses.w_racha IS 'Dirección en decenas de grado, velocidad en m/sg y fecha de la racha máxima';
COMMENT ON COLUMN public.meses.w_rec IS 'Recorrido medio diario (de 07 a 07 UTC) en Km';
