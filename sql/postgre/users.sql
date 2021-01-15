CREATE USER readonly WITH PASSWORD 'readonly';
GRANT CONNECT ON DATABASE municipios TO readonly;
GRANT USAGE ON SCHEMA aemet TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA aemet TO readonly;
GRANT SELECT ON aemet.prov_semana_prediccion TO readonly;
GRANT SELECT ON aemet.prov_dias TO readonly;
GRANT SELECT ON aemet.prov_semanas TO readonly;
