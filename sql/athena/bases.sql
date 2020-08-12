select
  indicativo id,
  provincia,
  nombre,
  indsinop,
  latitud,
  longitud,
  altitud
from
  bases
where
  indicativo is not null
