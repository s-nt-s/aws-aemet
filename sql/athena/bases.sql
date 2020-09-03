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
  indicativo is not null and not(
    provincia is null and
    nombre is null and
    indsinop is null and
    latitud is null and
    longitud is null and
    altitud is null
  )
