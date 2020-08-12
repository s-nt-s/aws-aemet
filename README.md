# Idea general

El objetivo es tener en nuestro AWS una base de datos con los
datos de la [AEMET](https://opendata.aemet.es/centrodedescargas/productosAEMET)
para abstraernos de algunos de los aspectos más peliagudos de la API original.

Por ejemplo, en muchos desarrollos vamos a querer trabajar a nivel de provincia,
pero la API de AEMET esta generalmente pensada para trabajar a nivel de base
(habiendo varias bases por provincia), por lo tanto si en nuestro sistema ya
tenemos estos datos agregados y almacenados podremos servirlos para nuestras
aplicaciones sin que ellas tengan que lidiar con la API AEMET, sus timeouts
y sus limites de peticiones por minutos.

# Requisitos

```console
$ apt install libpq-dev postgresql-client-common postgresql-client-11
$ pip install -r requirement.txt
```

# Flujo de trabajo

Para cada grupo de datos que queremos recopilar, el esquema general es:

1. Un script lee datos de la aemet, los guarda en `s3` y
lanza un trabajo el `Glue` para que los datos sean catalogados generando
una base de datos en `Athena`
2. Un script que lanza consultas en `Athena` y carga el resultado
(ya limpio y normalizado) en `RDS PostgreSQL`
3. Vistas materializadas en `RDS PostgreSQL` que contienen las agregaciones
que necesitamos que serán refrescadas periódicamente

Lo cual se orquesta con ejecuciones regulares para mantener los datos actualizados.

Los scripts están preparados para trabajar de manera incremental evitando cargar
datos de la AEMET ya cargados en ejecuciones anteriores, con la salvedad de que
los datos del año en curso (y del año anterior si estamos en marzo o antes)
siempre se solicitan a la AEMET machacando en `Athena` y `RDS PostgreSQL` lo que
ya hubiera. Esto se hace porque los datos del año en curso cambian durante todo
el año e incluso a veces los del año anterior tardan en estar consolidados.
