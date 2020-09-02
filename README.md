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

# Piezas

1. AWS S3 para guardar los datos crudos recuperados de la AEMET (con poca o ninguna modificación)
2. AWS Glue para crear un catalogo de datos sobre los archivos almacenados en AWS S3
3. AWS Athena para hacer consultas sobre esos datos y normalizarlos
4. AWS RDS PostgreSQL para almacenar el resultado y crear las vistas con las agregaciones
5. AWS CodeBuild para ejecutar los scripts que hacen el trabajo
6. AWS CloudWatch Rules para programar la ejecución de los proyectos AWS CodeBuild

# Flujo de trabajo

**a)** Para cada grupo de datos que queremos recopilar
(histórico diario, histórico mensual, predicción diaria) tendremos un proyecto
CodeBuild que recupere los datos de AEMET y los guarde en S3 (buildspec/scrap.yml)
el cual programaremos su ejecución periódica con  CloudWatch
(histórico diario y predicción diaria todos los días, y histórico mensual cada mes).

**b)** Definiremos un Crawler de Glue que se ejecutara diariamente para
detectar datos nuevos en S3 y dejarlos disponibles en Athena

**c)** Finalmente habrá un proyecto CodeBuild que sera lanzado automaticamente
cada vez que Glue termine y que se encargará de pasar los datos de Athena
a RDS PostgreSQL (buildspec/update.yml)
