# Idea general

El objetivo es tener en nuestro AWS una api adaptada a nuestras
necesidades que recupere datos de [AEMET](https://opendata.aemet.es/centrodedescargas/productosAEMET)
para abstraernos de algunos de los aspectos más peliagudos de la API original.

Por ejemplo, vamos a querer trabajar a nivel de provincia, pero la API
de AEMET esta generalmente pensada para trabajar a nivel de base (habiendo
varias bases por provincia), por lo tanto nos interesa que nuestra API-AWS
sea capar de aceptar como entrada una provincia y se encargue internamente
de agregar todos los datos de sus bases.
De esta manera, esta agregación sera transparente para las aplicaciones que
consuman nuestra API-AWS (en vez de la de la AEMET).
