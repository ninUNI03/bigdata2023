# bigdata2023
Este es el trabajo final de mi primer curso de BigData. trata sobre los datos de una red de tiendas.
Tenemos 3 archivos CSV donde se alojan datos de:
1. Los productos, su precio, descripción, categoría a la que pertenecen (archivo productos.csv cuyo link de enlace es https://drive.google.com/drive/folders/1g7XPxMY7x63u0vCqjyEKvP-O8ArpL7uA?usp=sharing)
2. Los empleados encargados de un conjunto de tiendas, aquí se incluyen datos sobre su comportamiento y eventos que se dan en el día a día como pueden ser quejas y reclamos entre ellos o hacia sus jefes (archivo comportamiento.csv)
3. Las tiendas, los productos que manejan, su stock y quien es su responsable (archivo TiendaResp3.csv)

Los scripts de lectura son wbronze.py y wsilver.py. El primer script es para guardar los datos tal cual llegan, sin procesar y podemos hacer que la ingesta sea controlada mediante:
`#!/bin/bash`
`watch` -n 3600 spark-submit /home/user/wbronze.py`

En este caso 3600 se refiere al intervalo de tiempo en segundos que se va a  ejecutar wbronze.py con PySpark, es decir cada 3600 segundos o una hora.
El script wsilver.py limpia los datos para guardarlos en la carpeta silver y también genera el modelo estrella.
