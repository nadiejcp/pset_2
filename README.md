## 📌 Overview

Este proyecto implementa un flujo completo de ingeniería de datos, desde la ingesta hasta el modelado analítico, utilizando pipelines automatizados y un modelo dimensional tipo estrella.

El objetivo es procesar datos históricos de viajes (2015–2020), limpiarlos, transformarlos y prepararlos para análisis eficientes.

## ⚙️ Arquitectura

El flujo general del proyecto es:

### Ingesta de datos
### Fuente: URL externa
### Descarga de datos por lotes

### 📥 Pipeline 1: Ingesta de datos
Extracción desde la fuente
Limpieza básica (nombres de columnas)
Carga en base de datos (schema raw)

### 🔁 Pipeline 2: Transformación
Extracción desde raw
Transformaciones y validaciones
Carga en schema clean
Orquestación
Procesamiento mensual
Ejecución automática mediante triggers
Periodo: enero 2015 → diciembre 2020

## 🧹 Transformaciones aplicadas

Se implementaron las siguientes reglas de limpieza y validación:

Manejo de valores nulos
Eliminación o tratamiento de duplicados
Normalización de tipos de datos
Estandarización de nombres de columnas
Validación de fechas y timestamps
Filtrado de registros imposibles o erróneos
Consistencia entre pickup y dropoff
Validación de métricas:
Distancia
Monto total
Duración del viaje
🗓️ Procesamiento temporal
Los datos se procesan mensualmente
Al finalizar un mes, se activa automáticamente el siguiente
Rango de datos:
Inicio: Enero 2015
Fin: Diciembre 2020

## 🗄️ Modelo de datos

Se implementó un modelo dimensional tipo estrella:

Tabla de hechos: fact_trips
Dimensiones:
dim_date
dim_location
dim_vendor
dim_rate
dim_payment
Granularidad

Un registro representa un viaje individual.

Este modelo permite consultas analíticas eficientes y agregaciones rápidas.

## 🛠️ Tecnologías utilizadas
Mage (orquestación de pipelines)
PostgreSQL (almacenamiento)
SQL (transformaciones)
Python (procesamiento de datos)

## 📈 Objetivo

Construir un pipeline robusto y escalable que permita:

Garantizar calidad de datos
Automatizar procesos ETL
Facilitar análisis exploratorio y reporting

## Cómo ejecutar
1. Instalar docker desktop
2. Iniciar docker desktop
3. Ir a la carpeta del proyecto
4. Ejecutar el comando docker-compose up --build (en caso de que no se haya ejecutado antes)
5. Ejecutar el comando docker-compose up (en caso de que ya se haya ejecutado antes)
6. Ir a la dirección http://localhost:6789 para acceder a la interfaz de mage (Todos los logs y códigos pueden ser visualizados)
7. Configurar los secretos de postgresql en mage en caso de usar algunos diferentes a los puestos en el archivo docker-compose.yaml
8. Abrir el pipeline "ny_taxi_dataset" y ejecutar el archivo controll_flow.
9. Esto creará triggers para extraer y guardar los datos como en el flujo de pipeline 1, desde enero de 2015 hasta el mes de diciembre de 2020. (Tarda aproximadamente 10 minutos por mes)
10. Repetir el proceso para el pipeline "ny_taxi_clean" para tener datos corregidos y transformados. (Tarda aproximadamente 10 minutos por mes)
11. Una vez que se haya completado la ejecución de los pipelines, se puede acceder a la base de datos para verificar los resultados
12. Ejecutar el comando docker-compose down para detener los servicios


## Notas

Este proyecto está diseñado con enfoque en buenas prácticas de ingeniería de datos, incluyendo separación por capas (raw y clean), validaciones y automatización de procesos.
