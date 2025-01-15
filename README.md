# Proyecto de Replicación de Datos a Google BigQuery

## Arquitectura

Este proyecto se basa en tres servidores de SQL Server simulados:

1. **Servidor Central (central)**:
   - Contiene la tabla `CatLineasAereas`, que incluye los códigos de las líneas aéreas.

2. **Servidor Sucursal 1 (sucursal1)**:
   - Contiene las tablas `Pasajeros` y `Vuelos` correspondientes a la sucursal 1.

3. **Servidor Sucursal 2 (sucursal2)**:
   - Contiene las tablas `Pasajeros` y `Vuelos` correspondientes a la sucursal 2.

## Objetivos

El objetivo principal de este proyecto es replicar datos de SQL Server a Google BigQuery. Para ello, se deben cumplir los siguientes puntos:

- Replicar los datos de SQL Server a Google BigQuery.
- Utilizar **Apache Beam** para orquestar el proceso de extracción, transformación y carga (ETL).
- Emplear **CDC** (Captura de Cambios de Datos) y **SQL Replicate** para mantener los datos sincronizados entre los sistemas.

## Requisitos

Para ejecutar este proyecto, se requieren los siguientes elementos:

- Python 3.x
- Cuenta de Google Cloud y credenciales de acceso para BigQuery.
- Google Cloud SDK instalado.
- SQL Server local con las tablas y datos proporcionados en este proyecto.
- **Apache Beam** para la orquestación del pipeline ETL.
- **pyodbc** para la conexión con SQL Server.

## Configuración de SQL Server

Es necesario habilitar la replicación de datos mediante **CDC** (Cambio de Datos Capturados) en las tablas relevantes. Para ello, se deben ejecutar los siguientes comandos en SQL Server:

```sql
-- Habilitar CDC para la base de datos
USE [test_deacero];
EXEC sys.sp_cdc_enable_db;

-- Habilitar CDC para las tablas de datos
EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'Pasajeros', 
    @role_name = NULL;

EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'Vuelos', 
    @role_name = NULL;

EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'central_lineas_aereas', 
    @role_name = NULL;
```
Estos comandos habilitan CDC para las tablas Pasajeros, Vuelos y central_lineas_aereas en la base de datos test_deacero.

## Cómo Ejecutar el Proyecto
1. Configuración de las Credenciales de Google Cloud
- Descarga el archivo JSON de las credenciales desde tu proyecto de Google Cloud.
- Establece la variable de entorno para las credenciales
export GOOGLE_APPLICATION_CREDENTIALS="/ruta/al/archivo-de-credenciales.json"
2. Conexión con SQL Server
- Asegúrate de que el servidor SQL Server esté activo y accesible desde el entorno en el que ejecutarás el script.
- Reemplaza localhost y ajusta las credenciales de conexión en el script de acuerdo con tu configuración.
3. Ejecución del Pipeline ETL
- Para ejecutar el pipeline de Apache Beam, utiliza el siguiente comando:
```
python load_to_BQ.py
```
