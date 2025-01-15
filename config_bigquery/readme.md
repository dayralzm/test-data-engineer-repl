Arquitectura
Este proyecto tiene como base tres servidores de SQL Server simulados:

Servidor Central (central):

Contiene la tabla CatLineasAereas con los códigos de las líneas aéreas.
Servidor Sucursal 1 (sucursal1):

Contiene las tablas Pasajeros y Vuelos de la sucursal 1.
Servidor Sucursal 2 (sucursal2):

Contiene las tablas Pasajeros y Vuelos de la sucursal 2.

Objetivos:
Replicar los datos de SQL Server a Google BigQuery.
Usar Apache Beam para orquestar el proceso de extracción, transformación y carga (ETL).
Automatizar la actualización de datos cada 20 minutos.
Usar CDC y SQL Replicate para mantener los datos sincronizados.

Requisitos
Python 3.x
Google Cloud Account y credenciales de acceso para BigQuery.
Google Cloud SDK instalado.
SQL Server local con las tablas y datos proporcionados.
Apache Beam para la orquestación del pipeline ETL.
pyodbc para la conexión con SQL Server.

1. SQL Server:
Habilitar la replicación de datos usando CDC (Cambio de Datos Capturados) en las tablas relevantes. Puedes hacerlo ejecutando los siguientes comandos en SQL Server:

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
Estos comandos habilitan CDC para las tablas Pasajeros, Vuelos y central_lineas_aereas en la base de datos test_deacero.

Cómo Ejecutar el Proyecto
Configura las credenciales de Google Cloud
Descarga el archivo JSON de las credenciales desde tu proyecto de Google Cloud.
Establece la variable de entorno para las credenciales 

Conexión con SQL Server:
Asegúrate de que tu servidor SQL Server esté corriendo y accesible desde el entorno donde ejecutes el script.
Reemplaza localhost y las credenciales de conexión en el script si es necesario.

Ejecuta el pipeline ETL:

El pipeline de Apache Beam se ejecuta de la siguiente manera:
python load_to_BQ.py


