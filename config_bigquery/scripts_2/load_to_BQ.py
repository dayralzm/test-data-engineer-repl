import pyodbc
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import os

# Establecer la variable de entorno de las credenciales de Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"credencial_test_path.json" 

# Función para conectarse a SQL Server y obtener los datos
def fetch_data_from_sql_server(query):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=localhost;'
                          'DATABASE=test_deacero;'
                          'UID=tobecompleted;'
                          'PWD=tobecompleted')  
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

# Función de transformación para Pasajeros
def transform_pasajeros(record):
    return {
        'ID_Pasajero': record[0],
        'Pasajero': record[1],
        'Edad': record[2]
    }

# Función de transformación para Vuelos
def transform_vuelos(record):
    return {
        'Sucursal': record[0],
        'Cve_LA': record[1],
        'Viaje': record[2],
        'Clase': record[3],
        'Precio': record[4],
        'Ruta': record[5],
        'Cve_Cliente': record[6]
    }

# Función de transformación para Lineas Aéreas (Central)
def transform_lineas_aereas(record):
    return {
        'Code': record[0],
        'Linea_Aerea': record[1]
    }

# Función para cargar los datos a BigQuery
def write_to_bigquery(pipeline, table_name, schema, data):
    client = bigquery.Client()
    dataset_id = 'deadataengineer.dea_test_dataset'
    table_id = f'{dataset_id}.{table_name}'
    
    # Prepara los datos para BigQuery
    errors = client.insert_rows_json(table_id, data)
    if errors:
        print(f"Error al cargar los datos: {errors}")
    else:
        print(f"Datos cargados a BigQuery correctamente: {table_name}")

# Función principal para ejecutar el pipeline
def run():
    # Consultas SQL para sucursal 1, sucursal 2 y datos centrales
    query_pasajeros_sucursal1 = """
        SELECT ID_Pasajero, Pasajero, Edad
        FROM sucursal1_pasajeros
    """
    query_vuelos_sucursal1 = """
        SELECT Sucursal, Cve_LA, Viaje, Clase, Precio, Ruta, Cve_Cliente
        FROM sucursal1_vuelos
    """
    query_pasajeros_sucursal2 = """
        SELECT ID_Pasajero, Pasajero, Edad
        FROM sucursal2_pasajeros
    """
    query_vuelos_sucursal2 = """
        SELECT Sucursal, Cve_LA, Viaje, Clase, Precio, Ruta, Cve_Cliente
        FROM sucursal2_vuelos
    """
    query_lineas_aereas = """
        SELECT Code, Linea_Aerea
        FROM central_lineas_aereas
    """

    # Conectar a BigQuery y preparar el pipeline
    options = PipelineOptions(
        flags=['--project=deadataengineer', '--runner=DirectRunner']
    )

    with beam.Pipeline(options=options) as p:
        # Paso 1: Leer los datos de SQL Server usando pyodbc
        pasajeros_sucursal1 = fetch_data_from_sql_server(query_pasajeros_sucursal1)
        vuelos_sucursal1 = fetch_data_from_sql_server(query_vuelos_sucursal1)
        pasajeros_sucursal2 = fetch_data_from_sql_server(query_pasajeros_sucursal2)
        vuelos_sucursal2 = fetch_data_from_sql_server(query_vuelos_sucursal2)
        lineas_aereas = fetch_data_from_sql_server(query_lineas_aereas)

        # Paso 2: Transformar los datos
        transformed_pasajeros_sucursal1 = [transform_pasajeros(row) for row in pasajeros_sucursal1]
        transformed_vuelos_sucursal1 = [transform_vuelos(row) for row in vuelos_sucursal1]
        transformed_pasajeros_sucursal2 = [transform_pasajeros(row) for row in pasajeros_sucursal2]
        transformed_vuelos_sucursal2 = [transform_vuelos(row) for row in vuelos_sucursal2]
        transformed_lineas_aereas = [transform_lineas_aereas(row) for row in lineas_aereas]

        # Paso 3: Cargar los datos a BigQuery
        write_to_bigquery(p, 'Pasajeros_Sucursal1', 'ID_Pasajero:INTEGER, Pasajero:STRING, Edad:INTEGER', transformed_pasajeros_sucursal1)
        write_to_bigquery(p, 'Vuelos_Sucursal1', 'Sucursal:STRING, Cve_LA:STRING, Viaje:DATE, Clase:STRING, Precio:INTEGER, Ruta:STRING, Cve_Cliente:INTEGER', transformed_vuelos_sucursal1)
        write_to_bigquery(p, 'Pasajeros_Sucursal2', 'ID_Pasajero:INTEGER, Pasajero:STRING, Edad:INTEGER', transformed_pasajeros_sucursal2)
        write_to_bigquery(p, 'Vuelos_Sucursal2', 'Sucursal:STRING, Cve_LA:STRING, Viaje:DATE, Clase:STRING, Precio:INTEGER, Ruta:STRING, Cve_Cliente:INTEGER', transformed_vuelos_sucursal2)
        write_to_bigquery(p, 'CatLineasAereas', 'Code:STRING, Linea_Aerea:STRING', transformed_lineas_aereas)

if __name__ == '__main__':
    run()
