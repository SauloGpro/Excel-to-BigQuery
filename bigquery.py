import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configuración de BigQuery
PROJECT_ID = "bigqueryproject-449010"  # Reemplaza con el ID de tu proyecto en Google Cloud
DATASET_ID = "dataset_prueba"  # Nombre del dataset que se creará automáticamente
TABLE_ID = "tabla_prueba"      # Nombre de la tabla que se creará automáticamente

# Ruta al archivo Excel
RUTA_EXCEL = r"C:\Users\saulg\Desktop\datos_prueba.xlsx"  # Cambia si es necesario

def crear_dataset_si_no_existe(client, dataset_id):
    """
    Crea un dataset en BigQuery si no existe.
    """
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"El dataset '{dataset_id}' ya existe.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # Cambia la región si es necesario
        client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' creado.")

def crear_tabla_si_no_existe(client, dataset_id, table_id, dataframe):
    """
    Crea una tabla en BigQuery si no existe, basada en el esquema del DataFrame.
    """
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        print(f"La tabla '{table_id}' ya existe.")
    except NotFound:
        # Crear esquema basado en el DataFrame
        schema = []
        for col, dtype in dataframe.dtypes.items():
            if dtype == "int64":
                field_type = "INTEGER"
            elif dtype == "float64":
                field_type = "FLOAT"
            elif dtype == "datetime64[ns]":
                field_type = "TIMESTAMP"
            else:
                field_type = "STRING"
            schema.append(bigquery.SchemaField(col, field_type))
        
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Tabla '{table_id}' creada con éxito.")

def cargar_a_bigquery(dataframe, project_id, dataset_id, table_id):
    """
    Carga un DataFrame de pandas a una tabla de BigQuery.
    """
    client = bigquery.Client()

    # Crear dataset si no existe
    crear_dataset_si_no_existe(client, dataset_id)

    # Crear tabla si no existe
    crear_tabla_si_no_existe(client, dataset_id, table_id, dataframe)

    # Cargar datos
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job = client.load_table_from_dataframe(
        dataframe,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")  # Agregar sin borrar datos existentes
    )
    job.result()  # Esperar a que termine
    print(f"Datos cargados exitosamente a la tabla '{table_id}' en BigQuery.")

def main():
    """
    Función principal para leer un Excel y cargarlo en BigQuery.
    """
    # Leer datos del Excel
    try:
        df = pd.read_excel(RUTA_EXCEL, engine="openpyxl")
        print(f"Archivo Excel leído con éxito. Total de filas: {len(df)}")
    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")
        return

    # Verificar que el Excel no esté vacío
    if df.empty:
        print("El archivo Excel está vacío. No hay datos para cargar.")
        return

    # Cargar datos a BigQuery
    cargar_a_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_ID)

if __name__ == "__main__":
    main()

