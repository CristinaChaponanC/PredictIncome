import requests
from datetime import datetime,timedelta
from google.cloud import storage
import pandas as pd
import numpy as np

def upload_blob(bucket_name, df, destination_blob_name,source_file_name):
    """Cargar archivos al bucket de GCP.
    input:
        bucket_name : "your-bucket-name" El ID del GCS bucket
        source_file_name : "local/path/to/file" Directorio del archivo a cargar
        destination_blob_name : "storage-object-name" El ID del objeto en GCS
    """
    import os
    from google.cloud import storage
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name + source_file_name)
    blob.upload_from_string(df.to_parquet(compression='gzip'), source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name + source_file_name))

def get_dji():
    '''
    Conectarse a las fuentes de datos abiertos para extraer los json disponibles de las
    Declaraciones Juradas de Intereses (DJI). Luego elimina las columas repetidas en cada
    fuente.
    
    input:
        url: Cada uno de las url para cada una de las tablas
    output:
        dataframes: 6 dataframes, uno para cada fuente disponible 
    '''
    import pandas as pd
    
    df0 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_funcionarios.json')
    df1 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_funcionarios_p1_detalle_empresas.json')
    df2 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_funcionarios_p2_participacion_directorios.json')
    df3 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_x_funcio_p3_vinculos_laborales.json')
    df4 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_x_funcio_p4_vinculos_gremiales.json')
    df5 = pd.read_json('https://www.datosabiertos.gob.pe/sites/default/files/dji_firmados_x_funcio_p5_vinculos_familiares.json')
    
    #Definimos las columnas para eliminar y que se repiten en cada uno de las tablas
    drop_column = ['paterno','materno','nombres','cargo','ruc_entidad','entidad','sector','tipo_dji','fecha_fir_dji','pregunta']
    
    #Procedemos a eliminar
    df1.drop(columns=drop_column,inplace=True)
    df2.drop(columns=drop_column,inplace=True)
    df3.drop(columns=drop_column,inplace=True)
    df4.drop(columns=drop_column,inplace=True)
    df5.drop(columns=drop_column,inplace=True)
    
    return df0,df1,df2,df3,df4,df5


def get_dji(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    periodo = request.args.get('periodo')

    df0,df1,df2,df3,df4,df5 = get_dji()

    #Guardamos los dataframe en el bucket de GCP
    upload_blob("income-bucket", df0, "01bd/03dji/", "dji_firmados_funcionarios.parquet.gzip")
    upload_blob("income-bucket", df1, "01bd/03dji/", "dji_firmados_funcionarios_p1_detalle_empresas.parquet.gzip")
    upload_blob("income-bucket", df2, "01bd/03dji/", "dji_firmados_funcionarios_p2_participacion_directorios.parquet.gzip")
    upload_blob("income-bucket", df3, "01bd/03dji/", "dji_firmados_funcionarios_p3_vinculos_laborales.parquet.gzip")
    upload_blob("income-bucket", df4, "01bd/03dji/", "dji_firmados_funcionarios_p4_vinculos_gremiales.parquet.gzip")
    upload_blob("income-bucket", df5, "01bd/03dji/", "dji_firmados_funcionarios_p5_vinculos_familiares.parquet.gzip")
  
    return print('Done DJI ' + str(periodo))