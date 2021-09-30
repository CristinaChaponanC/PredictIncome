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


def get_sunat():
    '''
    Descargar y cargar a memoria el padrón reducido de la sunat
    input:
        url (str): link de la sunat
    output:
        df (df): dataframe del padrón reducido de empresas
    '''
    
    #del df0 creamos nombre completo el cual concatena los nombres y apellidos
    import pandas as pd

    df = pd.read_csv('http://www2.sunat.gob.pe/padron_reducido_ruc.zip',sep='|',error_bad_lines=False,encoding ='latin1')
    df['UBIGEO'] = df['UBIGEO'].astype(str)
    return df

def save_sunat(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    df = get_sunat()

    upload_blob("income-bucket", df_padron_personas, "01bd/02person/", "padron_sunat.parquet.gzip")
    
    return print('Done SUNAT ' + str(periodo))