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

#Se construyen funciones para preprocesar los nombres de las personas y razon social de las empresas

def strip_accents(text):
    '''
    Elimina las tildes
    input:
        text (str): texto que se quiere transformar
    output:
        text (str): texto transformado
    '''
    import re
    import unicodedata
    try:
        text = unicode(text, 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

def apply_textblob(var):
    '''
    Elimina caractísticas adicionales
    input:
        text (str): texto que se quiere transformar
    output:
        text (str): texto transformado
    '''
    from textblob import TextBlob
    comment_blob = TextBlob(var)
    return ' '.join(comment_blob.words)

def num2word(text):
    '''
    Transforma números a su equivalente en palabras
    input:
        text (str): texto que se quiere transformar
    output:
        text (str): texto transformado
    '''
    import re
    import num2words
    return re.sub(r"(\d+)", lambda x: num2words.num2words(int(x.group(0))), str(text))

def clean_text(var):
    '''
    Aplica las funciones anteriores. Incluye transformar a minúsculas y eliminar los espacios en 
    blancos que están por demás. 
    input:
        text (str): texto que se quiere transformar
    output:
        text (str): texto transformado
    '''
    var = var.apply(num2word)
    var = var.apply(strip_accents)
    var = var.str.lower().str.strip()
    var = var.apply(apply_textblob)
    return var

def create_padron_personas(df0,df5):
    '''
    Proceso de limpieza y transformación de las fuentes que contengan información
    sobre los datos de personas naturales para construir el padrón de personas
    input:
        df0 (df): dataframe con la información de los funcionarios del estado
        df5 (df): dataframe con la información de los familiares del funcionario
    output:
        df_padron_personas (df): dataframe creado del padrón de personas
    '''
    
    #del df0 creamos nombre completo el cual concatena los nombres y apellidos
    df0['nombre_completo'] = df0['nombres'].str.lower()+' '+df0['paterno'].str.lower()+' ' +df0['materno'].str.lower()
    #creamos df5_nombre el cual incluye todos los nombres únicos
    df5_nombre = pd.DataFrame()
    df5_nombre['nombre_completo'] = df5['nombre'].unique()
    #creamos el padrón único de nombres
    df_padron_personas = pd.concat([df5_nombre,df0[['nombre_completo']].drop_duplicates()]).drop_duplicates()
    # Aplicamos la función de limpieza
    df_padron_personas['name_clean'] = clean_text(df_padron_personas['nombre_completo'])
    #reinicamos el índice
    df_padron_personas.reset_index(inplace=True,drop=True)
    # Creamos un dataframe temporal que tiene los nombre únicos 
    df_temp_name_clean = pd.DataFrame()
    df_temp_name_clean['name_clean'] = df_padron_personas['name_clean'].unique()
    
    # Creamos un dataframe temporal que tiene los nombre únicos 
    df_temp_name_clean = pd.DataFrame()
    df_temp_name_clean['name_clean'] = df_padron_personas['name_clean'].unique()

    # reiniciamos el índice que nos servirá para crear el id_persona
    df_temp_name_clean.reset_index(inplace=True)
    df_temp_name_clean.rename(columns={'index':'id_persona'},inplace=True)

    # rellenamos con ceros y apregamos la letra p para crear el id_persona
    df_temp_name_clean['id_persona'] = 'p'+df_temp_name_clean['id_persona'].astype('string').str.zfill(5)

    # al padrón personas le agregamos el id_persona
    df_padron_personas = pd.merge(left=df_temp_name_clean,right=df_padron_personas,on='name_clean',how='inner')

    return df_padron_personas

def create_padron(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    periodo = request.args.get('periodo')

    df0 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios.parquet.gzip')
    df1 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios_p1_detalle_empresas.parquet.gzip')
    df2 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios_p2_participacion_directorios.parquet.gzip')
    df3 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios_p3_vinculos_laborales.parquet.gzip')
    df4 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios_p4_vinculos_gremiales.parquet.gzip')
    df5 = pd.read_parquet('gs://income-bucket/01bd/03dji/dji_firmados_funcionarios_p5_vinculos_familiares.parquet.gzip')

    df_padron_personas = create_padron_personas(df0,df5)

    upload_blob("income-bucket", df_padron_personas, "01bd/02person/", "padron_personas.parquet.gzip")
    
    return print('Done DJI ' + str(periodo))