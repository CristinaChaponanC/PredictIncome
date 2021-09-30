import requests
from datetime import datetime,timedelta
from google.cloud import storage
import pandas as pd
import numpy as np

def add_months(periodo,diff):
    '''
    Agrega un número de meses a un periodo con el formato YYYYMM
    input:
        periodo(str): periodo en el formato YYYYMM
        diff(int): número de meses que se agregará o quitará al periodo
    return:
        fecha(str): periodo final al que se agregó o quitó un determinado número de meses
    '''
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    fecha = pd.to_datetime(periodo,format='%Y%m') +relativedelta(months=diff)
    
    return fecha.strftime("%Y%m")

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
    blob.upload_from_string(df, source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name + source_file_name))

import pandas as pd

def agregate_income():
    """Preprocesar los ingresos reales obtenidos del portal de transparencia.
    input:
        file (str) : nombre del archivo a procesar
    output:
        df_ingreso (pd.DataFrame) : dataframe con los ingresos reales agregados a nivel de persona y periodo
    """
    df_ingreso = pd.read_parquet('gs://income-bucket/01bd/01income/ingreso02.parquet.gzip')

    #create periodo
    df_ingreso['periodo'] = df_ingreso['anio']*100 + df_ingreso['mes']

    #aplica filtros de validación
    df_ingreso = df_ingreso[df_ingreso['MO_PERSONAL_TOTAL']<>0] #filtra los registros con valor 0
    df_ingreso = df_ingreso[df_ingreso['MO_PERSONAL_OTROS_BENEFICIOS']/df_ingreso['MO_PERSONAL_TOTAL']<=0.5]
    df_ingreso = df_ingreso[df_ingreso['MO_PERSONAL_REMUNERACIONES'] +
                            df_ingreso['MO_PERSONAL_HONORARIOS'] +
                            df_ingreso['MO_PERSONAL_INCENTIVO'] +
                            df_ingreso['MO_PERSONAL_GRATIFICACION'] +
                            df_ingreso['MO_PERSONAL_OTROS_BENEFICIOS'] == df_ingreso['MO_PERSONAL_TOTAL']]

    #agregar a nivel de persona y periodo
    df_ingreso = df_ingreso.groupby(['periodo','nrounico']).agg({'MO_PERSONAL_TOTAL':'max'})
    df_ingreso.rename(columns={'MO_PERSONAL_TOTAL_max':'sueldo'},inplace=True)
    return df_ingreso

def filter_income(df_ingreso,periodo_inicio):
    '''
    Filtra los ingresos de los últimos 6 meses
    input:
        df_ingreso (pd.DataFrame) : dataframe con los ingresos reales agregados a nivel de persona y periodo
        periodo_inicio (str) : periodo inicial en el formato YYYYMM
    output:
        df_ingreso (pd.DataFrame) : dataframe con los ingresos reales filtrados
    '''
    periodo_fin = add_months(periodo_inicio,periodo_inicio+6)
    df_ingreso = df_ingreso[(df_ingreso['periodo']>=periodo_inicio)&(df_ingreso['periodo']<=periodo_fin)]
    df_ingreso['periodo_ref'] = periodo_fin
    return df_ingreso

def estability_income(df_ingreso):
    '''
    Estabiliza los ingresos de los últimos 6 meses
    input:
        df_ingreso (pd.DataFrame) : dataframe con los ingresos reales de los últimos 6 meses
    output:
        df_ingreso (pd.DataFrame) : dataframe con los ingresos reales estabilizados. Se filtran 
        a las personas que tengan menos de 3 registros, y que su coeficiente de variación sea mayor a 0.4.
        El ingreso final es la mediana de todos los registros que quedan a nivel de personas.
    '''
    df_ingreso = df_ingreso.groupby(['coddoc','periodo_ref'],as_index=False).agg({'sueldo':['std','mean','median','count']})
    df_ingreso['existencia'] = df_ingreso['sueldo_count']
    df_ingreso['sueldo'] = df_ingreso['sueldo_median']
    df_ingreso['coef'] = df_ingreso['sueldo_std']/df_ingreso['sueldo_mean']
    
    df_ingreso = df_ingreso.query('existencia>=3').query('coef<=0.4')
    df_ingreso.rename(columns={'periodo_ref':'periodo'},inplace=True)
    return df_ingreso

def estabilizar_ingresos(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    periodo_inicio = request.args.get('periodo')

    df_ingreso = agregate_income()

    for i in range(3):
        periodo = add_months(periodo_inicio,i)
        periodo_fin = add_months(periodo,periodo+6)
        df_ingreso = filter_income(df_ingreso,periodo)
        df_ingreso = estability_income(df_ingreso)
        
        upload_blob("income-bucket", df_ingreso.to_parquet(), "01income/", "ingresos_" + periodo_fin + ".parquet.gzip")

    
    return print('Done estabilidad Ok ' + str(periodo_inicio) )