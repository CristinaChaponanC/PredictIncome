import requests
import urllib.parse as urlparse
from datetime import datetime,timedelta
import pickle
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

def parquet_loader(uri):
    from google.cloud import bigquery
    client = bigquery.Client()
    table_ref = client.dataset("income_db").table("tb_income")
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition =     bigquery.WriteDisposition.WRITE_APPEND

    
    job_config.source_format = bigquery.SourceFormat.PARQUET
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    return load_job.result()


def save_BigQuery(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    periodo = request.args.get('periodo')

    parquet_loader("gs://income-bucket/01bd/01income/ingreso" + str(periodo) + ".parquet.gzip")

    return print('Done ' + str(periodo))