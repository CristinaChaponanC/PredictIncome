import requests
import urllib.parse as urlparse
from datetime import datetime,timedelta
import pickle
from google.cloud import storage
import pandas as pd
import numpy as np


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

    parquet_loader("gs://income-bucket/01bd/02person/padron_sunat.parquet.gzip")

    return print('Done ' + str(periodo))