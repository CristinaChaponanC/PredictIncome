import requests
import urllib.parse as urlparse
from datetime import datetime,timedelta
import pickle
from google.cloud import storage
from bs4 import BeautifulSoup as bs
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

class EntidadEstado:
    def __init__(self,id,nombre,grupo_entidad,poder_estado,id_poder_estado):
        self.id               = id
        self.nombre           = nombre
        self.grupo_entidad    = grupo_entidad
        self.poder_estado     = poder_estado
        self.id_poder_estado  = id_poder_estado


def process_entidad(periodo,entidad):
    '''
    Extraer datos de los ingresos públicos de cada uno de una entidad pública
    
    input:
        url (str): url de la página de transparencia del estado
        data (dict): parámetros de consulta para la extracción
            id_entidad: id de la entidad pública
            in_anno_consulta: año de la consulta
            ch_mes_consulta: mes de la consulta
            ch_tipo_descarga: tipo de descarga, "0" excel, "1" html
    '''
    url  = 'https://www.transparencia.gob.pe/personal/pte_transparencia_personal_genera.aspx'
    data = {"id_entidad":entidad.id,"in_anno_consulta":periodo[:4],"ch_mes_consulta":periodo[-2:],"ch_tipo_descarga":"1"}
    try:
        r    = requests.get(url,headers=headers,cookies=cookies,data=data)
        assert r.status_code == 200, "Error al consultar ({},{},{})".format(periodo,entidad.nombre,entidad.id)
        
        if not r.text: return None
        df   = pd.read_html(r.text,header=0)
        assert len(df) == 1, "Error al procesar ({},{},{})".format(periodo,entidad.nombre,entidad.id)

        df   = df[0].replace(np.nan, "").replace("", None)
        df.insert(loc=0, column="ENTIDAD", value=entidad.nombre)
        df.insert(loc=1, column="ENTIDAD_ID", value=entidad.id)
        df["VC_PERSONAL_OBSERVACIONES"] = df["VC_PERSONAL_OBSERVACIONES"].astype(str)
        return df if df.shape[0] > 0 else None
    except Exception as e:
        return (entidad,str(e))

def process_periodo(entidades,periodo):
    with ThreadPool(2) as p:
        f_entidad = partial(process_entidad, periodo)
        data_full = p.map(f_entidad, entidades)
        
    dfall   = pd.DataFrame()
    error   = []
    for data in data_full:
        if data is None: 
            continue
        elif type(data) == tuple: 
            error.append([periodo,data[0].id,data[0].nombre,data[1]]) 
            continue
        dfall = pd.concat([dfall,data])
        
    print("DONE ",periodo)
    
    return dfall


def download_blob(bucket_name,bucket_source_file_name):
    """Cargar archivos al bucket de GCP.
    input:
        bucket_name : "your-bucket-name" El ID del GCS bucket
        source_file_name : "local/path/to/file" Directorio del archivo a cargar
        destination_blob_name : "storage-object-name" El ID del objeto en GCS
    """
    from google.cloud import storage
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_source_file_name)
    pickle_in = blob.download_as_string()
    return pickle_in


def upload_blob(bucket_name, objeto, destination_blob_name,source_file_name):
    """Cargar archivos al bucket de GCP.
    input:
        bucket_name : "your-bucket-name" El ID del GCS bucket
        source_file_name : "local/path/to/file" Directorio del archivo a cargar
        destination_blob_name : "storage-object-name" El ID del objeto en GCS
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name + source_file_name)
    blob.upload_from_string(objeto, source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name + source_file_name))
  

def process_entidades(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    headers   = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}
    cookies = pickle.loads(download_blob('income-bucket','02cookies/cookies.pickle'))
    entidades = pickle.loads(download_blob('income-bucket','01db/04entidades/entidades'))
    
    periodo = request.args.get('periodo')

    df = process_periodo(entidades[80:84],add_months(periodo,-1))

    upload_blob("income-bucket", df.to_parquet(), "01db/01income/", "ingreso" + str(periodo))
    return print('Done ' + str(periodo))