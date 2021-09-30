import requests
import urllib.parse as urlparse
from datetime import datetime,timedelta
import pickle
from google.cloud import storage
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np


class EntidadEstado:
    def __init__(self,id,nombre,grupo_entidad,poder_estado,id_poder_estado):
        self.id               = id
        self.nombre           = nombre
        self.grupo_entidad    = grupo_entidad
        self.poder_estado     = poder_estado
        self.id_poder_estado  = id_poder_estado


def get_entidades(headers,cookies):
    '''
    Obtener el listado de las entidades públicas disponibles para obtener
    sus datos de identificación.
    
    input:
        url(str): link de la página web que contiene el listado de las entidad públicas
        headers(str): parámetros de configuración para las distintos navegadores
        cookies(RequestsCookieJar): parámetros de la conexión almacenada en los cookies
    output:
        data_entidades (list): listado de clases que incluye la información básica de cada entidad pública
    '''
    url="https://www.transparencia.gob.pe/buscador/pte_transparencia_listado_entidades_poder.aspx"
    data_entidades = []
    entidades = set()
    for id_poder in [1,2,3,4,5,7]:
        r=requests.get(url,headers=headers,cookies=cookies,data={'Tipo_Pod':id_poder})
        assert r.status_code == 200, "No se pudo realizar conexión"
        html = bs(r.text, 'html.parser')
        item_page = html.find("div",attrs={'class':'bloque-cont'})
        poder_estado = html.find("h2").text.strip()
        for block in item_page.find_all("div",attrs={'class':'row'}):
            grupo_entidad = block.find("h4").text.strip()
            for item in block.find_all("li"):
                entidad = EntidadEstado(urlparse.parse_qs(urlparse.urlparse(item.find("a")["href"]).query)['id_entidad'][0],
                                        item.text.strip(),
                                        grupo_entidad,
                                        poder_estado,
                                        id_poder)
                if entidad.id in entidades:
                    continue
                entidades.add(entidad.id)
                data_entidades.append(entidad)
    return data_entidades


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
  

def save_entidades(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    headers   = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}
    pickle_in = download_blob('income-bucket','02cookies/cookies.pickle')
    cookies = pickle.loads(pickle_in)
    entidades = get_entidades(headers,cookies)

    upload_blob("income-bucket", pickle.dumps(entidades), "01db/04entidades/", "entidades")
    return str(entidades[0].nombre)