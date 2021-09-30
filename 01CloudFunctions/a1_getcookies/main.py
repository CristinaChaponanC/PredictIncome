import requests
import urllib.parse as urlparse
from datetime import datetime,timedelta
import pickle
from google.cloud import storage

def upload_blob(bucket_name, pickle, destination_blob_name,source_file_name):
    """Cargar archivos al bucket de GCP.
    input:
        bucket_name : "your-bucket-name" El ID del GCS bucket
        source_file_name : "local/path/to/file" Directorio del archivo a cargar
        destination_blob_name : "storage-object-name" El ID del objeto en GCS
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name + source_file_name)
    blob.upload_from_string(pickle, source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name + source_file_name))
  

def get_cookies(headers):
    '''
    Obtener la configuración de los cookies de la conexión
    
    input:
        url (str): link de la página web del que se quiere obtener los cookies
        headers(str): parámetros de configuración para las distintos navegadores
    output:
        r.cookies(RequestsCookieJar) : cookies de la conexión
    '''
    import requests
    url="https://www.transparencia.gob.pe/pte_transparencia_inicio.aspx"
    r = requests.get(url,headers=headers)
    assert r.status_code == 200, "No se pudo realizar conexion, status_code {}".format(r.status_code)
    #return r.status_code
    return r.cookies


def save_cookies(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    headers   = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}
    cookies   = get_cookies(headers)
    cookies_pickle = pickle.dumps(cookies)
    upload_blob("income-bucket", cookies_pickle, "02cookies/", "cookies.pickle")
    print('Cookies guardado')