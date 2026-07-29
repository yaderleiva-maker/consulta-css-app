"""
Servicio de Archivos
Manejo de archivos desde Google Drive usando la API.
"""

import streamlit as st
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from services.bigquery import get_client as get_bigquery_client

# ============================================================
# CONFIGURACIÓN
# ============================================================

DRIVE_ROOT_FOLDER_ID = "1valY4tU6X--x9-9gBlJPzJg7n1gW5NFO"

# ============================================================
# CLIENTE DE DRIVE
# ============================================================

@st.cache_resource
def get_drive_client():
    """
    Obtener cliente de Google Drive usando la Service Account.
    """
    try:
        # Usar las mismas credenciales que BigQuery
        client = get_bigquery_client()
        credentials = client._credentials
        
        if credentials is None:
            st.error("❌ No se encontraron credenciales para Drive")
            return None
        
        drive_service = build('drive', 'v3', credentials=credentials)
        return drive_service
    
    except Exception as e:
        st.error(f"❌ Error conectando a Google Drive: {e}")
        return None


# ============================================================
# FUNCIONES PRINCIPALES
# ============================================================

def buscar_archivo_por_nombre(nombre_archivo):
    """
    Buscar un archivo en Google Drive por su nombre dentro de la carpeta raíz.
    """
    drive_service = get_drive_client()
    if drive_service is None:
        return None
    
    query = f"name = '{nombre_archivo}' and '{DRIVE_ROOT_FOLDER_ID}' in parents and trashed = false"
    
    try:
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType)"
        ).execute()
        
        files = results.get('files', [])
        return files[0] if files else None
    
    except Exception as e:
        st.error(f"❌ Error buscando archivo en Drive: {e}")
        return None


def descargar_archivo(file_id):
    """
    Descargar el contenido de un archivo de Drive.
    """
    drive_service = get_drive_client()
    if drive_service is None:
        return None
    
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_data = io.BytesIO()
        downloader = MediaIoBaseDownload(file_data, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        return file_data.getvalue()
    
    except Exception as e:
        st.error(f"❌ Error descargando archivo: {e}")
        return None


def obtener_imagen_desde_ruta(ruta_relativa):
    """
    Obtener imagen desde una ruta de AppSheet.
    
    Args:
        ruta_relativa (str): Ej. 'NexoPeople/Empleados/EMPL86413e4e/EMPL86413e4e.foto_url.213759.jpg'
    
    Returns:
        bytes: Datos de la imagen o None si falla.
    """
    if not ruta_relativa:
        return None
    
    # Extraer el nombre del archivo de la ruta
    nombre_archivo = ruta_relativa.split('/')[-1]
    
    # Buscar el archivo en Drive
    file_info = buscar_archivo_por_nombre(nombre_archivo)
    if not file_info:
        return None
    
    # Descargar el archivo
    return descargar_archivo(file_info['id'])
