# services/archivos.py
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
# CLIENTE DE DRIVE (CON SCOPES CORRECTOS)
# ============================================================

@st.cache_resource
def get_drive_client():
    """
    Obtener cliente de Google Drive usando la Service Account con los scopes correctos.
    """
    try:
        # 👇 IMPORTANTE: Crear credenciales con scopes de Drive
        from google.oauth2 import service_account
        
        # Obtener las credenciales desde los secrets o variable de entorno
        import json
        import os
        
        # Intentar cargar desde secrets (Streamlit Cloud)
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            service_account_info = dict(st.secrets["gcp_service_account"])
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/drive.readonly']  # 👈 SCOPES CORRECTOS
            )
        else:
            # Fallback: usar variable de entorno
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                credentials = service_account.Credentials.from_service_account_file(
                    creds_path,
                    scopes=['https://www.googleapis.com/auth/drive.readonly']  # 👈 SCOPES CORRECTOS
                )
            else:
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
            fields="files(id, name, mimeType)",
            supportsAllDrives=True  # 👈 Para buscar en Drives compartidos
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
    """
    if not ruta_relativa:
        return None
    
    nombre_archivo = ruta_relativa.split('/')[-1]
    file_info = buscar_archivo_por_nombre(nombre_archivo)
    if not file_info:
        return None
    
    return descargar_archivo(file_info['id'])
