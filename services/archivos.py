# services/archivos.py
"""
Servicio de Archivos
Manejo de archivos desde Google Drive usando la API.
Recibe una ruta relativa y devuelve el archivo (imagen, PDF, etc.)
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

# ID de la carpeta raíz de NexoPeople en Drive
# Puedes obtenerlo desde la URL: https://drive.google.com/drive/folders/XXXXXXXXX
DRIVE_ROOT_FOLDER_ID = "1valY4tU6X--x9-9gBlJPzJg7n1gW5NFO"  # 👈 ¡REEMPLAZA ESTO!

# ============================================================
# CLIENTE DE DRIVE (cacheado)
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
        
        # Construir cliente de Drive
        drive_service = build('drive', 'v3', credentials=credentials)
        return drive_service
    
    except Exception as e:
        st.error(f"❌ Error conectando a Google Drive: {e}")
        return None


# ============================================================
# FUNCIONES PRINCIPALES
# ============================================================

def buscar_archivo_por_ruta(ruta_relativa):
    """
    Buscar un archivo en Google Drive por su ruta relativa.
    
    Args:
        ruta_relativa (str): Ruta como 'NexoPeople/Empleados/EMPL86413e4e/EMPL86413e4e.foto_url.213759.jpg'
    
    Returns:
        dict: Información del archivo (id, nombre, mimeType) o None si no se encuentra.
    """
    drive_service = get_drive_client()
    if drive_service is None:
        return None
    
    # Obtener el nombre del archivo de la ruta
    nombre_archivo = ruta_relativa.split('/')[-1]
    
    # Buscar el archivo en Drive por nombre y dentro de la carpeta raíz
    query = f"name = '{nombre_archivo}' and '{DRIVE_ROOT_FOLDER_ID}' in parents and trashed = false"
    
    try:
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType, parents)"
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            st.warning(f"⚠️ Archivo no encontrado: {nombre_archivo}")
            return None
        
        # Por seguridad, tomar el primero (debería ser único)
        return files[0]
    
    except Exception as e:
        st.error(f"❌ Error buscando archivo en Drive: {e}")
        return None


def descargar_archivo(file_id):
    """
    Descargar el contenido de un archivo de Drive.
    
    Args:
        file_id (str): ID del archivo en Google Drive.
    
    Returns:
        bytes: Contenido del archivo o None si falla.
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
    Obtener una imagen (para st.image) desde una ruta de Drive.
    
    Args:
        ruta_relativa (str): Ruta relativa del archivo en Drive.
    
    Returns:
        bytes: Datos de la imagen para usar con st.image().
    """
    # Buscar el archivo por ruta
    file_info = buscar_archivo_por_ruta(ruta_relativa)
    if not file_info:
        return None
    
    # Descargar el archivo
    file_content = descargar_archivo(file_info['id'])
    return file_content


def obtener_url_archivo_drive(file_id):
    """
    Obtener URL pública de un archivo de Drive (si es necesario).
    """
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return None


# ============================================================
# FUNCIONES ESPECÍFICAS PARA FOTOS
# ============================================================

def mostrar_foto_desde_ruta(ruta_relativa, width=150):
    """
    Mostrar una foto en Streamlit a partir de su ruta relativa en Drive.
    
    Args:
        ruta_relativa (str): Ruta como 'NexoPeople/Empleados/EMPL.../...jpg'
        width (int): Ancho de la imagen en píxeles.
    """
    if not ruta_relativa:
        return None
    
    # Intentar obtener la imagen
    imagen_data = obtener_imagen_desde_ruta(ruta_relativa)
    
    if imagen_data:
        st.image(imagen_data, width=width)
        return True
    else:
        # Si falla, mostrar avatar con las iniciales
        # Extraer nombre de empleado de la ruta o usar placeholder
        nombre_placeholder = "Usuario"
        from services.fotos import obtener_url_avatar
        st.image(obtener_url_avatar(nombre_placeholder), width=width)
        return False
