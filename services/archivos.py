# services/archivos.py
"""
Servicio de Archivos
Manejo de archivos desde Google Drive usando la API.
"""
import pandas as pd
import streamlit as st
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

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
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            service_account_info = dict(st.secrets["gcp_service_account"])
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
        else:
            st.error("❌ No se encontraron credenciales en secrets.toml")
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
    Buscar un archivo en Google Drive por su nombre (sin importar la carpeta).
    """
    drive_service = get_drive_client()
    if drive_service is None:
        return None
    
    # 🔥 BUSCAR SOLO POR NOMBRE (sin filtrar por carpeta)
    query = f"name = '{nombre_archivo}' and trashed = false"
    
    try:
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            st.warning(f"⚠️ Archivo no encontrado: {nombre_archivo}")
            return None
        
        # Si hay múltiples archivos con el mismo nombre (poco probable), tomar el primero
        return files[0]
    
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
    
    # Buscar el archivo en Drive por nombre
    file_info = buscar_archivo_por_nombre(nombre_archivo)
    if not file_info:
        return None
    
    # Descargar el archivo
    return descargar_archivo(file_info['id'])


# ============================================================
# FUNCIONES DE PRUEBA (para mantener la compatibilidad)
# ============================================================

def listar_archivos():
    """
    FUNCIÓN TEMPORAL DE PRUEBA: Listar archivos en Drive.
    """
    drive_service = get_drive_client()
    if drive_service is None:
        st.error("❌ No se pudo conectar a Drive")
        return []
    
    try:
        results = drive_service.files().list(
            pageSize=20,
            fields="files(id, name, parents)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            st.warning("⚠️ No se encontraron archivos (puede ser problema de permisos)")
        else:
            st.success(f"✅ Encontrados {len(files)} archivos")
            for file in files:
                st.write(f"📄 {file['name']} (ID: {file['id']})")
        
        return files
    
    except Exception as e:
        st.error(f"❌ Error listando archivos: {e}")
        return []

def leer_excel(archivo):
    """
    Lee un archivo Excel o CSV y retorna un DataFrame de pandas.
    
    Args:
        archivo: Archivo subido desde Streamlit (BytesIO o similar)
    
    Returns:
        pd.DataFrame: Datos del archivo
    """
    try:
        if archivo.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(archivo)
        elif archivo.name.endswith('.csv'):
            return pd.read_csv(archivo)
        else:
            raise ValueError(f"Formato no soportado: {archivo.name}")
    except Exception as e:
        st.error(f"❌ Error al leer el archivo: {str(e)}")
        raise

def validar_columnas(df, columnas_requeridas):
    """
    Valida que existan las columnas requeridas en el DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a validar
        columnas_requeridas (list): Lista de columnas que deben existir
    
    Returns:
        list: Lista de columnas faltantes (vacía si todas existen)
    """
    faltantes = [col for col in columnas_requeridas if col not in df.columns]
    return faltantes

def normalizar_columnas(df, mapeo_columnas):
    """
    Renombra columnas del DataFrame según un mapeo.
    
    Args:
        df (pd.DataFrame): DataFrame original
        mapeo_columnas (dict): Diccionario {columna_original: columna_nueva}
    
    Returns:
        pd.DataFrame: DataFrame con columnas renombradas
    """
    if not mapeo_columnas:
        return df
    return df.rename(columns=mapeo_columnas)
