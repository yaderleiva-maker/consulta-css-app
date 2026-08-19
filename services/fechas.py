"""
Servicio de normalización de fechas
Maneja múltiples formatos de fecha del VICI
Ubicación: services/fechas.py
"""

import re
from datetime import datetime
import pandas as pd
import numpy as np


def normalizar_fecha_vici(valor) -> pd.Timestamp:
    """
    Normaliza fecha del VICI al formato esperado por VTIGER
    
    Formatos soportados:
    - dd-mm-yyyy hh:mm:ss (ej: 26-06-2026 14:20:47)
    - yyyy-mm-dd hh:mm:ss
    - dd/mm/yyyy hh:mm:ss
    - Timestamp de pandas
    """
    if pd.isna(valor):
        return pd.NaT
    
    # Si ya es datetime
    if isinstance(valor, (pd.Timestamp, datetime)):
        return pd.Timestamp(valor)
    
    valor_str = str(valor).strip()
    
    # Si es "0000-00-00 00:00:00" o similar
    if valor_str.startswith('0000-00-00') or valor_str.startswith('0-0-0'):
        return pd.NaT
    
    # Intentar diferentes formatos
    formatos = [
        ('%d-%m-%Y %H:%M:%S', r'\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'),
        ('%Y-%m-%d %H:%M:%S', r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'),
        ('%d/%m/%Y %H:%M:%S', r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'),
        ('%m/%d/%Y %H:%M:%S', r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'),
        ('%d-%m-%Y', r'\d{2}-\d{2}-\d{4}'),
        ('%Y-%m-%d', r'\d{4}-\d{2}-\d{2}'),
        ('%d/%m/%Y', r'\d{2}/\d{2}/\d{4}'),
        ('%m/%d/%Y', r'\d{2}/\d{2}/\d{4}'),
    ]
    
    for fmt, pattern in formatos:
        if re.match(pattern, valor_str):
            try:
                return pd.Timestamp(datetime.strptime(valor_str, fmt))
            except ValueError:
                continue
    
    # Último intento: dejar que pandas lo intente
    try:
        return pd.Timestamp(valor_str)
    except:
        return pd.NaT


def formatear_fecha_vtiger(fecha) -> str:
    """
    Formatea fecha para VTIGER (CSV)
    Formato esperado: MM/DD/YYYY HH:MM:SS
    """
    if pd.isna(fecha):
        return ''
    
    if isinstance(fecha, (pd.Timestamp, datetime)):
        return fecha.strftime('%m/%d/%Y %H:%M:%S')
    
    return str(fecha)


def normalizar_fecha_access(fecha_str: str) -> datetime:
    """
    Normaliza fechas provenientes de Access (formato #MM/DD/YYYY#)
    """
    if not fecha_str:
        return None
    
    # Limpiar formato Access
    fecha_limpia = fecha_str.strip().strip('#')
    
    try:
        return datetime.strptime(fecha_limpia, '%m/%d/%Y')
    except:
        try:
            return datetime.strptime(fecha_limpia, '%m/%d/%Y %H:%M:%S')
        except:
            return None
