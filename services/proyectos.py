# services/proyectos.py
"""
Servicio para manejar la configuración de proyectos.
"""

import streamlit as st
import pandas as pd
import io
from services.bigquery import ejecutar_query

@st.cache_data(ttl=300)
def obtener_columnas_proyecto(id_proyecto):
    """
    Obtiene la configuración de columnas para un proyecto.
    
    Args:
        id_proyecto (str): ID del proyecto
    
    Returns:
        pd.DataFrame: DataFrame con la configuración de columnas
    """
    query = f"""
        SELECT 
            columna_origen,
            columna_destino,
            nombre_visible,
            es_requerida,
            tipo_dato,
            orden,
            grupo,
            es_homologable,
            descripcion
        FROM `proyecto-css-panama.cobranza.configuracion_columnas_proyecto`
        WHERE id_proyecto = '{id_proyecto}'
          AND activo = TRUE
        ORDER BY orden ASC
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.error(f"Error al obtener columnas del proyecto: {e}")
        return pd.DataFrame()

def validar_columnas_proyecto(df, df_columnas):
    """
    Valida que las columnas requeridas existan en el DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a validar
        df_columnas (pd.DataFrame): Configuración de columnas
    
    Returns:
        list: Lista de columnas faltantes
    """
    if df_columnas.empty:
        return []
    
    columnas_requeridas = df_columnas[df_columnas['es_requerida'] == True]['columna_origen'].tolist()
    faltantes = [col for col in columnas_requeridas if col not in df.columns]
    return faltantes

def generar_plantilla_proyecto(df_columnas, proyecto_nombre):
    """
    Genera plantilla Excel con las columnas específicas del proyecto.
    
    Args:
        df_columnas (pd.DataFrame): Configuración de columnas
        proyecto_nombre (str): Nombre del proyecto para el archivo
    
    Returns:
        bytes: Contenido del archivo Excel
    """
    if df_columnas.empty:
        return None
    
    # Obtener columnas origen y nombres visibles
    columnas_origen = df_columnas['columna_origen'].tolist()
    nombres_visibles = df_columnas['nombre_visible'].tolist()
    
    # Crear DataFrame con columnas
    df_plantilla = pd.DataFrame(columns=columnas_origen)
    
    # Agregar fila de ejemplo
    ejemplo = {}
    for col in columnas_origen:
        if col in ['identificacion', 'Codigo del Cliente', 'Cédula']:
            ejemplo[col] = '8-123-456'
        elif col in ['nombre', 'Nombre del Cliente', 'Nombre Completo']:
            ejemplo[col] = 'JUAN PEREZ GONZALEZ'
        elif col in ['cuenta', 'Número de Cuenta']:
            ejemplo[col] = '001-123456-7'
        elif col in ['saldo', 'Saldo Total adeudado', 'Saldo']:
            ejemplo[col] = 1250.00
        elif col in ['cartera', 'Estado inicial']:
            ejemplo[col] = 'PREDEMANDA'
        elif col in ['telefono']:
            ejemplo[col] = '61234567, 67891234'
        elif col in ['correo']:
            ejemplo[col] = 'juan@gmail.com'
        elif col in ['fecha_ultimo_pago', 'Fecha ultimo pago']:
            ejemplo[col] = '2026-06-01'
        else:
            ejemplo[col] = ''
    
    # Agregar ejemplo al DataFrame
    df_plantilla = pd.DataFrame([ejemplo])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_plantilla.to_excel(writer, sheet_name='Carga', index=False)
        
        # Agregar instrucciones
        instrucciones = pd.DataFrame({
            'Instrucciones': [
                f'FORMATO DE CARGA - {proyecto_nombre}',
                '',
                '📌 COLUMNAS OBLIGATORIAS:',
                *[f'  • {nombre}' for nombre in df_columnas[df_columnas['es_requerida']]['nombre_visible'].tolist() if nombre],
                '',
                '📌 COLUMNAS OPCIONALES:',
                *[f'  • {nombre}' for nombre in df_columnas[~df_columnas['es_requerida']]['nombre_visible'].tolist() if nombre],
                '',
                '⚠️ REGLAS IMPORTANTES:',
                '  1. Los teléfonos y correos deben ir en UNA SOLA columna',
                '  2. Múltiples valores separados por coma (,)',
                '  3. Las fechas en formato YYYY-MM-DD',
                '  4. Los nombres en MAYÚSCULAS (opcional)',
                '  5. No modificar los nombres de las columnas',
                '  6. Valores "NO APLICA" se guardarán como NULL'
            ]
        })
        instrucciones.to_excel(writer, sheet_name='Instrucciones', index=False, header=False)
        
        # Ajustar columnas
        worksheet = writer.sheets['Carga']
        for i, col in enumerate(df_plantilla.columns):
            worksheet.set_column(i, i, 25)
    
    return output.getvalue()
