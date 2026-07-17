# cargar_reportes.py

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime
import io

# Configuración
PROJECT_ID = "tu-proyecto-id"
DATASET_ID = "tu_dataset"
TABLE_ID = "inventario_actual"  # Nombre de la tabla

def actualizar_inventario_bigquery(df: pd.DataFrame) -> int:
    """
    Actualiza la tabla de inventario en BigQuery sobrescribiendo los datos existentes.
    
    Args:
        df: DataFrame con los datos del inventario
        
    Returns:
        Número de filas subidas
    """
    # Agregar columna de fecha_snapshot
    df['fecha_snapshot'] = datetime.now()
    
    # Inicializar cliente de BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    
    # Referencia a la tabla
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    # Configurar el job para sobrescribir la tabla
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # ¡Sobrescribir!
        autodetect=False,  # Usamos el esquema definido manualmente
    )
    
    # Subir los datos
    job = client.load_table_from_dataframe(
        df, 
        table_ref, 
        job_config=job_config
    )
    job.result()  # Esperar a que termine
    
    return len(df)

def leer_excel_inventario(archivo) -> pd.DataFrame:
    """
    Lee el archivo de Excel de inventario saltando las primeras 4 filas.
    """
    try:
        # Leer el archivo
        df = pd.read_excel(archivo, skiprows=4)
        
        # Limpieza básica: eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        
        # Eliminar filas vacías
        df = df.dropna(how='all')
        
        return df
    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")
        return None

def verificar_columnas_necesarias(df: pd.DataFrame) -> bool:
    """
    Verifica que el DataFrame tenga las columnas mínimas necesarias.
    """
    columnas_minimas = ['Id', 'InStock']
    for col in columnas_minimas:
        if col not in df.columns:
            st.error(f"❌ Falta la columna: '{col}' en el archivo.")
            return False
    return True

# --- Función principal para el módulo de Streamlit ---
def mostrar_actualizador_inventario():
    """
    Interfaz de Streamlit para actualizar el inventario en BigQuery.
    """
    st.subheader("📦 Actualizar Inventario en BigQuery")
    
    st.markdown("""
    **Instrucciones:**
    1. Descarga el archivo `Inventario17.xlsx` desde el sistema de la farmacia.
    2. Súbelo aquí para actualizar la tabla en BigQuery.
    3. La tabla se sobrescribirá completamente con los datos más recientes.
    """)
    
    archivo = st.file_uploader(
        "Selecciona el archivo de inventario (Inventario17.xlsx)", 
        type=['xlsx'],
        key="inventario_uploader"
    )
    
    if archivo is not None:
        # Mostrar información del archivo
        st.info(f"📄 Archivo: {archivo.name} - Tamaño: {archivo.size/1024:.2f} KB")
        
        # Leer el archivo
        with st.spinner("Leyendo archivo..."):
            df = leer_excel_inventario(archivo)
        
        if df is not None and not df.empty:
            # Verificar columnas
            if not verificar_columnas_necesarias(df):
                return
            
            # Mostrar vista previa
            st.subheader("📊 Vista previa de los datos")
            st.dataframe(df.head(10))
            
            # Mostrar estadísticas
            st.markdown(f"**Total de productos:** {len(df):,}")
            st.markdown(f"**Columnas:** {len(df.columns)}")
            
            # Calcular productos con inventario > 0
            productos_con_stock = len(df[df['InStock'] > 0])
            st.markdown(f"**Productos con stock:** {productos_con_stock:,}")
            
            # Botón de actualización
            if st.button("🚀 Actualizar Inventario en BigQuery", type="primary"):
                try:
                    with st.spinner("Subiendo datos a BigQuery..."):
                        filas_subidas = actualizar_inventario_bigquery(df)
                    
                    st.success(f"✅ ¡Inventario actualizado exitosamente!")
                    st.success(f"📊 Se subieron {filas_subidas:,} productos a la tabla `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`")
                    st.info(f"🕒 Snapshot creado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                except Exception as e:
                    st.error(f"❌ Error al actualizar BigQuery: {e}")
                    st.exception(e)
                    
        else:
            st.error("❌ No se pudieron leer los datos del archivo.")
    
    # Mostrar opción de consulta rápida
    with st.expander("🔍 Ver últimos datos en BigQuery", expanded=False):
        if st.button("Mostrar datos actuales en BigQuery"):
            try:
                client = bigquery.Client(project=PROJECT_ID)
                query = f"""
                    SELECT id, nombre, instock, fecha_snapshot
                    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                    WHERE status = 'ACTIVO'
                    ORDER BY nombre
                    LIMIT 100
                """
                df_result = client.query(query).to_dataframe()
                st.dataframe(df_result)
            except Exception as e:
                st.error(f"Error al consultar BigQuery: {e}")
