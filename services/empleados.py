# services/empleados.py
"""
Servicio de Empleados
Todas las consultas relacionadas con la tabla empleados.
"""

from services.bigquery import ejecutar_query
import streamlit as st
import pandas as pd

def obtener_empleado(id_empleado):
    """
    Obtener datos completos de un empleado por su ID.
    """
    query = """
    SELECT 
      e.id_empleado,
      e.cedula,
      e.nombres,
      e.apellidos,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.email_personal,
      e.email_corporativo,
      e.telefono,
      e.fecha_nacimiento,
      e.foto,
      e.fecha_ingreso_empresa,
      e.id_estado_empleado AS estado,
      emp.nombre AS empresa,
      p.nombre AS proyecto,
      c.nombre AS cargo,
      d.nombre AS departamento,
      sup.nombres AS supervisor_nombre
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.empresas` emp ON e.id_empresa = emp.id_empresa
    LEFT JOIN `nexo_people.proyectos` p ON e.id_proyecto = p.id_proyecto
    LEFT JOIN `nexo_people.catalogo_cargos` c ON e.id_cargo = c.id_cargo
    LEFT JOIN `nexo_people.catalogo_departamentos_empresa` d ON e.id_departamento = d.id_departamento
    LEFT JOIN `nexo_people.empleados` sup ON e.id_supervisor = sup.id_empleado
    WHERE e.id_empleado = @id_empleado
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    if df.empty:
        return None
    
    return df.iloc[0].to_dict()


def buscar_empleados(termino):
    """
    Buscar empleados por nombre o cédula.
    """
    if not termino or len(termino) < 2:
        return []
    
    query = """
    SELECT 
      id_empleado,
      CONCAT(nombres, ' ', apellidos) AS nombre_completo,
      cedula,
      id_estado_empleado AS estado,
      foto,
      (SELECT nombre FROM `nexo_people.catalogo_cargos` WHERE id_cargo = e.id_cargo) AS cargo
    FROM `nexo_people.empleados` e
    WHERE 
      LOWER(CONCAT(nombres, ' ', apellidos)) LIKE LOWER(@termino)
      OR LOWER(cedula) LIKE LOWER(@termino)
    ORDER BY nombre_completo
    LIMIT 20
    """
    
    params = [{"name": "termino", "type": "STRING", "value": f"%{termino}%"}]
    df = ejecutar_query(query, params)
    
    return df.to_dict('records')


# services/empleados.py

# services/empleados.py

def obtener_estadisticas_rapidas():
    """
    Estadísticas rápidas para el dashboard.
    """
    query = """
    SELECT 
      COUNT(*) AS total_empleados,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Activo')) AS activos,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo')) AS inactivos,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Vacaciones')) AS vacaciones
    FROM `nexo_people.empleados` e
    """
    
    df = ejecutar_query(query)
    return df.iloc[0].to_dict() if not df.empty else {}


# services/empleados.py


def obtener_activos_inactivos():
    """
    Obtener lista de empleados activos e inactivos.
    Maneja valores NULL correctamente.
    """
    query = """
    SELECT 
      e.id_empleado,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.cedula,
      e.id_estado_empleado AS estado_id,
      e.fecha_terminacion,
      e.fecha_ingreso_empresa,
      COALESCE((SELECT nombre FROM `nexo_people.catalogo_cargos` WHERE id_cargo = e.id_cargo), 'Sin cargo') AS cargo_nombre,
      COALESCE((SELECT nombre FROM `nexo_people.empresas` WHERE id_empresa = e.id_empresa), 'Sin empresa') AS empresa_nombre,
      COALESCE((SELECT nombre FROM `nexo_people.catalogo_estados_empleado` WHERE id_estado_empleado = e.id_estado_empleado), 'Desconocido') AS estado_nombre,
      CASE 
        WHEN e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo') THEN 0
        ELSE 1
      END AS orden_estado
    FROM `nexo_people.empleados` e
    ORDER BY 
      orden_estado ASC,
      CASE 
        WHEN e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo') 
        THEN e.fecha_terminacion 
        ELSE e.fecha_ingreso_empresa 
      END ASC
    """
    
    df = ejecutar_query(query)
    
    # Manejar fechas nulas: convertir None a ''
    if 'fecha_terminacion' in df.columns:
        df['fecha_terminacion'] = df['fecha_terminacion'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    
    if 'fecha_ingreso_empresa' in df.columns:
        df['fecha_ingreso_empresa'] = df['fecha_ingreso_empresa'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    
    return df.to_dict('records')


def generar_excel_activos_inactivos():
    """
    Generar un DataFrame con activos e inactivos para descargar como Excel.
    """
    query = """
    SELECT 
      CONCAT(e.nombres, ' ', e.apellidos) AS Nombre_Completo,
      e.cedula AS Cedula,
      e.fecha_ingreso_empresa AS Fecha_Ingreso,
      e.fecha_terminacion AS Fecha_Terminacion,
      COALESCE((SELECT nombre FROM `nexo_people.catalogo_estados_empleado` WHERE id_estado_empleado = e.id_estado_empleado), 'Desconocido') AS Estado,
      COALESCE((SELECT nombre FROM `nexo_people.catalogo_motivos_salida` WHERE id_motivo_salida = e.id_motivo_salida), '') AS Motivo_Salida
    FROM `nexo_people.empleados` e
    ORDER BY 
      CASE 
        WHEN e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo') THEN 0
        ELSE 1
      END ASC,
      e.fecha_terminacion ASC NULLS LAST
    """
    
    df = ejecutar_query(query)
    
    # Convertir fechas a string y manejar nulos
    for col in ['Fecha_Ingreso', 'Fecha_Terminacion']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    
    # Rellenar NaN con string vacío (seguridad extra)
    df = df.fillna('')
    
    return df
