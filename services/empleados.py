# services/empleados.py
"""
Servicio de Empleados
Todas las consultas relacionadas con la tabla empleados.
El cargo, proyecto, departamento y supervisor se obtienen del historial laboral actual usando JOINS.
"""

import pandas as pd
from services.bigquery import ejecutar_query


# ============================================================
# CONSULTAS PRINCIPALES
# ============================================================

def obtener_empleado(id_empleado):
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
      e.foto_url AS foto,
      e.fecha_ingreso_empresa,
      
      -- Estado con nombre en lugar de ID
      COALESCE(est.nombre, 'Desconocido') AS estado,  -- 👈 CAMBIADO
      
      emp.nombre AS empresa,
      c.nombre AS cargo,
      p.nombre AS proyecto,
      d.nombre AS departamento,
      CONCAT(sup.nombres, ' ', sup.apellidos) AS supervisor_nombre
      
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.empresas` emp ON e.id_empresa = emp.id_empresa
    LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado  -- 👈 JOIN con catálogo
    LEFT JOIN `nexo_people.historial_laboral` h ON e.id_empleado = h.id_empleado AND h.fecha_fin IS NULL
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    LEFT JOIN `nexo_people.proyectos` p ON h.id_proyecto = p.id_proyecto
    LEFT JOIN `nexo_people.catalogo_departamentos_empresa` d ON h.id_departamento = d.id_departamento
    LEFT JOIN `nexo_people.empleados` sup ON h.id_supervisor = sup.id_empleado
    WHERE e.id_empleado = @id_empleado
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    if df.empty:
        return None
    
    return df.iloc[0].to_dict()

def buscar_empleados(termino):
    if not termino or len(termino) < 2:
        return []
    
    query = """
    SELECT 
      e.id_empleado,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.cedula,
      COALESCE(est.nombre, 'Desconocido') AS estado,  -- 👈 Nombre del estado
      e.foto_url AS foto,
      COALESCE(c.nombre, 'Sin cargo') AS cargo
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado
    LEFT JOIN `nexo_people.historial_laboral` h ON e.id_empleado = h.id_empleado AND h.fecha_fin IS NULL
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    WHERE 
      LOWER(CONCAT(e.nombres, ' ', e.apellidos)) LIKE LOWER(@termino)
      OR LOWER(e.cedula) LIKE LOWER(@termino)
    ORDER BY nombre_completo
    LIMIT 20
    """
    
    params = [{"name": "termino", "type": "STRING", "value": f"%{termino}%"}]
    df = ejecutar_query(query, params)
    
    return df.to_dict('records')


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


def obtener_activos_inactivos():
    """
    Obtener lista de empleados activos e inactivos.
    El cargo se obtiene desde historial_laboral usando JOIN.
    """
    query = """
    SELECT 
      e.id_empleado,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.cedula,
      e.id_estado_empleado AS estado_id,
      e.fecha_terminacion,
      e.fecha_ingreso_empresa,
      
      -- Cargo actual desde historial_laboral
      COALESCE(c.nombre, 'Sin cargo') AS cargo_nombre,
      
      COALESCE(emp.nombre, 'Sin empresa') AS empresa_nombre,
      COALESCE(est.nombre, 'Desconocido') AS estado_nombre,
      
      CASE 
        WHEN e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo') THEN 0
        ELSE 1
      END AS orden_estado
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.historial_laboral` h ON e.id_empleado = h.id_empleado AND h.fecha_fin IS NULL
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    LEFT JOIN `nexo_people.empresas` emp ON e.id_empresa = emp.id_empresa
    LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado
    ORDER BY 
      orden_estado ASC,
      CASE 
        WHEN e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'Inactivo') 
        THEN e.fecha_terminacion 
        ELSE e.fecha_ingreso_empresa 
      END ASC
    """
    
    df = ejecutar_query(query)
    
    # Manejar fechas nulas
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
      
      -- Cargo actual desde historial_laboral
      COALESCE(c.nombre, 'Sin cargo') AS Cargo,
      
      COALESCE(est.nombre, 'Desconocido') AS Estado,
      COALESCE(ms.nombre, '') AS Motivo_Salida
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.historial_laboral` h ON e.id_empleado = h.id_empleado AND h.fecha_fin IS NULL
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado
    LEFT JOIN `nexo_people.catalogo_motivos_salida` ms ON e.id_motivo_salida = ms.id_motivo_salida
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
    
    # Rellenar NaN con string vacío
    df = df.fillna('')
    
    return df


def obtener_empleados_por_supervisor(id_supervisor):
    """
    Obtener empleados que reportan a un supervisor específico.
    """
    query = """
    SELECT 
      e.id_empleado,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.cedula,
      e.id_estado_empleado AS estado,
      e.foto_url AS foto,  -- 👈 CAMBIADO: e.foto → e.foto_url AS foto
      COALESCE(c.nombre, 'Sin cargo') AS cargo
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.historial_laboral` h ON e.id_empleado = h.id_empleado AND h.fecha_fin IS NULL
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    WHERE e.id_supervisor = @id_supervisor
    ORDER BY e.nombres
    """
    
    params = [{"name": "id_supervisor", "type": "STRING", "value": id_supervisor}]
    df = ejecutar_query(query, params)
    
    # Asegurar que la columna 'cargo' exista
    if 'cargo' not in df.columns:
        df['cargo'] = 'Sin cargo'
    
    return df.to_dict('records')

# services/empleados.py

def obtener_lista_empleados(estado=None):
    """
    Obtener lista de empleados (básica) para el buscador.
    Si se pasa estado, filtra por ese estado.
    """
    query = """
    SELECT 
      id_empleado,
      CONCAT(nombres, ' ', apellidos) AS nombre_completo,
      cedula,
      id_estado_empleado AS estado
    FROM `nexo_people.empleados`
    """
    
    if estado:
        query += f" WHERE id_estado_empleado = '{estado}'"
    
    query += " ORDER BY nombre_completo"
    
    df = ejecutar_query(query)
    return df.to_dict('records')
