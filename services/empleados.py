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


def obtener_estadisticas_rapidas():
    """
    Estadísticas rápidas para el dashboard.
    """
    query = """
    SELECT 
      COUNT(*) AS total_empleados,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'ACTIVO')) AS activos,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'INACTIVO')) AS inactivos,
      COUNTIF(e.id_estado_empleado = (SELECT id_estado_empleado FROM `nexo_people.catalogo_estados_empleado` WHERE nombre = 'VACACIONES')) AS vacaciones
    FROM `nexo_people.empleados` e
    """
    
    df = ejecutar_query(query)
    return df.iloc[0].to_dict() if not df.empty else {}
