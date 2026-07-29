# services/empleados.py
"""
Servicio de Empleados
Todas las consultas relacionadas con la tabla empleados y sus relaciones básicas.
"""

from services.bigquery import ejecutar_query, leer_sql
import streamlit as st

# ============================================================
# CONSULTAS PRINCIPALES
# ============================================================

def obtener_empleado(id_empleado):
    """
    Obtener datos completos de un empleado por su ID.
    Incluye: empresa, proyecto, cargo, departamento, supervisor.
    EXCLUYE: salarios, información bancaria (datos sensibles).
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
      e.id_sexo,
      e.id_estado_civil,
      e.id_provincia,
      e.id_distrito,
      e.id_corregimiento,
      e.direccion,
      e.foto,
      e.fecha_ingreso_empresa,
      e.id_estado_empleado AS estado,
      e.observaciones,
      e.usuario_app,
      
      -- Datos de la empresa
      emp.nombre AS empresa,
      emp.id_pais AS id_pais_empresa,
      
      -- Datos del proyecto actual (desde historial)
      p.nombre AS proyecto,
      
      -- Datos del cargo actual (desde historial)
      c.nombre AS cargo,
      
      -- Datos del departamento actual (desde historial)
      d.nombre AS departamento,
      
      -- Datos del supervisor
      sup.nombres AS supervisor_nombre,
      sup.apellidos AS supervisor_apellidos
      
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


def buscar_empleados(termino, estado=None):
    """
    Buscar empleados por nombre o cédula (para el buscador en tiempo real).
    """
    if not termino or len(termino) < 2:
        return []
    
    query = """
    SELECT 
      id_empleado,
      CONCAT(nombres, ' ', apellidos) AS nombre_completo,
      cedula,
      id_estado_empleado AS estado,
      foto
    FROM `nexo_people.empleados`
    WHERE 
      (LOWER(CONCAT(nombres, ' ', apellidos)) LIKE LOWER(@termino)
       OR LOWER(cedula) LIKE LOWER(@termino))
    """
    
    if estado:
        query += f" AND id_estado_empleado = '{estado}'"
    
    query += " ORDER BY nombre_completo LIMIT 20"
    
    params = [{"name": "termino", "type": "STRING", "value": f"%{termino}%"}]
    df = ejecutar_query(query, params)
    
    return df.to_dict('records')


def obtener_empleados_activos():
    """Obtener lista de empleados activos."""
    return obtener_lista_empleados(estado="ACTIVO")


def obtener_empleados_por_supervisor(id_supervisor):
    """Obtener empleados que reportan a un supervisor específico."""
    query = """
    SELECT 
      e.id_empleado,
      CONCAT(e.nombres, ' ', e.apellidos) AS nombre_completo,
      e.cedula,
      e.id_estado_empleado AS estado,
      e.foto,
      c.nombre AS cargo
    FROM `nexo_people.empleados` e
    LEFT JOIN `nexo_people.catalogo_cargos` c ON e.id_cargo = c.id_cargo
    WHERE e.id_supervisor = @id_supervisor
    ORDER BY e.nombres
    """
    
    params = [{"name": "id_supervisor", "type": "STRING", "value": id_supervisor}]
    df = ejecutar_query(query, params)
    return df.to_dict('records')


def obtener_estadisticas_rapidas():
    """
    Estadísticas rápidas para el dashboard.
    """
    query = """
    SELECT 
      COUNT(*) AS total_empleados,
      COUNTIF(id_estado_empleado = 'ACTIVO') AS activos,
      COUNTIF(id_estado_empleado = 'INACTIVO') AS inactivos,
      COUNTIF(id_estado_empleado = 'VACACIONES') AS vacaciones,
      COUNTIF(id_estado_empleado = 'LICENCIA') AS licencia
    FROM `nexo_people.empleados`
    """
    
    df = ejecutar_query(query)
    return df.iloc[0].to_dict() if not df.empty else {}
