# services/vacaciones.py
"""
Servicio de Vacaciones e Incidencias
Cálculo de saldos, historial, gestión de vacaciones e incidencias.
"""

import pandas as pd
from services.bigquery import ejecutar_query
from datetime import date, datetime


# ============================================================
# FUNCIONES EXISTENTES (Vacaciones)
# ============================================================

def obtener_historial_vacaciones(id_empleado):
    """
    Obtener el historial de vacaciones de un empleado.
    """
    query = """
    SELECT 
      i.id_incidencia,
      i.fecha_inicio,
      i.fecha_fin,
      i.dias_calculados,
      i.estado,
      i.observacion,
      i.fecha_creacion
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'Vacaciones')
    ORDER BY i.fecha_inicio DESC
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    return df.to_dict('records')


def obtener_saldo_vacaciones(id_empleado):
    """
    Calcular el saldo de vacaciones de un empleado.
    """
    query = """
    SELECT 
      COALESCE(SUM(dias_calculados), 0) AS dias_usados
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'Vacaciones')
      AND i.estado = 'Aprobado'
      AND EXTRACT(YEAR FROM i.fecha_inicio) = EXTRACT(YEAR FROM CURRENT_DATE())
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    dias_usados = df.iloc[0]['dias_usados'] if not df.empty else 0
    
    # Calcular saldo: 15 días al año - días usados
    query_politica = """
    SELECT 
      p.dias_por_anio
    FROM `nexo_people.empleados` e
    JOIN `nexo_people.politicas_vacaciones` p ON e.id_empresa = p.id_empresa
      AND p.estado = 'ACTIVO'
      AND EXTRACT(YEAR FROM CURRENT_DATE()) = EXTRACT(YEAR FROM p.fecha_inicio_vigencia)
    WHERE e.id_empleado = @id_empleado
    LIMIT 1
    """
    
    df_politica = ejecutar_query(query_politica, params)
    
    if not df_politica.empty:
        dias_por_anio = df_politica.iloc[0]['dias_por_anio']
    else:
        dias_por_anio = 15  # Valor por defecto
    
    saldo_actual = dias_por_anio - dias_usados
    
    # Calcular próximas vacaciones
    query_proximas = """
    SELECT 
      fecha_inicio,
      fecha_fin
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'Vacaciones')
      AND i.estado = 'Aprobado'
      AND i.fecha_inicio >= CURRENT_DATE()
    ORDER BY i.fecha_inicio ASC
    LIMIT 1
    """
    
    df_proximas = ejecutar_query(query_proximas, params)
    
    if not df_proximas.empty:
        fecha_inicio = df_proximas.iloc[0]['fecha_inicio']
        fecha_fin = df_proximas.iloc[0]['fecha_fin']
        if hasattr(fecha_inicio, 'strftime'):
            fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
            fecha_fin = fecha_fin.strftime('%Y-%m-%d')
        proximas_vacaciones = f"{fecha_inicio} al {fecha_fin}"
    else:
        proximas_vacaciones = "No hay próximas vacaciones"
    
    return {
        "saldo_actual": max(saldo_actual, 0),
        "dias_usados": dias_usados,
        "proximas_vacaciones": proximas_vacaciones
    }


def generar_excel_vacaciones_empleado(id_empleado):
    """
    Generar Excel con el historial de vacaciones de un empleado.
    """
    from io import BytesIO
    
    historial = obtener_historial_vacaciones(id_empleado)
    if not historial:
        return None
    
    df = pd.DataFrame(historial)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Vacaciones', index=False)
    
    return output.getvalue()


# ============================================================
# NUEVAS FUNCIONES PARA INCIDENCIAS
# ============================================================

def obtener_tipos_incidencia():
    """
    Obtener la lista de tipos de incidencia para los filtros.
    """
    query = """
    SELECT id_tipo_incidencia, nombre
    FROM `nexo_people.catalogo_tipos_incidencia`
    WHERE estado = 'ACTIVO'
    ORDER BY nombre
    """
    df = ejecutar_query(query)
    return df.to_dict('records')


def obtener_incidencias_empleado(id_empleado, tipo_filtro=None):
    """
    Obtener incidencias de un empleado con filtro opcional por tipo.
    """
    query = """
    SELECT 
      i.id_incidencia,
      i.fecha_inicio,
      i.fecha_fin,
      i.dias_calculados,
      i.estado,
      i.observacion,
      i.fecha_creacion,
      t.nombre AS tipo
    FROM `nexo_people.incidencias` i
    JOIN `nexo_people.catalogo_tipos_incidencia` t ON i.id_tipo_incidencia = t.id_tipo_incidencia
    WHERE i.id_empleado = @id_empleado
    """
    
    if tipo_filtro and tipo_filtro != 'Todas':
        query += " AND t.nombre = @tipo_filtro"
    
    query += " ORDER BY i.fecha_inicio DESC"
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    if tipo_filtro and tipo_filtro != 'Todas':
        params.append({"name": "tipo_filtro", "type": "STRING", "value": tipo_filtro})
    
    df = ejecutar_query(query, params)
    return df.to_dict('records')


def obtener_resumen_incidencias(id_empleado):
    """
    Obtener resumen de todas las incidencias de un empleado.
    """
    query = """
    SELECT 
      t.nombre AS tipo,
      COUNT(*) AS total,
      SUM(i.dias_calculados) AS total_dias,
      COUNTIF(i.estado = 'Aprobado') AS aprobadas,
      COUNTIF(i.estado = 'Pendiente') AS pendientes
    FROM `nexo_people.incidencias` i
    JOIN `nexo_people.catalogo_tipos_incidencia` t ON i.id_tipo_incidencia = t.id_tipo_incidencia
    WHERE i.id_empleado = @id_empleado
    GROUP BY t.nombre
    ORDER BY t.nombre
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    return df.to_dict('records')


def generar_excel_incidencias_empleado(id_empleado, tipo_filtro=None):
    """
    Generar Excel con el historial de incidencias de un empleado.
    """
    from io import BytesIO
    
    incidencias = obtener_incidencias_empleado(id_empleado, tipo_filtro)
    if not incidencias:
        return None
    
    df = pd.DataFrame(incidencias)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Incidencias', index=False)
    
    return output.getvalue()
