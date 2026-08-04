# services/vacaciones.py
"""
Servicio de Vacaciones
Cálculo de saldos, historial y gestión de vacaciones.
"""

import pandas as pd
from services.bigquery import ejecutar_query
from datetime import date, datetime


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
      AND i.fecha_inicio >= '2026-01-01'
      AND i.fecha_inicio <= '2026-12-31'
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    dias_usados = df.iloc[0]['dias_usados'] if not df.empty else 0
    
    # Calcular saldo: 15 días al año - días usados
    saldo_actual = 15 - dias_usados
    
    # Calcular próximas vacaciones (buscar la próxima incidencia con fecha futura)
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
        proximas_vacaciones = f"{df_proximas.iloc[0]['fecha_inicio']} al {df_proximas.iloc[0]['fecha_fin']}"
    else:
        proximas_vacaciones = "No hay próximas vacaciones"
    
    return {
        "saldo_actual": max(saldo_actual, 0),  # No puede ser negativo
        "dias_usados": dias_usados,
        "proximas_vacaciones": proximas_vacaciones
    }


def generar_excel_vacaciones_empleado(id_empleado):
    """
    Generar Excel con el historial de vacaciones de un empleado.
    """
    from io import BytesIO
    import pandas as pd
    
    historial = obtener_historial_vacaciones(id_empleado)
    if not historial:
        return None
    
    df = pd.DataFrame(historial)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Vacaciones', index=False)
    
    return output.getvalue()
