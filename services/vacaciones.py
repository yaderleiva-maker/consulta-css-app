# services/vacaciones.py
"""
Servicio de Vacaciones e Incidencias
Cálculo de saldos, historial, gestión de vacaciones e incidencias.
"""

import pandas as pd
from services.bigquery import ejecutar_query
from datetime import date, datetime


# ============================================================
# FUNCIONES PARA VACACIONES
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
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
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
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
      AND i.estado = 'Aprobado'
      AND EXTRACT(YEAR FROM i.fecha_inicio) = EXTRACT(YEAR FROM CURRENT_DATE())
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    dias_usados = df.iloc[0]['dias_usados'] if not df.empty else 0
    
    # Obtener la política de vacaciones
    query_politica = """
    SELECT 
      p.dias_por_anio
    FROM `nexo_people.empleados` e
    JOIN `nexo_people.politicas_vacaciones` p ON e.id_empresa = p.id_empresa
      AND p.estado = 'ACTIVO'
    WHERE e.id_empleado = @id_empleado
    LIMIT 1
    """
    
    df_politica = ejecutar_query(query_politica, params)
    
    if not df_politica.empty:
        dias_por_anio = df_politica.iloc[0]['dias_por_anio']
    else:
        dias_por_anio = 15
    
    saldo_actual = dias_por_anio - dias_usados
    
    # Próximas vacaciones
    query_proximas = """
    SELECT 
      fecha_inicio,
      fecha_fin
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
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
# FUNCIONES PARA INCIDENCIAS
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

# services/vacaciones.py

def ejecutar_merge_calculo():
    """
    Ejecuta el MERGE para calcular todas las incidencias pendientes.
    """
    query = """
    MERGE `nexo_people.incidencias` T
    USING (
      WITH 
      incidencias_base AS (
        SELECT 
          i.id_incidencia,
          i.fecha_inicio,
          i.fecha_fin,
          i.dias_libres_sql,  -- 🔥 Usamos la columna normalizada
          i.id_pais,
          p.descuenta_festivos
        FROM `nexo_people.incidencias` i
        JOIN `nexo_people.politicas_vacaciones` p ON i.id_pais = p.id_pais
          AND p.estado = 'ACTIVO'
          AND p.fecha_inicio_vigencia <= i.fecha_inicio
          AND (p.fecha_fin_vigencia IS NULL OR p.fecha_fin_vigencia >= i.fecha_inicio)
        WHERE i.estado_calculo = 'PENDIENTE'
      ),
      
      festivos AS (
        SELECT f.fecha
        FROM `nexo_people.calendario_festivos` f
        WHERE f.id_pais IN (SELECT DISTINCT id_pais FROM incidencias_base)
      ),
      
      resultado_calculo AS (
        SELECT 
          ib.id_incidencia,
          COUNT(*) AS dias_calculados,
          COUNTIF(EXTRACT(DAY FROM fecha) BETWEEN 1 AND 15) AS quincena1,
          COUNTIF(EXTRACT(DAY FROM fecha) BETWEEN 16 AND 31) AS quincena2
        FROM incidencias_base ib
        CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(ib.fecha_inicio, ib.fecha_fin)) AS fecha
        WHERE 
          -- El día NO es un día libre del empleado (usando la columna normalizada)
          NOT (
            CASE EXTRACT(DAYOFWEEK FROM fecha)
              WHEN 1 THEN 'DOMINGO'
              WHEN 2 THEN 'LUNES'
              WHEN 3 THEN 'MARTES'
              WHEN 4 THEN 'MIÉRCOLES'
              WHEN 5 THEN 'JUEVES'
              WHEN 6 THEN 'VIERNES'
              WHEN 7 THEN 'SÁBADO'
            END IN UNNEST(SPLIT(ib.dias_libres_sql, ','))
          )
          -- El día NO es un festivo (si aplica según política)
          AND (
            ib.descuenta_festivos = FALSE
            OR fecha NOT IN (SELECT fecha FROM festivos)
          )
        GROUP BY ib.id_incidencia
      )
      
      SELECT 
        id_incidencia,
        dias_calculados,
        quincena1,
        quincena2
      FROM resultado_calculo
    ) S
    ON T.id_incidencia = S.id_incidencia
    WHEN MATCHED THEN
    UPDATE SET 
      T.dias_calculados = S.dias_calculados,
      T.dias_quincena1 = S.quincena1,
      T.dias_quincena2 = S.quincena2,
      T.estado_calculo = 'CALCULADO';
    """
    
    from services.bigquery import ejecutar_query
    df = ejecutar_query(query)
    
    # Si no hubo error, asumimos que funcionó
    return True
