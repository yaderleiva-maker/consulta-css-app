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


# services/vacaciones.py

# services/vacaciones.py

# services/vacaciones.py

def obtener_saldo_vacaciones(id_empleado):
    """
    Calcular el saldo de vacaciones de un empleado en tiempo real.
    """
    query = """
    WITH empleado_data AS (
      SELECT 
        e.id_empleado,
        e.fecha_ingreso_empresa,
        DATE_DIFF(CURRENT_DATE(), e.fecha_ingreso_empresa, MONTH) AS meses_trabajados,
        e.id_empresa
      FROM `nexo_people.empleados` e
      WHERE e.id_empleado = @id_empleado
    ),
    politica AS (
      SELECT 
        p.dias_por_anio,
        ROUND(p.dias_por_anio / 12, 2) AS dias_por_mes
      FROM `nexo_people.politicas_vacaciones` p
      JOIN empleado_data e ON p.id_empresa = e.id_empresa
      WHERE p.estado = 'ACTIVO'
        AND p.fecha_inicio_vigencia <= CURRENT_DATE()
        AND (p.fecha_fin_vigencia IS NULL OR p.fecha_fin_vigencia >= CURRENT_DATE())
      LIMIT 1
    ),
    vacaciones_usadas AS (
      SELECT 
        COALESCE(SUM(i.dias_calculados), 0) AS total_usados
      FROM `nexo_people.incidencias` i
      WHERE i.id_empleado = @id_empleado
        AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
        AND i.estado = 'Aprobado'
    ),
    proximas_vacaciones AS (
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
    )
    SELECT 
      e.meses_trabajados,
      p.dias_por_mes,
      p.dias_por_anio,
      ROUND(e.meses_trabajados * p.dias_por_mes, 2) AS dias_ganados,
      v.total_usados,
      ROUND((e.meses_trabajados * p.dias_por_mes) - v.total_usados, 2) AS saldo_actual,
      pv.fecha_inicio AS proxima_fecha_inicio,
      pv.fecha_fin AS proxima_fecha_fin
    FROM empleado_data e
    CROSS JOIN politica p
    CROSS JOIN vacaciones_usadas v
    LEFT JOIN proximas_vacaciones pv ON 1=1
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    if df.empty:
        return {
            "dias_ganados": 0,
            "dias_usados": 0,
            "saldo_actual": 0,
            "meses_trabajados": 0,
            "dias_por_mes": 0,
            "proximas_vacaciones": "No hay próximas vacaciones"
        }
    
    row = df.iloc[0]
    
    # 🔥 CORRECCIÓN: Verificar si la fecha es NaT antes de formatear
    proximas_vacaciones = "No hay próximas vacaciones"
    if row.get('proxima_fecha_inicio') and row.get('proxima_fecha_fin'):
        fecha_inicio = row['proxima_fecha_inicio']
        fecha_fin = row['proxima_fecha_fin']
        
        # Verificar que no sean NaT (Not a Time)
        import pandas as pd
        if not pd.isna(fecha_inicio) and not pd.isna(fecha_fin):
            if hasattr(fecha_inicio, 'strftime'):
                fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
                fecha_fin = fecha_fin.strftime('%Y-%m-%d')
            proximas_vacaciones = f"{fecha_inicio} al {fecha_fin}"
    
    return {
        "dias_ganados": float(row.get('dias_ganados', 0)),
        "dias_usados": float(row.get('total_usados', 0)),
        "saldo_actual": float(row.get('saldo_actual', 0)),
        "meses_trabajados": int(row.get('meses_trabajados', 0)),
        "dias_por_mes": float(row.get('dias_por_mes', 1.25)),
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

# services/vacaciones.py

def obtener_reporte_vacaciones(fecha_inicio=None, fecha_fin=None, id_empleado=None):
    """
    Obtener reporte de vacaciones con filtros opcionales por período y empleado.
    """
    query = """
    WITH ultimo_cargo AS (
      SELECT 
        h.id_empleado, 
        h.id_cargo,
        ROW_NUMBER() OVER (PARTITION BY h.id_empleado ORDER BY h.fecha_inicio DESC) AS rn
      FROM `nexo_people.historial_laboral` h
    )
    SELECT 
      CONCAT(e.nombres, ' ', e.apellidos) AS NOMBRE,
      e.cedula AS CC,
      c.nombre AS CARGO,
      i.fecha_inicio AS `FECHA INICIO`,
      i.fecha_fin AS `FECHA FIN`,
      i.dias_calculados AS `DIA HABIL`,
      (DATE_DIFF(i.fecha_fin, i.fecha_inicio, DAY) + 1) - i.dias_calculados AS `DIA NO HABIL`,
      i.dias_libres_sql AS `DIA DE DESCANSO`
    FROM `nexo_people.incidencias` i
    JOIN `nexo_people.empleados` e ON i.id_empleado = e.id_empleado
    LEFT JOIN ultimo_cargo h ON e.id_empleado = h.id_empleado AND h.rn = 1
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    WHERE i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
      AND i.estado = 'Aprobado'
    """
    
    params = []
    
    if id_empleado:
        query += " AND i.id_empleado = @id_empleado"
        params.append({"name": "id_empleado", "type": "STRING", "value": id_empleado})
    
    if fecha_inicio:
        query += " AND i.fecha_inicio >= @fecha_inicio"
        params.append({"name": "fecha_inicio", "type": "DATE", "value": fecha_inicio})
    
    if fecha_fin:
        query += " AND i.fecha_fin <= @fecha_fin"
        params.append({"name": "fecha_fin", "type": "DATE", "value": fecha_fin})
    
    query += " ORDER BY i.fecha_inicio DESC"
    
    df = ejecutar_query(query, params)
    return df


def generar_excel_reporte_vacaciones(df, nombre_archivo="reporte_vacaciones"):
    """
    Generar archivo Excel a partir del DataFrame del reporte.
    """
    from io import BytesIO
    
    if df.empty:
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Vacaciones', index=False)
        
        # Ajustar ancho de columnas
        workbook = writer.book
        worksheet = writer.sheets['Vacaciones']
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
    
    return output.getvalue()


def contar_incidencias_pendientes():
    """
    Contar incidencias pendientes de aprobación.
    """
    query = """
    SELECT 
      COUNT(*) AS total_pendientes,
      COUNT(DISTINCT id_empleado) AS empleados_afectados
    FROM `nexo_people.incidencias`
    WHERE estado = 'Pendiente'
    """
    
    df = ejecutar_query(query)
    
    if df.empty:
        return {"total_pendientes": 0, "empleados_afectados": 0}
    
    return {
        "total_pendientes": int(df.iloc[0]['total_pendientes']),
        "empleados_afectados": int(df.iloc[0]['empleados_afectados'])
    }
