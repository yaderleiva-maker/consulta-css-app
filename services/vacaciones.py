# services/vacaciones.py
"""
Servicio de Vacaciones e Incidencias
Cálculo de saldos, historial, gestión de vacaciones e incidencias.
"""
#import streamlit as st
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
    Calcular el saldo de vacaciones de un empleado en tiempo real (VERSIÓN SIMPLIFICADA).
    """
    query = """
    SELECT 
      COALESCE(SUM(i.dias_calculados), 0) AS dias_usados
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
      AND i.estado = 'Aprobado'
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    
    dias_usados = df.iloc[0]['dias_usados'] if not df.empty else 0
    
    # Obtener meses trabajados
    query_meses = """
    SELECT 
      DATE_DIFF(CURRENT_DATE(), fecha_ingreso_empresa, MONTH) AS meses_trabajados
    FROM `nexo_people.empleados`
    WHERE id_empleado = @id_empleado
    """
    df_meses = ejecutar_query(query_meses, params)
    meses_trabajados = df_meses.iloc[0]['meses_trabajados'] if not df_meses.empty else 0
    
    # Obtener política
    query_politica = """
    SELECT 
      ROUND(dias_por_anio / 12, 2) AS dias_por_mes
    FROM `nexo_people.politicas_vacaciones` p
    JOIN `nexo_people.empleados` e ON p.id_empresa = e.id_empresa
    WHERE e.id_empleado = @id_empleado
      AND p.estado = 'ACTIVO'
    LIMIT 1
    """
    df_politica = ejecutar_query(query_politica, params)
    dias_por_mes = df_politica.iloc[0]['dias_por_mes'] if not df_politica.empty else 1.25
    
    dias_ganados = meses_trabajados * dias_por_mes
    saldo_actual = dias_ganados - dias_usados
    
    query_proximas = """
    SELECT 
      fecha_inicio,
      fecha_fin
    FROM `nexo_people.incidencias` i
    WHERE i.id_empleado = @id_empleado
      AND i.id_tipo_incidencia = (SELECT id_tipo_incidencia FROM `nexo_people.catalogo_tipos_incidencia` WHERE nombre = 'VACACIONES')
      AND i.estado = 'Aprobado'
      AND i.fecha_inicio > CURRENT_DATE()  -- 🔥 Cambiado de >= a >
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
        "dias_ganados": round(dias_ganados, 2),
        "dias_usados": round(dias_usados, 2),
        "saldo_actual": round(saldo_actual, 2),
        "meses_trabajados": meses_trabajados,
        "dias_por_mes": dias_por_mes,
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
    
    # 🔥 CONVERTIR FECHAS A STRING ANTES DE EXPORTAR
    for col in ['fecha_inicio', 'fecha_fin', 'fecha_creacion']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    
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
      i.fecha_inicio AS Inicio,
      i.fecha_fin AS Fin,
      i.dias_calculados AS Calculo,
      i.estado AS Estado,
      i.observacion,
      i.fecha_creacion,
      t.nombre AS tipo,
      i.dias_libres_sql AS Descansa
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
    Ejecuta el MERGE para calcular todas las incidencias pendientes (VERSIÓN SIMPLIFICADA).
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
      -- 🔥 El día NO es un festivo en el PAÍS de la incidencia
      AND (
        ib.descuenta_festivos = FALSE
        OR fecha NOT IN (
          SELECT f.fecha
          FROM `nexo_people.calendario_festivos` f
          WHERE f.id_pais = ib.id_pais  -- 🔥 Compara con el país de la incidencia
        )
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
    
    try:
        from services.bigquery import ejecutar_query
        ejecutar_query(query)
        return True
    except Exception as e:
        # Si hay error, mostrar el SQL para depurar
        #import streamlit as st
        st.error(f"❌ Error en el MERGE: {e}")
        st.code(query, language="sql")
        raise

def obtener_reporte_vacaciones(fecha_inicio=None, fecha_fin=None, id_empleado=None, quincena=None):
    """
    Obtener reporte de vacaciones dividido por quincenas.
    """
    # ============================================================
    # BASE DE LA CONSULTA
    # ============================================================
        # 🔥 DEPURACIÓN
    import streamlit as st
    st.write(f"🔍 SERVICE - Fecha Inicio: {fecha_inicio} (tipo: {type(fecha_inicio)})")
    st.write(f"🔍 SERVICE - Fecha Fin: {fecha_fin} (tipo: {type(fecha_fin)})")
    st.write(f"🔍 SERVICE - Quincena: {quincena}")
    st.write(f"🔍 SERVICE - ID Empleado: {id_empleado}")
    
    base_query = """
    WITH ultimo_cargo AS (
      SELECT 
        h.id_empleado, 
        h.id_cargo,
        ROW_NUMBER() OVER (PARTITION BY h.id_empleado ORDER BY h.fecha_inicio DESC) AS rn
      FROM `nexo_people.historial_laboral` h
    ),
    incidencias_base AS (
      SELECT 
        i.id_incidencia,
        i.id_empleado,
        i.fecha_inicio,
        i.fecha_fin,
        i.dias_calculados,
        i.dias_quincena1,
        i.dias_quincena2,
        i.dias_libres_sql,
        i.estado
      FROM `nexo_people.incidencias` i
      JOIN `nexo_people.catalogo_tipos_incidencia` t ON i.id_tipo_incidencia = t.id_tipo_incidencia
      WHERE t.nombre = 'VACACIONES'
        AND i.estado = 'Aprobado'
        AND i.dias_calculados IS NOT NULL
    """
    
    params = []
    
    # ============================================================
    # FILTRO DE FECHAS (TRASLAPE) - CORREGIDO
    # ============================================================
    if id_empleado:
        base_query += " AND i.id_empleado = @id_empleado"
        params.append({"name": "id_empleado", "type": "STRING", "value": id_empleado})
    
    if fecha_inicio and fecha_fin:
        base_query += """
            AND i.fecha_inicio <= @fecha_fin
            AND i.fecha_fin >= @fecha_inicio
        """
        params.append({"name": "fecha_inicio", "type": "DATE", "value": fecha_inicio.strftime("%Y-%m-%d")})
        params.append({"name": "fecha_fin", "type": "DATE", "value": fecha_fin.strftime("%Y-%m-%d")})
    
    base_query += """
    )
    """
    
    # ============================================================
    # QUINCENA 1
    # ============================================================
    q1_query = """
    SELECT 
      CONCAT(e.nombres, ' ', e.apellidos) AS NOMBRE,
      e.cedula AS CC,
      c.nombre AS CARGO,
      'Q1' AS QUINCENA,
      GREATEST(i.fecha_inicio, DATE_TRUNC(i.fecha_inicio, MONTH)) AS FECHA_INICIO,
      LEAST(i.fecha_fin, DATE_ADD(DATE_TRUNC(i.fecha_inicio, MONTH), INTERVAL 14 DAY)) AS FECHA_FIN,
      i.dias_quincena1 AS DIA_HABIL,
      (DATE_DIFF(
        LEAST(i.fecha_fin, DATE_ADD(DATE_TRUNC(i.fecha_inicio, MONTH), INTERVAL 14 DAY)),
        GREATEST(i.fecha_inicio, DATE_TRUNC(i.fecha_inicio, MONTH)),
        DAY
      ) + 1) - i.dias_quincena1 AS DIA_NO_HABIL,
      i.dias_libres_sql AS DIA_DESCANSO
    FROM incidencias_base i
    JOIN `nexo_people.empleados` e ON i.id_empleado = e.id_empleado
    LEFT JOIN ultimo_cargo h ON e.id_empleado = h.id_empleado AND h.rn = 1
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    WHERE i.dias_quincena1 > 0
    """
    
    # ============================================================
    # QUINCENA 2
    # ============================================================
    q2_query = """
    SELECT 
      CONCAT(e.nombres, ' ', e.apellidos) AS NOMBRE,
      e.cedula AS CC,
      c.nombre AS CARGO,
      'Q2' AS QUINCENA,
      GREATEST(i.fecha_inicio, DATE_ADD(DATE_TRUNC(i.fecha_inicio, MONTH), INTERVAL 15 DAY)) AS FECHA_INICIO,
      LEAST(i.fecha_fin, LAST_DAY(i.fecha_inicio)) AS FECHA_FIN,
      i.dias_quincena2 AS DIA_HABIL,
      (DATE_DIFF(
        LEAST(i.fecha_fin, LAST_DAY(i.fecha_inicio)),
        GREATEST(i.fecha_inicio, DATE_ADD(DATE_TRUNC(i.fecha_inicio, MONTH), INTERVAL 15 DAY)),
        DAY
      ) + 1) - i.dias_quincena2 AS DIA_NO_HABIL,
      i.dias_libres_sql AS DIA_DESCANSO
    FROM incidencias_base i
    JOIN `nexo_people.empleados` e ON i.id_empleado = e.id_empleado
    LEFT JOIN ultimo_cargo h ON e.id_empleado = h.id_empleado AND h.rn = 1
    LEFT JOIN `nexo_people.catalogo_cargos` c ON h.id_cargo = c.id_cargo
    WHERE i.dias_quincena2 > 0
    """
    
    # ============================================================
    # CONSTRUIR LA CONSULTA FINAL (ROBUSTA)
    # ============================================================
    # Normalizar el valor de quincena
    quincena_normalizada = quincena or "Ambas"
    
    opciones_quincena = {
        "Quincena 1 (1-15)": q1_query,
        "Q1": q1_query,
        "1": q1_query,
        "Quincena 2 (16-31)": q2_query,
        "Q2": q2_query,
        "2": q2_query,
        "Ambas": q1_query + " UNION ALL " + q2_query,
        "Todas": q1_query + " UNION ALL " + q2_query,
        "Ambas quincenas": q1_query + " UNION ALL " + q2_query,
    }
    
    if quincena_normalizada not in opciones_quincena:
        query = base_query + opciones_quincena["Ambas"]
    else:
        query = base_query + opciones_quincena[quincena_normalizada]
    
    query += " ORDER BY NOMBRE, QUINCENA"
    
    # ============================================================
    # EJECUTAR
    # ============================================================
        # 🔥 DEPURACIÓN: Mostrar la consulta SQL
    st.code(query, language="sql")
    
    df = ejecutar_query(query, params)
    
    df = ejecutar_query(query, params)
    
    # Renombrar columnas
    if not df.empty:
        df.columns = [
            "NOMBRE",
            "CC",
            "CARGO",
            "QUINCENA",
            "FECHA INICIO",
            "FECHA FIN",
            "DIA HABIL",
            "DIA NO HABIL",
            "DIA DE DESCANSO"
        ]
    
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

# services/vacaciones.py

def obtener_dashboard_vacaciones():
    """
    Obtener indicadores agregados para el dashboard de vacaciones.
    SIN subconsultas correlacionadas.
    """
    query = """
    WITH 
    -- 1. Empleados activos con sus datos
    empleados_activos AS (
      SELECT 
        e.id_empleado,
        e.nombres,
        e.apellidos,
        e.cedula,
        e.fecha_ingreso_empresa,
        e.id_empresa,
        e.id_departamento,
        e.id_supervisor,
        emp.nombre AS empresa_nombre,
        dep.nombre AS departamento_nombre,
        CONCAT(sup.nombres, ' ', sup.apellidos) AS supervisor_nombre,
        est.nombre AS estado_empleado
      FROM `nexo_people.empleados` e
      LEFT JOIN `nexo_people.empresas` emp ON e.id_empresa = emp.id_empresa
      LEFT JOIN `nexo_people.catalogo_departamentos_empresa` dep ON e.id_departamento = dep.id_departamento
      LEFT JOIN `nexo_people.empleados` sup ON e.id_supervisor = sup.id_empleado
      LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado
      WHERE est.nombre = 'Activo'
    ),
    
    -- 2. Políticas de vacaciones (días por mes)
    politicas AS (
      SELECT 
        id_empresa,
        ROUND(dias_por_anio / 12, 2) AS dias_por_mes
      FROM `nexo_people.politicas_vacaciones`
      WHERE estado = 'ACTIVO'
    ),
    
    -- 3. Vacaciones usadas por empleado
    vacaciones_usadas AS (
      SELECT 
        id_empleado,
        COALESCE(SUM(dias_calculados), 0) AS dias_usados
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      GROUP BY id_empleado
    ),
    
    -- 4. Última vacación por empleado
    ultima_vacacion AS (
      SELECT 
        id_empleado,
        MAX(fecha_fin) AS ultima_vacacion
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      GROUP BY id_empleado
    ),
    
    -- 5. Próxima vacación por empleado
    proxima_vacacion AS (
      SELECT 
        id_empleado,
        fecha_inicio AS proxima_vacacion,
        fecha_fin AS proxima_vacacion_fin,
        ROW_NUMBER() OVER (PARTITION BY id_empleado ORDER BY fecha_inicio ASC) AS rn
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      AND fecha_inicio >= CURRENT_DATE()
    )
    
    -- 6. Consulta principal
    SELECT 
      ea.id_empleado,
      ea.nombres,
      ea.apellidos,
      ea.cedula,
      ea.fecha_ingreso_empresa,
      ea.empresa_nombre,
      ea.departamento_nombre,
      ea.supervisor_nombre,
      ea.estado_empleado,
      
      -- Días ganados
      ROUND(
        DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25),
        2
      ) AS dias_ganados,
      
      -- Días usados
      COALESCE(vu.dias_usados, 0) AS dias_usados,
      
      -- Última vacación
      uv.ultima_vacacion,
      
      -- Próxima vacación (solo la primera)
      pv.proxima_vacacion,
      pv.proxima_vacacion_fin,
      
      -- Saldo
      ROUND(
        DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
        2
      ) AS saldo,
      
      -- Estado del saldo
      CASE 
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) < 0 THEN 'NEGATIVO'
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) < 5 THEN 'BAJO'
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) <= 15 THEN 'NORMAL'
        ELSE 'ACUMULADO'
      END AS estado_saldo,
      
      -- Antigüedad de la última vacación
      CASE 
        WHEN uv.ultima_vacacion IS NULL THEN 'NUNCA'
        WHEN DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, MONTH) >= 12 THEN 'MAS_12_MESES'
        WHEN DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, MONTH) >= 6 THEN 'MAS_6_MESES'
        ELSE 'RECIENTE'
      END AS antiguedad_vacacion,
      
      -- Días sin vacaciones
      DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, DAY) AS dias_sin_vacaciones
      
    FROM empleados_activos ea
    LEFT JOIN politicas p ON ea.id_empresa = p.id_empresa
    LEFT JOIN vacaciones_usadas vu ON ea.id_empleado = vu.id_empleado
    LEFT JOIN ultima_vacacion uv ON ea.id_empleado = uv.id_empleado
    LEFT JOIN proxima_vacacion pv ON ea.id_empleado = pv.id_empleado AND pv.rn = 1
    """
    
    from services.bigquery import ejecutar_query
    import pandas as pd
    from datetime import datetime
    
    df = ejecutar_query(query)
    
    if df.empty:
        return {
            "resumen": {
                "total_empleados": 0,
                "negativos": 0,
                "acumulados": 0,
                "bajos": 0,
                "normales": 0,
                "sin_vacaciones_12": 0
            },
            "distribucion": [],
            "vacaciones_por_mes": [],
            "proximas_30_dias": [],
            "sin_vacaciones_12_meses": [],
            "empleados_detalle": []
        }
    
    # 🔥 CONVERTIR FECHAS A DATETIME
    for col in ['fecha_ingreso_empresa', 'ultima_vacacion', 'proxima_vacacion', 'proxima_vacacion_fin']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 🔥 CALCULAR INDICADORES
    total_empleados = len(df)
    negativos = len(df[df['estado_saldo'] == 'NEGATIVO'])
    acumulados = len(df[df['estado_saldo'] == 'ACUMULADO'])
    bajos = len(df[df['estado_saldo'] == 'BAJO'])
    normales = len(df[df['estado_saldo'] == 'NORMAL'])
    sin_vacaciones_12 = len(df[df['antiguedad_vacacion'] == 'MAS_12_MESES'])
    
    # 🔥 VACACIONES POR MES (con manejo de NaT)
    df_proximas = df[df['proxima_vacacion'].notna()].copy()
    if not df_proximas.empty:
        df_proximas['mes'] = df_proximas['proxima_vacacion'].dt.month
        df_proximas['nombre_mes'] = df_proximas['proxima_vacacion'].dt.strftime('%B')
        vacaciones_por_mes = df_proximas.groupby(['mes', 'nombre_mes']).size().reset_index(name='cantidad')
        vacaciones_por_mes = vacaciones_por_mes.sort_values('mes').to_dict('records')
    else:
        vacaciones_por_mes = []
    
    # 🔥 PRÓXIMOS 30 DÍAS (con manejo de NaT)
    ahora = pd.Timestamp.now()
    df_proximas_30 = df[
        df['proxima_vacacion'].notna() & 
        (df['proxima_vacacion'] <= ahora + pd.Timedelta(days=30))
    ].copy()
    df_proximas_30 = df_proximas_30.sort_values('proxima_vacacion')
    proximas_30_dias = df_proximas_30.to_dict('records')
    
    # 🔥 SIN VACACIONES > 12 MESES
    df_sin_12 = df[df['antiguedad_vacacion'] == 'MAS_12_MESES'].copy()
    df_sin_12 = df_sin_12.sort_values('dias_sin_vacaciones', ascending=False)
    sin_vacaciones_12_meses = df_sin_12.to_dict('records')
    
    return {
        "resumen": {
            "total_empleados": total_empleados,
            "negativos": negativos,
            "acumulados": acumulados,
            "bajos": bajos,
            "normales": normales,
            "sin_vacaciones_12": sin_vacaciones_12
        },
        "distribucion": [
            {"estado": "🔴 Negativo", "cantidad": negativos, "color": "danger"},
            {"estado": "🟡 Bajo", "cantidad": bajos, "color": "warning"},
            {"estado": "🟢 Normal", "cantidad": normales, "color": "success"},
            {"estado": "🟠 Acumulado", "cantidad": acumulados, "color": "primary"}
        ],
        "vacaciones_por_mes": vacaciones_por_mes,
        "proximas_30_dias": proximas_30_dias,
        "sin_vacaciones_12_meses": sin_vacaciones_12_meses,
        "empleados_detalle": df.to_dict('records')
    }
