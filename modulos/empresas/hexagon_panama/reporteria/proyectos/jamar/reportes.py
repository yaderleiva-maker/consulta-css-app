import streamlit as st
import pandas as pd
from datetime import datetime
import io

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"
PROYECTO_ID = "JAMAR"

# ============================================================
# FUNCIÓN PARA PREPARAR DATAFRAME PARA EXCEL
# ============================================================

def preparar_para_excel(df):
    """Excel no acepta timestamps con zona horaria."""
    resultado = df.copy()
    for columna in resultado.select_dtypes(include=["datetimetz"]).columns:
        resultado[columna] = resultado[columna].dt.tz_localize(None)
    return resultado

# ============================================================
# REPORTE 1: Resumen de Cuentas Pre-Demanda
# ============================================================

def generar_resumen_predemanda(proyecto_id, fecha_reporte=None):
    """Resumen de cuentas pre-demanda (acumulado)"""
    # No usa fecha, es acumulado
    query = f"""
    WITH pagos_agrupados AS (
        SELECT 
            llave,
            COUNT(*) AS cantidad_pagos,
            SUM(saldo) AS total_saldo,
            SUM(recaudo_periodo) AS total_recaudo,
            MAX(fecha_up) AS ultimo_pago
        FROM `{PROYECTO_BQ}.pagos_jamar`
        WHERE id_proyecto = '{proyecto_id}'
        GROUP BY llave
    ),
    gestiones_agrupadas AS (
        SELECT 
            llave,
            MAX(fechahoragestion) AS ultima_gestion_fecha,
            MAX(CASE WHEN resultado_gestion = 'COMPROMISO DE PAGO' THEN fechahoragestion END) AS ultima_promesa,
            COUNT(*) AS total_gestiones,
            COUNT(CASE WHEN resultado_gestion = 'COMPROMISO DE PAGO' THEN 1 END) AS total_promesas,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO EFECTIVO' THEN 1 END) AS total_contactos,
            COUNT(CASE WHEN resultado_gestion = 'NO CONTACTOS' THEN 1 END) AS total_no_contactos,
            COUNT(CASE WHEN resultado_gestion = 'CONTACTO CON TERCERO' THEN 1 END) AS total_contactos_tercero
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
        GROUP BY llave
    )
    SELECT 
        c.llave,
        c.estado_inicial,
        c.tramo_inicial,
        c.codigo_agencia,
        c.numero_cuenta,
        c.codigo_cliente,
        c.nombre_cliente,
        c.rank,
        c.saldo_total_adeudado,
        c.saldo_total_vencido,
        c.fecha_ultimo_pago AS fecha_ultimo_pago_cartera,
        p.cantidad_pagos,
        p.total_recaudo,
        p.ultimo_pago AS fecha_ultimo_pago_real,
        g.ultima_gestion_fecha,
        g.ultima_promesa,
        g.total_gestiones,
        g.total_promesas,
        g.total_contactos,
        g.total_no_contactos,
        g.total_contactos_tercero,
        CASE 
            WHEN g.total_promesas > 0 AND p.cantidad_pagos > 0 THEN 'CUMPLE'
            WHEN g.total_promesas > 0 AND p.cantidad_pagos = 0 THEN 'PROMESA SIN PAGO'
            WHEN g.total_contactos > 0 AND p.cantidad_pagos = 0 THEN 'CONTACTADO SIN PAGO'
            ELSE 'SIN CONTACTO'
        END AS estado_cobranza
    FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
    LEFT JOIN pagos_agrupados p ON c.llave = p.llave
    LEFT JOIN gestiones_agrupadas g ON c.llave = g.llave
    WHERE c.id_proyecto = '{proyecto_id}'
    ORDER BY c.saldo_total_adeudado DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_resumen = pd.DataFrame({
            'Métrica': [
                'Proyecto',
                'Fecha de generación',
                'Total de cuentas',
                'Saldo total adeudado',
                'Cuentas con pago',
                'Cuentas con promesa',
                'Cuentas sin contacto',
                'Cuentas con gestión'
            ],
            'Valor': [
                proyecto_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                f"{len(df):,}",
                f"${df['saldo_total_adeudado'].sum():,.2f}",
                f"{df['cantidad_pagos'].notna().sum():,}",
                f"{df['total_promesas'].notna().sum():,}",
                f"{len(df[df['total_gestiones'].isna()]):,}",
                f"{df['total_gestiones'].notna().sum():,}"
            ]
        })
        df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        df.to_excel(writer, sheet_name='Datos', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Reporte generado con {len(df):,} registros"

# ============================================================
# REPORTE 2: Cartera Comercial
# ============================================================

def generar_cartera_comercial(proyecto_id, fecha_reporte=None):
    """Reporte simple de cartera comercial"""
    query = f"""
    SELECT 
        llave,
        codigo_agencia,
        numero_cuenta,
        codigo_cliente,
        nombre_cliente,
        estado_inicial,
        tramo_inicial,
        rank,
        saldo_total_adeudado,
        saldo_total_vencido,
        fecha_ultimo_pago
    FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
    WHERE id_proyecto = '{proyecto_id}'
    ORDER BY saldo_total_adeudado DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Cartera', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Cartera generada con {len(df):,} registros"

# ============================================================
# REPORTE 3: Seguimiento de Cobro
# ============================================================

def generar_seguimiento_cobro(proyecto_id, fecha_reporte=None):
    """Reporte de seguimiento de cobro"""
    query = f"""
    SELECT 
        g.llave,
        g.codigo_cliente,
        g.fechahoragestion,
        g.codigo_gestion,
        g.mejor_gestion_jamar,
        g.resultado_gestion,
        g.observacion,
        g.numeromarcado,
        g.valorpromesa,
        g.fechapromesa,
        c.nombre_cliente,
        c.saldo_total_adeudado
    FROM `{PROYECTO_BQ}.gestiones_jamar` g
    LEFT JOIN `{PROYECTO_BQ}.cartera_predemanda_jamar` c 
        ON g.llave = c.llave AND c.id_proyecto = '{proyecto_id}'
    WHERE g.id_proyecto = '{proyecto_id}'
    ORDER BY g.llave, g.fechahoragestion DESC
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay gestiones disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Seguimiento', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    return output.getvalue(), f"✅ Seguimiento generado con {len(df):,} registros"

# ============================================================
# REPORTE 4: Cuadro de Gestión por RANK (CON DÍA)
# ============================================================

def generar_cuadro_gestion_rank(proyecto_id, fecha_reporte=None):
    """Cuadro de gestión por RANK para un día específico"""
    
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'"
    
    query = f"""
    WITH gestiones_filtradas AS (
        SELECT 
            g.llave,
            g.codigo_cliente,
            g.codigo_gestion,
            g.tipo_gestion,
            g.area_gestion,
            c.rank,
            CASE 
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion = 'T' THEN 'WHATSAPP'
                WHEN g.codigo_gestion = '90' AND g.tipo_gestion != 'T' THEN 'CORREO'
                ELSE 'LLAMADA'
            END AS tipo_gestion_real
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        LEFT JOIN `{PROYECTO_BQ}.cartera_predemanda_jamar` c 
            ON g.llave = c.llave AND c.id_proyecto = '{proyecto_id}'
        WHERE g.id_proyecto = '{proyecto_id}'
          {filtro_fechas}
    ),
    conteo_rank AS (
        SELECT 
            rank,
            tipo_gestion_real,
            COUNT(*) AS cantidad,
            COUNT(DISTINCT llave) AS cuentas_gestionadas
        FROM gestiones_filtradas
        WHERE rank IS NOT NULL
        GROUP BY rank, tipo_gestion_real
    ),
    total_cuentas_rank AS (
        SELECT 
            rank,
            COUNT(DISTINCT llave) AS total_cuentas
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
        WHERE id_proyecto = '{proyecto_id}'
          AND rank IS NOT NULL
        GROUP BY rank
    )
    SELECT 
        c.rank,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) AS correo_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'CORREO' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS correo_intensidad,
        0 AS mensaje_cantidad,
        0.00 AS mensaje_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) AS whatsapp_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'WHATSAPP' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS whatsapp_intensidad,
        COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) AS llamada_cantidad,
        ROUND(COALESCE(SUM(CASE WHEN tipo_gestion_real = 'LLAMADA' THEN cantidad ELSE 0 END), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS llamada_intensidad,
        COALESCE(SUM(cantidad), 0) AS total_cantidad,
        ROUND(COALESCE(SUM(cantidad), 0) * 1.0 / NULLIF(t.total_cuentas, 0), 2) AS total_intensidad,
        t.total_cuentas
    FROM conteo_rank c
    LEFT JOIN total_cuentas_rank t ON c.rank = t.rank
    GROUP BY c.rank, t.total_cuentas
    ORDER BY c.rank
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles para la fecha seleccionada."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Cuadro Gestión', index=False)
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Cuadro de gestión generado ({fecha_str})"

# ============================================================
# REPORTE 5: Cuadro de Resultado de Gestión (CON DÍA)
# ============================================================

def generar_cuadro_resultado_gestion(proyecto_id, fecha_reporte=None):
    """Cuadro de resultado de gestión por RANK para un día específico"""
    
    filtro_fechas = ""
    if fecha_reporte is not None:
        fecha_reporte_str = fecha_reporte.strftime('%Y-%m-%d')
        filtro_fechas = f"AND DATE(g.fechahoragestion) = '{fecha_reporte_str}'"
    
    query = f"""
    WITH ultima_gestion_por_cuenta AS (
        SELECT 
            g.llave,
            g.resultado_gestion,
            g.fechahoragestion,
            ROW_NUMBER() OVER (PARTITION BY g.llave ORDER BY g.fechahoragestion DESC) AS rn
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
          {filtro_fechas}
    ),
    ultima_gestion AS (
        SELECT 
            llave,
            resultado_gestion,
            fechahoragestion
        FROM ultima_gestion_por_cuenta
        WHERE rn = 1
    ),
    cuentas_con_gestion AS (
        SELECT 
            c.llave,
            c.rank,
            CASE 
                WHEN ug.resultado_gestion IS NULL THEN 'SIN GESTION AL CORTE'
                WHEN ug.resultado_gestion = 'CONTACTO EFECTIVO' THEN 'CONTACTO EFECTIVO'
                WHEN ug.resultado_gestion = 'COMPROMISO DE PAGO' THEN 'COMPROMISO DE PAGO'
                WHEN ug.resultado_gestion = 'NO CONTACTOS' THEN 'SIN CONTACTO'
                WHEN ug.resultado_gestion = 'CONTACTO CON TERCERO' THEN 'CONTACTO TERCERO'
                WHEN ug.resultado_gestion IN ('Tono ocupado', 'Equivocado', 'Ilocalizable') THEN 'BUZON DE VOZ'
                ELSE 'SIN CONTACTO'
            END AS categoria_resultado
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN ultima_gestion ug ON c.llave = ug.llave
        WHERE c.id_proyecto = '{proyecto_id}'
    )
    SELECT 
        categoria_resultado AS resultado_gestion,
        COALESCE(SUM(CASE WHEN rank = 'A' THEN 1 ELSE 0 END), 0) AS A,
        COALESCE(SUM(CASE WHEN rank = 'B' THEN 1 ELSE 0 END), 0) AS B,
        COALESCE(SUM(CASE WHEN rank = 'C' THEN 1 ELSE 0 END), 0) AS C,
        COALESCE(SUM(CASE WHEN rank = 'D' THEN 1 ELSE 0 END), 0) AS D,
        COALESCE(COUNT(*), 0) AS TOTAL
    FROM cuentas_con_gestion
    GROUP BY categoria_resultado
    ORDER BY 
        CASE categoria_resultado
            WHEN 'CONTACTO EFECTIVO' THEN 1
            WHEN 'COMPROMISO DE PAGO' THEN 2
            WHEN 'SIN CONTACTO' THEN 3
            WHEN 'CONTACTO TERCERO' THEN 4
            WHEN 'BUZON DE VOZ' THEN 5
            WHEN 'SIN GESTION AL CORTE' THEN 6
            ELSE 7
        END
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles para la fecha seleccionada."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Resultado Gestión', index=False)
        
        query_totales = f"""
            SELECT 
                rank,
                COUNT(*) AS total_cuentas
            FROM `{PROYECTO_BQ}.cartera_predemanda_jamar`
            WHERE id_proyecto = '{proyecto_id}'
            GROUP BY rank
            ORDER BY rank
        """
        df_totales = ejecutar_query(query_totales)
        if not df_totales.empty:
            df_totales.to_excel(writer, sheet_name='Totales por Rank', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 25, 18)
    
    fecha_str = fecha_reporte.strftime('%d/%m/%Y') if fecha_reporte else "Todas las fechas"
    return output.getvalue(), f"✅ Cuadro de resultado generado ({fecha_str})"

# ============================================================
# REPORTE 6: Recaudo y Compromisos (SIN FECHA - ACUMULADO)
# ============================================================

def generar_recaudo_compromisos(proyecto_id, fecha_reporte=None):
    """
    Cuadro de recaudo y compromisos por RANK.
    SIN FILTRO DE FECHA - muestra datos acumulados hasta hoy.
    """
    
    query = f"""
    WITH recaudo_por_rank AS (
        SELECT 
            c.rank,
            SUM(p.recaudo_periodo) AS total_recaudo
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN `{PROYECTO_BQ}.pagos_jamar` p 
            ON c.llave = p.llave AND p.id_proyecto = '{proyecto_id}'
        WHERE c.id_proyecto = '{proyecto_id}'
        GROUP BY c.rank
    ),
    ultima_promesa_por_cuenta AS (
        SELECT 
            g.llave,
            g.valorpromesa,
            g.fechapromesa,
            g.codigo_gestion,
            ROW_NUMBER() OVER (PARTITION BY g.llave ORDER BY g.fechapromesa DESC) AS rn
        FROM `{PROYECTO_BQ}.gestiones_jamar` g
        WHERE g.id_proyecto = '{proyecto_id}'
          AND g.codigo_gestion IN ('01', '88', '89')
          AND g.valorpromesa IS NOT NULL
          AND g.valorpromesa > 0
    ),
    ultima_promesa AS (
        SELECT 
            llave,
            valorpromesa,
            fechapromesa
        FROM ultima_promesa_por_cuenta
        WHERE rn = 1
    ),
    compromisos_por_rank AS (
        SELECT 
            c.rank,
            SUM(CASE 
                WHEN up.fechapromesa >= CURRENT_DATE() THEN up.valorpromesa 
                ELSE 0 
            END) AS compromisos_activos,
            SUM(CASE 
                WHEN up.fechapromesa < CURRENT_DATE() THEN up.valorpromesa 
                ELSE 0 
            END) AS compromisos_incumplidos
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
        LEFT JOIN ultima_promesa up ON c.llave = up.llave
        WHERE c.id_proyecto = '{proyecto_id}'
        GROUP BY c.rank
    )
    SELECT 
        COALESCE(r.rank, c.rank) AS rank,
        ROUND(COALESCE(r.total_recaudo, 0), 2) AS recaudo,
        ROUND(COALESCE(cp.compromisos_activos, 0), 2) AS compromisos_activos,
        ROUND(COALESCE(cp.compromisos_incumplidos, 0), 2) AS compromisos_incumplidos,
        COUNT(DISTINCT c.llave) AS total_cuentas
    FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` c
    LEFT JOIN recaudo_por_rank r ON c.rank = r.rank
    LEFT JOIN compromisos_por_rank cp ON c.rank = cp.rank
    WHERE c.id_proyecto = '{proyecto_id}'
    GROUP BY c.rank, r.total_recaudo, cp.compromisos_activos, cp.compromisos_incumplidos
    ORDER BY c.rank
    """
    
    df = ejecutar_query(query)
    df = preparar_para_excel(df)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles."
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Recaudo y Compromisos', index=False)
        
        totales = pd.DataFrame({
            'Métrica': ['RECAUDO', 'COMPROMISOS ACTIVOS', 'COMPROMISOS INCUMPLIDOS'],
            'Total': [
                df['recaudo'].sum(),
                df['compromisos_activos'].sum(),
                df['compromisos_incumplidos'].sum()
            ]
        })
        totales.to_excel(writer, sheet_name='Totales Generales', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 25, 18)
    
    return output.getvalue(), f"✅ Recaudo y compromisos generado (datos acumulados)"

# ============================================================
# REGISTRO DE REPORTES
# ============================================================

REPORTES = {
    "📊 Resumen de Cuentas Pre-Demanda": generar_resumen_predemanda,
    "📋 Cartera Comercial": generar_cartera_comercial,
    "📈 Seguimiento de Cobro": generar_seguimiento_cobro,
    "📊 Cuadro de Gestión por RANK": generar_cuadro_gestion_rank,
    "📊 Cuadro de Resultado de Gestión": generar_cuadro_resultado_gestion,
    "💰 Recaudo y Compromisos": generar_recaudo_compromisos,
}
