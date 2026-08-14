import streamlit as st
import pandas as pd
from datetime import datetime
import io

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"

# ============================================================
# REPORTE: Resumen de Cuentas Pre-Demanda
# ============================================================

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
# REGISTRO DE REPORTES
# ============================================================

REPORTES = {
    "📊 Resumen de Cuentas Pre-Demanda": generar_resumen_predemanda,
    "📋 Cartera Comercial": generar_cartera_comercial,
    "📈 Seguimiento de Cobro": generar_seguimiento_cobro,
}

# ============================================================
# REPORTE 1: Resumen de Cuentas Pre-Demanda
# ============================================================

def generar_resumen_predemanda(proyecto_id):
    """
    Genera el reporte consolidado de Jamar:
    - Cartera pre-demanda
    - Última gestión
    - Mejor gestión
    - Pagos realizados
    """
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
        FROM `{PROYECTO_BQ}.gestiones_jamar`
        WHERE id_proyecto = '{proyecto_id}'
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
        -- Pagos
        p.cantidad_pagos,
        p.total_recaudo,
        p.ultimo_pago AS fecha_ultimo_pago_real,
        -- Gestiones
        g.ultima_gestion_fecha,
        g.ultima_promesa,
        g.total_gestiones,
        g.total_promesas,
        g.total_contactos,
        g.total_no_contactos,
        g.total_contactos_tercero,
        -- Indicadores
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
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles para generar el reporte."
    
    # ---- Crear Excel ----
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Hoja 1: Resumen ejecutivo
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
        
        # Hoja 2: Datos completos
        df.to_excel(writer, sheet_name='Datos', index=False)
        
        # Ajustar ancho de columnas
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    mensaje = f"✅ Reporte generado con {len(df):,} registros"
    return output.getvalue(), mensaje

# ============================================================
# REPORTE 2: Cartera Comercial
# ============================================================

def generar_cartera_comercial(proyecto_id):
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

def generar_seguimiento_cobro(proyecto_id):
    """Reporte de seguimiento de cobro con todas las gestiones"""
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
# REGISTRO DE REPORTES (DESPUÉS de definir la función)
# ============================================================

REPORTES = {
    "📊 Resumen de Cuentas Pre-Demanda": generar_resumen_predemanda,
}
