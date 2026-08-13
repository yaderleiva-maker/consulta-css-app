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
# REGISTRO DE REPORTES
# ============================================================

REPORTES = {
    "📊 Resumen de Cuentas Pre-Demanda": generar_resumen_predemanda,
}

# ============================================================
# REPORTE: Resumen de Cuentas Pre-Demanda
# ============================================================

def generar_resumen_predemanda(proyecto_id):
    """
    Genera el reporte "Resumen de Cuentas Pre-Demanda" de Jamar.
    """
    query = f"""
        SELECT 
            j.estado_inicial,
            j.tramo_inicial,
            j.codigo_agencia,
            j.numero_cuenta,
            j.llave,
            j.tipo_credito,
            j.saldo_total_adeudado,
            j.saldo_total_vencido,
            j.fecha_ultimo_pago,
            j.codigo_cliente,
            j.nombre_cliente,
            j.entidad,
            j.rank,
            j.vr_pagar_dcto_1,
            j.vr_pagar_dcto_2,
            j.plazo_dcto_1,
            j.plazo_dcto_2,
            j.vr_pagar_plan_al_dia,
            j.cuota_inicial_arreglo,
            j.saldo_diferir_cuotas,
            -- Información de investigación desde Cobranza
            p.id_persona,
            p.nombre AS nombre_persona,
            c.saldo AS saldo_cobranza,
            c.empresa AS empresa_actual,
            -- Teléfonos investigados
            (
                SELECT STRING_AGG(DISTINCT t.numero, ', ')
                FROM `{PROYECTO_BQ}.telefonos_proyecto` tp
                JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
                WHERE tp.id_persona = p.id_persona
                  AND tp.id_proyecto = '{proyecto_id}'
                  AND tp.fuente = 'INVESTIGACION'
            ) AS telefonos_investigados,
            (
                SELECT STRING_AGG(DISTINCT t.numero, ', ')
                FROM `{PROYECTO_BQ}.telefonos_proyecto` tp
                JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
                WHERE tp.id_persona = p.id_persona
                  AND tp.id_proyecto = '{proyecto_id}'
                  AND tp.fuente = 'BASE'
            ) AS telefonos_base
        FROM `{PROYECTO_BQ}.cartera_predemanda_jamar` j
        LEFT JOIN `{PROYECTO_BQ}.personas` p 
            ON j.codigo_cliente = p.identificacion
        LEFT JOIN `{PROYECTO_BQ}.cuentas` c 
            ON p.id_persona = c.id_persona AND c.id_proyecto = '{proyecto_id}'
        WHERE j.id_proyecto = '{proyecto_id}'
        ORDER BY j.saldo_total_adeudado DESC
    """
    
    df = ejecutar_query(query)
    
    if df.empty:
        return None, "⚠️ No hay datos disponibles para generar el reporte. Asegúrate de haber cargado la cartera de Jamar."
    
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
                'Saldo promedio',
                'Saldo máximo',
                'Saldo mínimo',
                'Cuentas con investigación',
                'Cuentas sin investigación',
                'Rank A',
                'Rank B',
                'Rank C',
                'Rank D',
                'Rank E'
            ],
            'Valor': [
                proyecto_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                f"{len(df):,}",
                f"${df['saldo_total_adeudado'].sum():,.2f}",
                f"${df['saldo_total_adeudado'].mean():,.2f}",
                f"${df['saldo_total_adeudado'].max():,.2f}",
                f"${df['saldo_total_adeudado'].min():,.2f}",
                f"{df['id_persona'].notna().sum():,}",
                f"{df['id_persona'].isna().sum():,}",
                f"{len(df[df['rank'] == 'A']):,}",
                f"{len(df[df['rank'] == 'B']):,}",
                f"{len(df[df['rank'] == 'C']):,}",
                f"{len(df[df['rank'] == 'D']):,}",
                f"{len(df[df['rank'] == 'E']):,}"
            ]
        })
        df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        # Hoja 2: Datos completos
        df.to_excel(writer, sheet_name='Datos', index=False)
        
        # Hoja 3: Análisis por Rank
        if 'rank' in df.columns:
            df_rank = df.groupby('rank').agg({
                'codigo_cliente': 'count',
                'saldo_total_adeudado': ['sum', 'mean']
            }).round(2)
            df_rank.columns = ['Cantidad', 'Saldo Total', 'Saldo Promedio']
            df_rank.to_excel(writer, sheet_name='Análisis por Rank')
        
        # Hoja 4: Cuentas sin investigación (prioridad)
        df_sin_inv = df[df['id_persona'].isna()][[
            'codigo_cliente', 'nombre_cliente', 'numero_cuenta', 
            'saldo_total_adeudado', 'estado_inicial', 'rank'
        ]]
        if not df_sin_inv.empty:
            df_sin_inv.to_excel(writer, sheet_name='Sin Investigación', index=False)
        
        # Ajustar ancho de columnas
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    
    mensaje = f"✅ Reporte generado con {len(df):,} registros"
    return output.getvalue(), mensaje
