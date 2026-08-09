import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime

from services.bigquery import ejecutar_query

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROYECTO_BQ = "proyecto-css-panama.cobranza"

# ============================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================

def validar_telefono(numero):
    """Valida un número de teléfono para Panamá (7 u 8 dígitos, sin prefijo)."""
    if not numero or str(numero).strip() in ['', '0', '000', 'nan', 'None']:
        return None
    limpio = re.sub(r'[^0-9]', '', str(numero))
    if len(limpio) not in [7, 8]:
        return None
    if limpio.count('0') == len(limpio):
        return None
    if limpio.startswith('6') and len(limpio) != 8:
        return None
    if not limpio.startswith('6') and len(limpio) not in [7, 8]:
        return None
    return limpio

def validar_correo(correo):
    """Valida formato básico de correo electrónico."""
    if not correo or str(correo).strip() in ['', 'nan', 'None']:
        return None
    correo = str(correo).strip().lower()
    if '@' not in correo or '.' not in correo:
        return None
    return correo

# ============================================================
# FUNCIONES DE BIGQUERY (consultas batch)
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    query = f"""
        SELECT id_proyecto, nombre
        FROM `{PROYECTO_BQ}.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    return ejecutar_query(query)

def obtener_personas_por_cedula(cedulas):
    if not cedulas:
        return pd.DataFrame()
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT id_persona, identificacion, nombre
        FROM `{PROYECTO_BQ}.personas`
        WHERE identificacion IN ('{cedulas_escapadas}')
    """
    return ejecutar_query(query)

def obtener_telefonos_existentes(proyecto_id, cedulas):
    """Devuelve conjunto de (cedula, numero) que ya existen en telefonos_proyecto para el proyecto."""
    if not cedulas:
        return set()
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT p.identificacion, t.numero
        FROM `{PROYECTO_BQ}.telefonos_proyecto` tp
        JOIN `{PROYECTO_BQ}.personas` p ON tp.id_persona = p.id_persona
        JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE tp.id_proyecto = '{proyecto_id}'
          AND p.identificacion IN ('{cedulas_escapadas}')
    """
    df = ejecutar_query(query)
    return set(zip(df['identificacion'], df['numero'])) if not df.empty else set()

def obtener_correos_existentes(proyecto_id, cedulas):
    if not cedulas:
        return set()
    cedulas_escapadas = "', '".join([str(c).strip() for c in cedulas if str(c).strip()])
    query = f"""
        SELECT p.identificacion, c.correo
        FROM `{PROYECTO_BQ}.correos_proyecto` cp
        JOIN `{PROYECTO_BQ}.personas` p ON cp.id_persona = p.id_persona
        JOIN `{PROYECTO_BQ}.correos` c ON cp.id_correo = c.id_correo
        WHERE cp.id_proyecto = '{proyecto_id}'
          AND p.identificacion IN ('{cedulas_escapadas}')
    """
    df = ejecutar_query(query)
    return set(zip(df['identificacion'], df['correo'])) if not df.empty else set()

def obtener_catalogo_telefonos(numeros):
    """Devuelve dict {numero: id_telefono} para números existentes en catálogo global."""
    if not numeros:
        return {}
    valores = "', '".join(str(x).replace("'", "''") for x in numeros if x)
    query = f"""
        SELECT numero, id_telefono
        FROM `{PROYECTO_BQ}.telefonos`
        WHERE numero IN ('{valores}')
    """
    df = ejecutar_query(query)
    return dict(zip(df['numero'], df['id_telefono'])) if not df.empty else {}

def obtener_catalogo_correos(correos):
    if not correos:
        return {}
    valores = "', '".join(str(x).replace("'", "''") for x in correos if x)
    query = f"""
        SELECT correo, id_correo
        FROM `{PROYECTO_BQ}.correos`
        WHERE correo IN ('{valores}')
    """
    df = ejecutar_query(query)
    return dict(zip(df['correo'], df['id_correo'])) if not df.empty else {}

# ============================================================
# FUNCIONES DE INSERCIÓN POR LOTE
# ============================================================

def insertar_telefonos_batch(telefonos_nuevos):
    if not telefonos_nuevos:
        return
    valores = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.telefonos` (id_telefono, numero)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_telefonos_proyecto_batch(relaciones):
    if not relaciones:
        return
    valores = [f"""(
        '{r['id_telefono']}',
        '{r['id_persona']}',
        '{r['id_proyecto']}',
        '{r['fuente']}',
        {r['prioridad']},
        '{r['estado']}'
    )""" for r in relaciones]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.telefonos_proyecto`
        (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_correos_batch(correos_nuevos):
    if not correos_nuevos:
        return
    valores = [f"('{c['id_correo']}', '{c['correo']}')" for c in correos_nuevos]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.correos` (id_correo, correo)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

def insertar_correos_proyecto_batch(relaciones):
    if not relaciones:
        return
    valores = [f"""(
        '{r['id_correo']}',
        '{r['id_persona']}',
        '{r['id_proyecto']}',
        '{r['fuente']}',
        {r['prioridad']},
        '{r['estado']}'
    )""" for r in relaciones]
    query = f"""
        INSERT INTO `{PROYECTO_BQ}.correos_proyecto`
        (id_correo, id_persona, id_proyecto, fuente, prioridad, estado)
        VALUES {', '.join(valores)}
    """
    ejecutar_query(query)

# ============================================================
# FUNCIÓN PRINCIPAL DE ANEXADO
# ============================================================

def anexar_investigacion(df, proyecto_id, tipo):
    """
    Anexa teléfonos o correos de investigación a Cobranza.
    tipo: 'telefonos' o 'correos'
    """
    import time
    start_time = time.time()

    total = len(df)
    anexados = 0
    errores = 0
    detalles = []

    # Determinar columna de valor
    col_valor = 'numero' if tipo == 'telefonos' else 'correo'

    # Validar columnas
    if 'cedula' not in df.columns or col_valor not in df.columns:
        return total, 0, total, f"Faltan columnas: 'cedula' y '{col_valor}'"

    # Normalizar y limpiar datos
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    df['cedula'] = df['cedula'].fillna('').astype(str).str.strip()
    df[col_valor] = df[col_valor].fillna('').astype(str).str.strip()
    cedulas = df.loc[df['cedula'] != '', 'cedula'].unique().tolist()

    # Obtener personas existentes
    df_personas = obtener_personas_por_cedula(cedulas)
    map_cedula_a_id = dict(zip(df_personas['identificacion'], df_personas['id_persona']))

    # Obtener existentes en el proyecto y catálogo global
    if tipo == 'telefonos':
        existentes_set = obtener_telefonos_existentes(proyecto_id, cedulas)
        validar_valor = validar_telefono
        fuente = 'INVESTIGACION'
    else:
        existentes_set = obtener_correos_existentes(proyecto_id, cedulas)
        validar_valor = validar_correo
        fuente = 'INVESTIGACION'

    nuevos_catalogos = []
    nuevas_relaciones = []

    # Pre-cargar IDs de catálogo global para todos los valores válidos
    valores_validos = {
        validar_valor(valor)
        for valor in df[col_valor]
        if validar_valor(valor)
    }
    cache_ids = (
        obtener_catalogo_telefonos(valores_validos)
        if tipo == 'telefonos'
        else obtener_catalogo_correos(valores_validos)
    )
    relaciones_pendientes = set()

    for _, row in df.iterrows():
        cedula = str(row['cedula']).strip()
        valor_raw = str(row[col_valor]).strip()

        valor_limpio = validar_valor(valor_raw)
        if not valor_limpio:
            errores += 1
            detalles.append(f"Cédula {cedula}: {col_valor} inválido '{valor_raw}'")
            continue

        if cedula not in map_cedula_a_id:
            errores += 1
            detalles.append(f"Cédula {cedula}: persona no encontrada en Cobranza")
            continue

        id_persona = map_cedula_a_id[cedula]
        clave_relacion = (cedula, valor_limpio)

        if clave_relacion in existentes_set or clave_relacion in relaciones_pendientes:
            continue

        # Verificar si ya existe en catálogo global (cache)
        if valor_limpio in cache_ids:
            id_valor = cache_ids[valor_limpio]
        else:
            # Nuevo en catálogo global
            id_valor = str(uuid.uuid4())
            cache_ids[valor_limpio] = id_valor
            nuevos_catalogos.append({
                'id': id_valor,
                'valor': valor_limpio
            })

        # Crear relación con el proyecto
        nuevas_relaciones.append({
            'id_valor': id_valor,
            'id_persona': id_persona,
            'id_proyecto': proyecto_id,
            'fuente': fuente,
            'prioridad': 10,  # prioridad más baja que BASE
            'estado': 'ACTIVO'
        })
        relaciones_pendientes.add(clave_relacion)

    # Insertar en BigQuery
    if nuevos_catalogos:
        if tipo == 'telefonos':
            insertar_telefonos_batch([{'id_telefono': c['id'], 'numero': c['valor']} for c in nuevos_catalogos])
        else:
            insertar_correos_batch([{'id_correo': c['id'], 'correo': c['valor']} for c in nuevos_catalogos])

    if nuevas_relaciones:
        if tipo == 'telefonos':
            relaciones = [{
                'id_telefono': r['id_valor'],
                'id_persona': r['id_persona'],
                'id_proyecto': r['id_proyecto'],
                'fuente': r['fuente'],
                'prioridad': r['prioridad'],
                'estado': r['estado']
            } for r in nuevas_relaciones]
            insertar_telefonos_proyecto_batch(relaciones)
        else:
            relaciones = [{
                'id_correo': r['id_valor'],
                'id_persona': r['id_persona'],
                'id_proyecto': r['id_proyecto'],
                'fuente': r['fuente'],
                'prioridad': r['prioridad'],
                'estado': r['estado']
            } for r in nuevas_relaciones]
            insertar_correos_proyecto_batch(relaciones)

    anexados = len(nuevas_relaciones)
    elapsed_time = time.time() - start_time
    detalle = f"{anexados} {tipo} anexados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    return total, anexados, errores, detalle

# ============================================================
# FUNCIONES PARA GENERAR REPORTE EXCEL
# ============================================================

def generar_reporte_investigacion(proyecto_id):
    """Genera un dict con DataFrames para el reporte completo."""
    # 1. Personas + Cuentas
    query_personas = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            c.cuenta,
            c.empresa,
            c.ocupacion,
            c.direccion,
            c.saldo,
            c.fecha_ultimo_pago,
            c.dias_mora,
            c.cartera
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.cuentas` c ON p.id_persona = c.id_persona
        WHERE c.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre
    """
    df_personas = ejecutar_query(query_personas)

    # 2. Teléfonos
    query_telefonos = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            t.numero,
            t.tipo,
            tp.fuente AS origen,
            tp.prioridad,
            tp.cant_toques,
            tp.cant_contactos,
            tp.cant_no_contactos,
            tp.estado
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.telefonos_proyecto` tp ON p.id_persona = tp.id_persona
        JOIN `{PROYECTO_BQ}.telefonos` t ON tp.id_telefono = t.id_telefono
        WHERE tp.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre, tp.prioridad
    """
    df_telefonos = ejecutar_query(query_telefonos)

    # 3. Correos
    query_correos = f"""
        SELECT 
            p.identificacion,
            p.nombre,
            c.correo,
            cp.fuente AS origen,
            cp.prioridad,
            cp.estado
        FROM `{PROYECTO_BQ}.personas` p
        JOIN `{PROYECTO_BQ}.correos_proyecto` cp ON p.id_persona = cp.id_persona
        JOIN `{PROYECTO_BQ}.correos` c ON cp.id_correo = c.id_correo
        WHERE cp.id_proyecto = '{proyecto_id}'
        ORDER BY p.nombre, cp.prioridad
    """
    df_correos = ejecutar_query(query_correos)

    # 4. Resumen
    resumen = {
        "Proyecto": proyecto_id,
        "Fecha generación": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Total clientes": len(df_personas),
        "Total teléfonos": len(df_telefonos),
        "Total correos": len(df_correos),
        "Teléfonos BASE": len(df_telefonos[df_telefonos['origen'] == 'BASE']) if not df_telefonos.empty else 0,
        "Teléfonos INVESTIGACION": len(df_telefonos[df_telefonos['origen'] == 'INVESTIGACION']) if not df_telefonos.empty else 0,
        "Teléfonos INACTIVOS": len(df_telefonos[df_telefonos['estado'] == 'INACTIVO']) if not df_telefonos.empty else 0,
    }
    df_resumen = pd.DataFrame([resumen])

    return {
        'resumen': df_resumen,
        'personas': df_personas,
        'telefonos': df_telefonos,
        'correos': df_correos
    }

def generar_excel_reporte(data):
    """Genera bytes de un archivo Excel con múltiples hojas."""
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        data['resumen'].to_excel(writer, sheet_name='Resumen', index=False)
        if not data['personas'].empty:
            data['personas'].to_excel(writer, sheet_name='Clientes', index=False)
        if not data['telefonos'].empty:
            data['telefonos'].to_excel(writer, sheet_name='Teléfonos', index=False)
        if not data['correos'].empty:
            data['correos'].to_excel(writer, sheet_name='Correos', index=False)
        # Ajustar anchos
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column(0, 20, 18)
    return output.getvalue()
