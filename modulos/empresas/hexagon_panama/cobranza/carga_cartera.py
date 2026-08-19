# ============================================================
# modulo_cargar_base.py - VERSIÓN CON CONFIGURACIÓN DINÁMICA
# ============================================================

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
import re
import io

# Importar servicios
from services.bigquery import ejecutar_query
from services.archivos import leer_excel, validar_columnas
from services.proyectos import obtener_columnas_proyecto, generar_plantilla_proyecto, validar_columnas_proyecto

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_identificacion(valor):
    if pd.isna(valor):
        return None
    return str(valor).strip()

def normalizar_nombre(valor):
    if pd.isna(valor):
        return None
    nombre = str(valor).strip().upper()
    return ' '.join(nombre.split())

def normalizar_telefonos(valor):
    if pd.isna(valor):
        return []
    if isinstance(valor, str):
        valor = valor.replace(';', ',')
        telefonos = [t.strip() for t in valor.split(',') if t.strip()]
        telefonos = list(dict.fromkeys(telefonos))
        return telefonos
    return []

def normalizar_correos(valor):
    if pd.isna(valor):
        return []
    if isinstance(valor, str):
        valor = valor.replace(';', ',')
        correos = [c.strip().lower() for c in valor.split(',') if c.strip()]
        correos = list(dict.fromkeys(correos))
        return correos
    return []

def normalizar_saldo(valor):
    """Normaliza valores numéricos (saldo, montos, etc.)"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        # Limpiar caracteres no numéricos (excepto punto y coma)
        limpiar = re.sub(r'[^\d.,-]', '', valor)
        limpiar = limpiar.replace(',', '.')
        # Si es "NO APLICA" o similar, retornar None
        if limpiar.strip() in ['', 'NO APLICA', 'N/A', 'NA']:
            return None
        try:
            return float(limpiar)
        except:
            return None
    return None

def normalizar_fecha(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.date().isoformat()
    if isinstance(valor, str):
        # Si es "NO APLICA" o similar, retornar None
        if valor.strip() in ['', 'NO APLICA', 'N/A', 'NA']:
            return None
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S']:
            try:
                return datetime.strptime(valor.strip(), fmt).date().isoformat()
            except:
                continue
    return None

def normalizar_string(valor):
    """Normaliza strings, convirtiendo 'NO APLICA' a None"""
    if pd.isna(valor):
        return None
    valor_str = str(valor).strip()
    if valor_str in ['', 'NO APLICA', 'N/A', 'NA', 'nan', 'None']:
        return None
    return valor_str

def normalizar_plazo(valor):
    """Normaliza plazos (pueden ser 'MES VIGENTE', 'HASTA 6 MESES', etc.)"""
    if pd.isna(valor):
        return None
    valor_str = str(valor).strip()
    if valor_str in ['', 'NO APLICA', 'N/A', 'NA', 'nan', 'None']:
        return None
    return valor_str

def normalizar_valor_plan(valor):
    """Normaliza valores de planes de pago"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        valor_limpio = re.sub(r'[^\d.,-]', '', valor)
        valor_limpio = valor_limpio.replace(',', '.')
        if valor_limpio.strip() in ['', 'NO APLICA', 'N/A', 'NA']:
            return None
        try:
            return float(valor_limpio)
        except:
            return None
    return None

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

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    query = """
        SELECT 
            id_proyecto,
            nombre,
            fecha_inicio,
            tiene_descuento_directo
        FROM `proyecto-css-panama.cobranza.proyectos`
        WHERE activo = TRUE
        ORDER BY nombre ASC
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        st.warning(f"⚠️ No se pudieron cargar los proyectos: {str(e)}")
        return pd.DataFrame()

def obtener_historial_cargas(proyecto, limite=20):
    query = f"""
        SELECT 
            fecha_carga,
            registros,
            procesados,
            errores,
            estado
        FROM `proyecto-css-panama.cobranza.historial_cargas`
        WHERE id_proyecto = '{proyecto}'
        ORDER BY fecha_carga DESC
        LIMIT {limite}
    """
    try:
        df = ejecutar_query(query)
        return df
    except Exception as e:
        return pd.DataFrame()

def registrar_carga_en_bigquery(proyecto, registros, procesados, errores, estado, detalle=None):
    id_carga = str(uuid.uuid4())
    query = f"""
        INSERT INTO `proyecto-css-panama.cobranza.historial_cargas`
        (id_carga, id_proyecto, fecha_carga, registros, procesados, errores, estado, detalle)
        VALUES (
            '{id_carga}',
            '{proyecto}',
            CURRENT_TIMESTAMP(),
            {registros},
            {procesados},
            {errores},
            '{estado}',
            '{detalle or ""}'
        )
    """
    try:
        ejecutar_query(query)
        return True
    except Exception as e:
        st.error(f"❌ Error al registrar carga: {str(e)}")
        return False

# ============================================================
# PROCESO DE INGESTA (BATCH) - VERSIÓN DINÁMICA
# ============================================================

def procesar_carga(df, proyecto, df_columnas):
    """
    Procesa la carga de datos usando configuración dinámica.
    
    Args:
        df: DataFrame con los datos
        proyecto: ID del proyecto
        df_columnas: DataFrame con la configuración de columnas
    """
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    detalles = []
    
    # Validar columnas requeridas según configuración
    faltantes = validar_columnas_proyecto(df, df_columnas)
    if faltantes:
        return total, 0, total, f"Faltan columnas: {', '.join(faltantes)}"
    
    # ============================================================
    # PASO 1: Normalizar en memoria
    # ============================================================
    
    personas_para_insertar = []
    cuentas_para_insertar = []
    telefonos_para_insertar = []
    telefonos_proyecto_para_insertar = []
    correos_para_insertar = []
    correos_proyecto_para_insertar = []
    
    ids_personas_unicas = set()
    telefonos_unicos = set()
    correos_unicos = set()
    
    # Crear mapeo de columnas
    col_map = dict(zip(df_columnas['columna_origen'], df_columnas['columna_destino']))
    tipo_map = dict(zip(df_columnas['columna_destino'], df_columnas['tipo_dato']))
    
    for idx, row in df.iterrows():
        try:
            # --- FUNCIÓN PARA NORMALIZAR SEGÚN TIPO ---
            def get_valor(columna_origen, tipo_dato):
                if columna_origen not in row:
                    return None
                valor = row.get(columna_origen)
                if pd.isna(valor):
                    return None
                
                # Normalizar según tipo
                if tipo_dato == 'string':
                    return normalizar_string(valor)
                elif tipo_dato == 'number':
                    return normalizar_saldo(valor)
                elif tipo_dato == 'date':
                    return normalizar_fecha(valor)
                elif tipo_dato == 'phone':
                    # Para teléfonos, retornamos el valor crudo para procesar después
                    return str(valor).strip() if pd.notna(valor) else None
                elif tipo_dato == 'email':
                    return str(valor).strip().lower() if pd.notna(valor) else None
                elif tipo_dato == 'plazo':
                    return normalizar_plazo(valor)
                else:
                    return str(valor).strip() if pd.notna(valor) else None
            
            # --- OBTENER VALORES NORMALIZADOS ---
            identificacion = get_valor('Codigo del Cliente', 'string')
            nombre = get_valor('Nombre del Cliente', 'string')
            cuenta = get_valor('Número de Cuenta', 'string')
            saldo = get_valor('Saldo Total adeudado', 'number')
            cartera = get_valor('Estado inicial', 'string')
            fecha_ultimo_pago = get_valor('Fecha ultimo pago', 'date')
            
            # Campos específicos JAMAR
            tramo_inicial = get_valor('Tramo inicial', 'string')
            cod_agencia = get_valor('Codigo de la Agencia', 'string')
            tipo_credito = get_valor('Tipo credito', 'string')
            saldo_total_vencido = get_valor('Saldo Total vencido', 'number')
            clasificacion = get_valor('Rank', 'string')
            valor_plan1 = get_valor('VR A PAGAR DCTO 1', 'number')
            valor_plan2 = get_valor('VR A PAGAR DCTO 2', 'number')
            plazo_plan_1 = get_valor('PLAZO DCTO 1', 'plazo')
            plazo_plan_2 = get_valor('PLAZO DCTO 2', 'plazo')
            valor_plan3 = get_valor('Vr a pagar PLAN AL DIA', 'number')
            cuota_inicial_arreglo = get_valor('CUOTA INICIAL ARREGLO', 'number')
            saldo_a_diferir = get_valor('Saldo a diferir por cuotas', 'number')
            cod_cobrador = get_valor('Codigo del cobrador', 'string')
            codeudor = get_valor('CODEUDOR', 'string')
            identificacion_codeudor = get_valor('DOC DE CODEUDOR', 'string')
            obligacion = get_valor('obligacion', 'string')
            
            # Validar obligatorios
            if not identificacion or not nombre or not cuenta or saldo is None:
                errores += 1
                detalles.append(f"Fila {idx+2}: Datos obligatorios incompletos (ID: {identificacion}, Cuenta: {cuenta})")
                continue
            
            ids_personas_unicas.add(identificacion)
            
            # --- CONSTRUIR CUENTA ---
            cuentas_para_insertar.append({
                'id_cuenta': str(uuid.uuid4()),
                'identificacion': identificacion,
                'id_proyecto': proyecto,
                'cuenta': cuenta,
                'obligacion': obligacion,
                'saldo': saldo,
                'fecha_ultimo_pago': fecha_ultimo_pago,
                'cartera': cartera,
                # Campos específicos JAMAR
                'tramo_inicial': tramo_inicial,
                'cod_agencia': cod_agencia,
                'tipo_credito': tipo_credito,
                'saldo_total_vencido': saldo_total_vencido,
                'clasificacion': clasificacion,
                'valor_plan1': valor_plan1,
                'valor_plan2': valor_plan2,
                'plazo_plan_1': plazo_plan_1,
                'plazo_plan_2': plazo_plan_2,
                'valor_plan3': valor_plan3,
                'cuota_inicial_arreglo': cuota_inicial_arreglo,
                'saldo_a_diferir': saldo_a_diferir,
                'cod_cobrador': cod_cobrador,
                'codeudor': codeudor,
                'identificacion_codeudor': identificacion_codeudor
            })
            
            # --- PROCESAR TELÉFONOS ---
            telefonos_raw = normalizar_telefonos(row.get('telefono', ''))
            for i, telefono in enumerate(telefonos_raw):
                telefono_limpio = validar_telefono(telefono)
                if not telefono_limpio:
                    continue
                telefonos_unicos.add(telefono_limpio)
                telefonos_proyecto_para_insertar.append({
                    'id_telefono': None,
                    'numero': telefono_limpio,
                    'identificacion': identificacion,
                    'id_proyecto': proyecto,
                    'fuente': 'BASE',
                    'prioridad': i + 1,
                    'estado': 'ACTIVO'
                })
            
            # --- PROCESAR CORREOS ---
            correos = normalizar_correos(row.get('correo', ''))
            for i, correo in enumerate(correos):
                if not correo:
                    continue
                correos_unicos.add(correo)
                correos_proyecto_para_insertar.append({
                    'id_correo': None,
                    'correo': correo,
                    'identificacion': identificacion,
                    'id_proyecto': proyecto,
                    'fuente': 'BASE',
                    'prioridad': i + 1,
                    'estado': 'ACTIVO'
                })
                
        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")
    
    # ============================================================
    # PASO 2: Consultar BigQuery (IDs existentes)
    # ============================================================
    
    if ids_personas_unicas:
        ids_list = "', '".join(ids_personas_unicas)
        query_personas = f"""
            SELECT identificacion, id_persona, nombre
            FROM `proyecto-css-panama.cobranza.personas`
            WHERE identificacion IN ('{ids_list}')
        """
        df_personas_existentes = ejecutar_query(query_personas)
        map_identificacion_a_id = dict(zip(df_personas_existentes['identificacion'], df_personas_existentes['id_persona']))
        map_nombres_existentes = dict(zip(df_personas_existentes['identificacion'], df_personas_existentes['nombre']))
    else:
        map_identificacion_a_id = {}
        map_nombres_existentes = {}
    
    if telefonos_unicos:
        tel_list = "', '".join(telefonos_unicos)
        query_telefonos = f"""
            SELECT numero, id_telefono
            FROM `proyecto-css-panama.cobranza.telefonos`
            WHERE numero IN ('{tel_list}')
        """
        df_telefonos_existentes = ejecutar_query(query_telefonos)
        map_telefono_a_id = dict(zip(df_telefonos_existentes['numero'], df_telefonos_existentes['id_telefono']))
    else:
        map_telefono_a_id = {}
    
    if correos_unicos:
        corr_list = "', '".join(correos_unicos)
        query_correos = f"""
            SELECT correo, id_correo
            FROM `proyecto-css-panama.cobranza.correos`
            WHERE correo IN ('{corr_list}')
        """
        df_correos_existentes = ejecutar_query(query_correos)
        map_correo_a_id = dict(zip(df_correos_existentes['correo'], df_correos_existentes['id_correo']))
    else:
        map_correo_a_id = {}
    
    # ============================================================
    # PASO 3: Asignar IDs en memoria
    # ============================================================
    
    personas_nuevas = []
    for ident in ids_personas_unicas:
        if ident not in map_identificacion_a_id:
            id_persona = str(uuid.uuid4())
            map_identificacion_a_id[ident] = id_persona
            nombre_persona = df[df['Codigo del Cliente'] == ident]['Nombre del Cliente'].iloc[0]
            personas_nuevas.append({
                'id_persona': id_persona,
                'identificacion': ident,
                'nombre': normalizar_nombre(nombre_persona)
            })
    
    telefonos_nuevos = []
    for telefono in telefonos_unicos:
        if telefono not in map_telefono_a_id:
            id_telefono = str(uuid.uuid4())
            map_telefono_a_id[telefono] = id_telefono
            telefonos_nuevos.append({
                'id_telefono': id_telefono,
                'numero': telefono
            })
    
    correos_nuevos = []
    for correo in correos_unicos:
        if correo not in map_correo_a_id:
            id_correo = str(uuid.uuid4())
            map_correo_a_id[correo] = id_correo
            correos_nuevos.append({
                'id_correo': id_correo,
                'correo': correo
            })
    
    # Asignar id_persona a cuentas
    for cuenta in cuentas_para_insertar:
        ident = cuenta.pop('identificacion')
        cuenta['id_persona'] = map_identificacion_a_id[ident]
    
    # Asignar IDs a relaciones
    for rel_tel in telefonos_proyecto_para_insertar:
        rel_tel['id_telefono'] = map_telefono_a_id[rel_tel['numero']]
        rel_tel['id_persona'] = map_identificacion_a_id[rel_tel.pop('identificacion')]
    
    for rel_corr in correos_proyecto_para_insertar:
        rel_corr['id_correo'] = map_correo_a_id[rel_corr['correo']]
        rel_corr['id_persona'] = map_identificacion_a_id[rel_corr.pop('identificacion')]
    
    # ============================================================
    # PASO 4: Insertar por lotes
    # ============================================================
    
    # Personas
    if personas_nuevas:
        valores_personas = [f"('{p['id_persona']}', '{p['identificacion']}', '{p['nombre']}')" for p in personas_nuevas]
        insert_personas = f"""
            INSERT INTO `proyecto-css-panama.cobranza.personas`
            (id_persona, identificacion, nombre)
            VALUES {', '.join(valores_personas)}
        """
        ejecutar_query(insert_personas)
    
    # Cuentas (con todos los campos nuevos)
    if cuentas_para_insertar:
        valores_cuentas = []
        for c in cuentas_para_insertar:
            valores_cuentas.append(f"""(
                '{c['id_cuenta']}',
                '{c['id_persona']}',
                '{c['id_proyecto']}',
                '{c['cuenta']}',
                {f"'{c['obligacion']}'" if c['obligacion'] else 'NULL'},
                {c['saldo']},
                {f"'{c['fecha_ultimo_pago']}'" if c['fecha_ultimo_pago'] else 'NULL'},
                {f"'{c['cartera']}'" if c['cartera'] else 'NULL'},
                -- Campos nuevos JAMAR
                {f"'{c['tramo_inicial']}'" if c.get('tramo_inicial') else 'NULL'},
                {f"'{c['cod_agencia']}'" if c.get('cod_agencia') else 'NULL'},
                {f"'{c['tipo_credito']}'" if c.get('tipo_credito') else 'NULL'},
                {c.get('saldo_total_vencido') if c.get('saldo_total_vencido') is not None else 'NULL'},
                {f"'{c['clasificacion']}'" if c.get('clasificacion') else 'NULL'},
                {c.get('valor_plan1') if c.get('valor_plan1') is not None else 'NULL'},
                {c.get('valor_plan2') if c.get('valor_plan2') is not None else 'NULL'},
                {f"'{c['plazo_plan_1']}'" if c.get('plazo_plan_1') else 'NULL'},
                {f"'{c['plazo_plan_2']}'" if c.get('plazo_plan_2') else 'NULL'},
                {c.get('valor_plan3') if c.get('valor_plan3') is not None else 'NULL'},
                {c.get('cuota_inicial_arreglo') if c.get('cuota_inicial_arreglo') is not None else 'NULL'},
                {c.get('saldo_a_diferir') if c.get('saldo_a_diferir') is not None else 'NULL'},
                {f"'{c['cod_cobrador']}'" if c.get('cod_cobrador') else 'NULL'},
                {f"'{c['codeudor']}'" if c.get('codeudor') else 'NULL'},
                {f"'{c['identificacion_codeudor']}'" if c.get('identificacion_codeudor') else 'NULL'}
            )""")
        
        if valores_cuentas:
            insert_cuentas = f"""
                INSERT INTO `proyecto-css-panama.cobranza.cuentas`
                (id_cuenta, id_persona, id_proyecto, cuenta, obligacion, saldo, 
                 fecha_ultimo_pago, cartera,
                 tramo_inicial, cod_agencia, tipo_credito, saldo_total_vencido,
                 clasificacion, valor_plan1, valor_plan2, plazo_plan_1, plazo_plan_2,
                 valor_plan3, cuota_inicial_arreglo, saldo_a_diferir,
                 cod_cobrador, codeudor, identificacion_codeudor)
                VALUES {', '.join(valores_cuentas)}
            """
            ejecutar_query(insert_cuentas)
    
    # Teléfonos y correos (igual que antes)
    if telefonos_nuevos:
        valores_telefonos = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
        insert_telefonos = f"""
            INSERT INTO `proyecto-css-panama.cobranza.telefonos`
            (id_telefono, numero)
            VALUES {', '.join(valores_telefonos)}
        """
        ejecutar_query(insert_telefonos)
    
    if telefonos_proyecto_para_insertar:
        valores_rel_tel = [f"""(
            '{t['id_telefono']}',
            '{t['id_persona']}',
            '{t['id_proyecto']}',
            '{t['fuente']}',
            {t['prioridad']},
            '{t['estado']}'
        )""" for t in telefonos_proyecto_para_insertar]
        insert_rel_tel = f"""
            INSERT INTO `proyecto-css-panama.cobranza.telefonos_proyecto`
            (id_telefono, id_persona, id_proyecto, fuente, prioridad, estado)
            VALUES {', '.join(valores_rel_tel)}
        """
        ejecutar_query(insert_rel_tel)
    
    if correos_nuevos:
        valores_correos = [f"('{c['id_correo']}', '{c['correo']}')" for c in correos_nuevos]
        insert_correos = f"""
            INSERT INTO `proyecto-css-panama.cobranza.correos`
            (id_correo, correo)
            VALUES {', '.join(valores_correos)}
        """
        ejecutar_query(insert_correos)
    
    if correos_proyecto_para_insertar:
        valores_rel_corr = [f"""(
            '{c['id_correo']}',
            '{c['id_persona']}',
            '{c['id_proyecto']}',
            '{c['fuente']}',
            {c['prioridad']},
            '{c['estado']}'
        )""" for c in correos_proyecto_para_insertar]
        insert_rel_corr = f"""
            INSERT INTO `proyecto-css-panama.cobranza.correos_proyecto`
            (id_correo, id_persona, id_proyecto, fuente, prioridad, estado)
            VALUES {', '.join(valores_rel_corr)}
        """
        ejecutar_query(insert_rel_corr)
    
    procesados = total - errores
    elapsed_time = time.time() - start_time
    detalle = f"{procesados} procesados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    if detalles:
        detalle += f" | Primeros errores: {', '.join(detalles[:3])}"
    
    return total, procesados, errores, detalle

# ============================================================
# GENERAR PLANTILLA (dinámica)
# ============================================================

def generar_plantilla_proyecto(df_columnas, proyecto_nombre):
    """Genera plantilla Excel con las columnas específicas del proyecto"""
    columnas_origen = df_columnas['columna_origen'].tolist()
    nombres_visibles = df_columnas['nombre_visible'].tolist()
    
    # Crear DataFrame con columnas
    df_plantilla = pd.DataFrame(columns=columnas_origen)
    
    # Agregar fila de ejemplo
    ejemplo = {}
    for col in columnas_origen:
        if col in ['identificacion', 'Codigo del Cliente']:
            ejemplo[col] = '8-123-456'
        elif col in ['nombre', 'Nombre del Cliente']:
            ejemplo[col] = 'JUAN PEREZ GONZALEZ'
        elif col in ['cuenta', 'Número de Cuenta']:
            ejemplo[col] = '001-123456-7'
        elif col in ['saldo', 'Saldo Total adeudado']:
            ejemplo[col] = 1250.00
        elif col in ['cartera', 'Estado inicial']:
            ejemplo[col] = 'PREDEMANDA'
        elif col in ['telefono']:
            ejemplo[col] = '61234567, 67891234'
        elif col in ['correo']:
            ejemplo[col] = 'juan@gmail.com'
        else:
            ejemplo[col] = ''
    
    # Agregar ejemplo al DataFrame
    df_plantilla = pd.DataFrame([ejemplo])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_plantilla.to_excel(writer, sheet_name='Carga', index=False)
        
        # Agregar instrucciones
        instrucciones = pd.DataFrame({
            'Instrucciones': [
                f'FORMATO DE CARGA - {proyecto_nombre}',
                '',
                '📌 COLUMNAS OBLIGATORIAS:',
                *[f'  • {col}' for col in df_columnas[df_columnas['es_requerida']]['nombre_visible'].tolist()],
                '',
                '📌 COLUMNAS OPCIONALES:',
                *[f'  • {col}' for col in df_columnas[~df_columnas['es_requerida']]['nombre_visible'].tolist() if col not in df_columnas[df_columnas['es_requerida']]['nombre_visible'].tolist()],
                '',
                '⚠️ REGLAS IMPORTANTES:',
                '  1. Los teléfonos y correos deben ir en UNA SOLA columna',
                '  2. Múltiples valores separados por coma (,)',
                '  3. Las fechas en formato YYYY-MM-DD',
                '  4. Los nombres en MAYÚSCULAS (opcional)',
                '  5. No modificar los nombres de las columnas'
            ]
        })
        instrucciones.to_excel(writer, sheet_name='Instrucciones', index=False, header=False)
        
        # Ajustar columnas
        worksheet = writer.sheets['Carga']
        for i, col in enumerate(df_plantilla.columns):
            worksheet.set_column(i, i, 25)
    
    return output.getvalue()

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    st.markdown("""
    <style>
        .main-header { font-size: 24px; font-weight: 600; color: #1a1a1a; margin-bottom: 8px; }
        .sub-header { font-size: 14px; color: #6b6b6b; margin-bottom: 24px; }
        .card { background-color: #ffffff; border-radius: 12px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04); border: 1px solid #f0f0f0; margin-bottom: 16px; }
        .card-title { font-size: 16px; font-weight: 500; color: #1a1a1a; margin-bottom: 12px; }
        .selected-file { background-color: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 12px 16px; display: flex; align-items: center; gap: 12px; }
        .selected-file .file-name { font-weight: 500; color: #166534; }
        .selected-file .file-size { color: #6b6b6b; font-size: 13px; }
        .history-item { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #f3f4f6; }
        .history-item:last-child { border-bottom: none; }
        .history-date { color: #6b6b6b; font-size: 13px; }
        .history-count { font-weight: 500; }
        .status-success { color: #16a34a; font-weight: 500; }
        .status-warning { color: #ea580c; font-weight: 500; }
        .status-error { color: #dc2626; font-weight: 500; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="main-header">📥 Carga de Cartera</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el archivo con la cartera de clientes. El sistema validará y normalizará la información automáticamente.</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # Obtener proyectos activos
    proyectos_df = obtener_proyectos_activos()
    if len(proyectos_df) == 0:
        st.warning("⚠️ No hay proyectos activos en el sistema. Contacta al administrador.")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    opciones_proyectos = {row['nombre']: row['id_proyecto'] for _, row in proyectos_df.iterrows()}
    nombres_proyectos = list(opciones_proyectos.keys())
    
    proyecto_seleccionado_nombre = st.selectbox(
        "🏢 Proyecto",
        nombres_proyectos,
        index=0 if nombres_proyectos else None,
        help="Selecciona el proyecto al que pertenece esta cartera"
    )
    proyecto_seleccionado = opciones_proyectos.get(proyecto_seleccionado_nombre)
    
    # Obtener configuración de columnas del proyecto
    df_columnas = obtener_columnas_proyecto(proyecto_seleccionado)
    if len(df_columnas) == 0:
        st.warning(f"⚠️ No hay configuración de columnas para el proyecto '{proyecto_seleccionado_nombre}'. Contacta al administrador.")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    st.markdown('<div class="helper-text">La cartera se asignará a este proyecto. Los clientes, cuentas y contactos se vincularán automáticamente.</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([4, 1])
    with col2:
        plantilla_bytes = generar_plantilla_proyecto(df_columnas, proyecto_seleccionado_nombre)
        st.download_button(
            label="📄 Descargar Plantilla",
            data=plantilla_bytes,
            file_name=f"FORMATO_CARGA_{proyecto_seleccionado_nombre}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
        key="carga_cartera_uploader"
    )
    
    if uploaded_file is not None:
        size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        st.markdown(f"""
        <div class="selected-file">
            <span>📄</span>
            <span class="file-name">{uploaded_file.name}</span>
            <span class="file-size">({size_mb:.1f} MB)</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

    if uploaded_file is not None:
        with st.spinner("📊 Procesando archivo..."):
            try:
                # Leer archivo
                df = leer_excel(uploaded_file)
                
                # Validar columnas según configuración
                faltantes = validar_columnas_proyecto(df, df_columnas)
                if faltantes:
                    st.error(f"⚠️ Faltan columnas obligatorias: {', '.join(faltantes)}")
                    st.stop()
                
                # Mostrar vista previa
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown('<div class="card-title">📊 Vista previa del archivo</div>', unsafe_allow_html=True)
                st.dataframe(df.head(10), use_container_width=True)
                
                # Métricas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    has_telefono = 'telefono' in df.columns
                    st.metric("Teléfonos", f"{df['telefono'].notna().sum() if has_telefono else 0:,}")
                with col3:
                    has_correo = 'correo' in df.columns
                    st.metric("Correos", f"{df['correo'].notna().sum() if has_correo else 0:,}")
                with col4:
                    has_cartera = 'Estado inicial' in df.columns
                    if has_cartera:
                        carteras = df['Estado inicial'].value_counts().head(3)
                        st.metric("Carteras", f"{', '.join([f'{k}: {v}' for k, v in carteras.items()])}")
                    else:
                        st.metric("Carteras", "N/A")
                
                # Botón procesar
                if st.button("🚀 Procesar carga", type="primary", use_container_width=True):
                    with st.spinner("🔄 Procesando carga..."):
                        total, procesados, errores, detalle = procesar_carga(df, proyecto_seleccionado, df_columnas)
                        estado = "completada" if errores == 0 else "con_errores"
                        registrar_carga_en_bigquery(proyecto_seleccionado, total, procesados, errores, estado, detalle)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📊 Total", f"{total:,}")
                        with col2:
                            st.metric("✅ Procesados", f"{procesados:,}", delta=f"{procesados/total*100:.1f}%" if total > 0 else "0%")
                        with col3:
                            st.metric("❌ Errores", f"{errores:,}", delta=f"{-errores/total*100:.1f}%" if errores > 0 else "0%")
                        
                        if errores == 0:
                            st.success("🎉 Carga completada exitosamente. Todos los registros fueron procesados.")
                        else:
                            st.warning(f"⚠️ Carga completada con {errores} errores. Revisa el detalle: {detalle}")
                
                st.markdown('</div>', unsafe_allow_html=True)

            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)

    # Mostrar historial de cargas
    if proyecto_seleccionado:
        st.markdown("""
        <div class="card">
            <div class="card-title">📋 Últimas cargas</div>
        """, unsafe_allow_html=True)
        historial_df = obtener_historial_cargas(proyecto_seleccionado)
        if len(historial_df) > 0:
            for _, row in historial_df.iterrows():
                fecha = row['fecha_carga'].strftime('%d/%m/%Y %H:%M') if hasattr(row['fecha_carga'], 'strftime') else str(row['fecha_carga'])
                registros = int(row['registros'])
                estado = row['estado']
                icono = "✅" if estado == "completada" else "⚠️"
                clase = "status-success" if estado == "completada" else "status-warning"
                st.markdown(f"""
                <div class="history-item">
                    <div>
                        <span class="history-date">{fecha}</span>
                        <span style="margin-left: 16px;" class="history-count">{registros:,} registros</span>
                    </div>
                    <div>
                        <span class="{clase}">{icono} {estado.replace('_', ' ').title()}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="text-align: center; padding: 24px; color: #9ca3af; font-size: 14px;">
                No hay cargas registradas para este proyecto.
            </div>
            """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align: center; margin-top: 32px; font-size: 12px; color: #9ca3af; border-top: 1px solid #f0f0f0; padding-top: 16px;">
        Hexagon · Cobranza · Versión 2.0
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    render()
