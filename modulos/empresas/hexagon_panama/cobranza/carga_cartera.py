import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
import re
import io

# Importar servicios de Hexagon
from services.bigquery import ejecutar_query
from services.archivos import leer_excel, validar_columnas

# ============================================================
# CONFIGURACIÓN
# ============================================================

COLUMNAS_REQUERIDAS = ['identificacion', 'nombre', 'cuenta', 'saldo']
COLUMNAS_OPCIONALES = ['telefono', 'correo', 'empresa', 'direccion', 'ocupacion', 
                       'fecha_ultimo_pago', 'dias_mora', 'cartera', 'observaciones']

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalizar_identificacion(valor):
    """Mantiene la identificación exactamente como llega"""
    if pd.isna(valor):
        return None
    return str(valor).strip()

def normalizar_nombre(valor):
    """Convierte nombre a MAYÚSCULAS y elimina espacios dobles"""
    if pd.isna(valor):
        return None
    nombre = str(valor).strip().upper()
    return ' '.join(nombre.split())

def normalizar_telefonos(valor):
    """Separa teléfonos por coma, limpia espacios y elimina duplicados"""
    if pd.isna(valor):
        return []
    if isinstance(valor, str):
        valor = valor.replace(';', ',')
        telefonos = [t.strip() for t in valor.split(',') if t.strip()]
        telefonos = list(dict.fromkeys(telefonos))
        return telefonos
    return []

def normalizar_correos(valor):
    """Separa correos por coma, limpia espacios y elimina duplicados"""
    if pd.isna(valor):
        return []
    if isinstance(valor, str):
        valor = valor.replace(';', ',')
        correos = [c.strip().lower() for c in valor.split(',') if c.strip()]
        correos = list(dict.fromkeys(correos))
        return correos
    return []

def normalizar_saldo(valor):
    """Convierte saldo a float, manejando formatos con comas y puntos"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        limpiar = re.sub(r'[^\d.,-]', '', valor)
        limpiar = limpiar.replace(',', '.')
        try:
            return float(limpiar)
        except:
            return None
    return None

def normalizar_fecha(valor):
    """Convierte a formato DATE (YYYY-MM-DD)"""
    if pd.isna(valor):
        return None
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.date().isoformat()
    if isinstance(valor, str):
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
            try:
                return datetime.strptime(valor.strip(), fmt).date().isoformat()
            except:
                continue
    return None

# ============================================================
# FUNCIONES DE BIGQUERY
# ============================================================

@st.cache_data(ttl=300)
def obtener_proyectos_activos():
    """Obtiene lista de proyectos activos desde BigQuery"""
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
    """Obtiene el historial de cargas de un proyecto"""
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
    """Registra una carga en el historial de BigQuery"""
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
# PROCESO DE INGESTA
# ============================================================

def procesar_carga(df, proyecto):
    """
    Procesa el DataFrame y lo descompone en las tablas de BigQuery.
    VERSIÓN BATCH - OPTIMIZADA PARA GRANDES VOLÚMENES.
    
    Estrategia:
    1. Normalizar TODO en memoria (Pandas)
    2. Extraer identificadores únicos
    3. 3 consultas a BigQuery para obtener existentes
    4. Match en memoria (sets/dicts)
    5. Preparar inserts por lote (4-6 consultas de escritura)
    """
    import time
    start_time = time.time()
    
    total = len(df)
    errores = 0
    detalles = []

    # ============================================================
    # PASO 1: Validar columnas requeridas
    # ============================================================
    
    faltantes = validar_columnas(df, COLUMNAS_REQUERIDAS)
    if faltantes:
        return total, 0, total, f"Faltan columnas: {', '.join(faltantes)}"

    # ============================================================
    # PASO 2: Normalizar TODO en memoria (una sola pasada)
    # ============================================================
    
    # Listas para acumular datos
    personas_para_insertar = []
    cuentas_para_insertar = []
    telefonos_para_insertar = []
    telefonos_proyecto_para_insertar = []
    correos_para_insertar = []
    correos_proyecto_para_insertar = []
    
    # Conjuntos para extraer identificadores únicos
    ids_personas_unicas = set()
    telefonos_unicos = set()
    correos_unicos = set()
    
    # Diccionario para mapear identificacion -> id_persona (después de la consulta)
    # Lo llenaremos después de consultar BigQuery
    
    for idx, row in df.iterrows():
        try:
            identificacion = normalizar_identificacion(row.get('identificacion'))
            nombre = normalizar_nombre(row.get('nombre'))
            cuenta = str(row.get('cuenta', '')).strip()
            saldo = normalizar_saldo(row.get('saldo'))
            
            if not identificacion or not nombre or not cuenta or saldo is None:
                errores += 1
                detalles.append(f"Fila {idx+2}: Datos obligatorios incompletos")
                continue

            # Guardar identificación única para consultar después
            ids_personas_unicas.add(identificacion)
            
            # Preparar datos de cuenta (sin id_persona todavía, lo asignaremos después)
            obligacion = str(row.get('obligacion', '')).strip() if pd.notna(row.get('obligacion')) else None
            empresa = str(row.get('empresa', '')).strip() if pd.notna(row.get('empresa')) else None
            direccion = str(row.get('direccion', '')).strip() if pd.notna(row.get('direccion')) else None
            ocupacion = str(row.get('ocupacion', '')).strip() if pd.notna(row.get('ocupacion')) else None
            dias_mora = int(row.get('dias_mora')) if pd.notna(row.get('dias_mora')) else None
            cartera = str(row.get('cartera', '')).strip() if pd.notna(row.get('cartera')) else None
            observaciones = str(row.get('observaciones', '')).strip() if pd.notna(row.get('observaciones')) else None
            fecha_ultimo_pago = normalizar_fecha(row.get('fecha_ultimo_pago')) if pd.notna(row.get('fecha_ultimo_pago')) else None
            
            cuentas_para_insertar.append({
                'id_cuenta': str(uuid.uuid4()),
                'identificacion': identificacion,  # Temporal, después lo reemplazamos con id_persona
                'id_proyecto': proyecto,
                'cuenta': cuenta,
                'obligacion': obligacion,
                'saldo': saldo,
                'fecha_ultimo_pago': fecha_ultimo_pago,
                'empresa': empresa,
                'direccion': direccion,
                'ocupacion': ocupacion,
                'dias_mora': dias_mora,
                'cartera': cartera,
                'observaciones': observaciones
            })

            # Preparar TELÉFONOS
            telefonos = normalizar_telefonos(row.get('telefono'))
            for i, telefono in enumerate(telefonos):
                telefonos_unicos.add(telefono)
                telefonos_proyecto_para_insertar.append({
                    'id_telefono': None,  # Temporal, lo asignamos después
                    'numero': telefono,
                    'identificacion': identificacion,  # Para match después
                    'id_proyecto': proyecto,
                    'fuente': 'CARGA_INICIAL',
                    'prioridad': i + 1,
                    'estado': 'ACTIVO'
                })

            # Preparar CORREOS
            correos = normalizar_correos(row.get('correo'))
            for i, correo in enumerate(correos):
                correos_unicos.add(correo)
                correos_proyecto_para_insertar.append({
                    'id_correo': None,  # Temporal
                    'correo': correo,
                    'identificacion': identificacion,  # Para match después
                    'id_proyecto': proyecto,
                    'fuente': 'CARGA_INICIAL',
                    'prioridad': i + 1,
                    'estado': 'ACTIVO'
                })

        except Exception as e:
            errores += 1
            detalles.append(f"Fila {idx+2}: {str(e)}")

    # ============================================================
    # PASO 3: Consultar BigQuery UNA SOLA VEZ por cada tipo de entidad
    # ============================================================
    
    # 3.1 Consultar PERSONAS existentes
    if ids_personas_unicas:
        ids_list = "', '".join(ids_personas_unicas)
        query_personas = f"""
            SELECT identificacion, id_persona, nombre
            FROM `proyecto-css-panama.cobranza.personas`
            WHERE identificacion IN ('{ids_list}')
        """
        df_personas_existentes = ejecutar_query(query_personas)
        # Crear diccionario: identificacion -> id_persona
        map_identificacion_a_id = dict(zip(df_personas_existentes['identificacion'], df_personas_existentes['id_persona']))
        # También guardar nombres para actualizar si cambian
        map_nombres_existentes = dict(zip(df_personas_existentes['identificacion'], df_personas_existentes['nombre']))
    else:
        map_identificacion_a_id = {}
        map_nombres_existentes = {}

    # 3.2 Consultar TELÉFONOS existentes
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

    # 3.3 Consultar CORREOS existentes
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
    # PASO 4: Procesar en memoria para asignar IDs
    # ============================================================
    
    # 4.1 Determinar qué personas son NUEVAS
    personas_nuevas = []
    for ident in ids_personas_unicas:
        if ident not in map_identificacion_a_id:
            id_persona = str(uuid.uuid4())
            map_identificacion_a_id[ident] = id_persona
            # Buscar el nombre de esta persona (de la primera fila donde apareció)
            nombre = df[df['identificacion'] == ident]['nombre'].iloc[0]
            personas_nuevas.append({
                'id_persona': id_persona,
                'identificacion': ident,
                'nombre': normalizar_nombre(nombre)
            })

    # 4.2 Determinar qué teléfonos son NUEVOS
    telefonos_nuevos = []
    for telefono in telefonos_unicos:
        if telefono not in map_telefono_a_id:
            id_telefono = str(uuid.uuid4())
            map_telefono_a_id[telefono] = id_telefono
            telefonos_nuevos.append({
                'id_telefono': id_telefono,
                'numero': telefono
            })

    # 4.3 Determinar qué correos son NUEVOS
    correos_nuevos = []
    for correo in correos_unicos:
        if correo not in map_correo_a_id:
            id_correo = str(uuid.uuid4())
            map_correo_a_id[correo] = id_correo
            correos_nuevos.append({
                'id_correo': id_correo,
                'correo': correo
            })

    # 4.4 Ahora asignar id_persona a las cuentas (reemplazar identificacion)
    for cuenta in cuentas_para_insertar:
        ident = cuenta.pop('identificacion')
        cuenta['id_persona'] = map_identificacion_a_id[ident]

    # 4.5 Asignar id_telefono a las relaciones telefonos_proyecto
    for rel_tel in telefonos_proyecto_para_insertar:
        rel_tel['id_telefono'] = map_telefono_a_id[rel_tel['numero']]
        rel_tel['id_persona'] = map_identificacion_a_id[rel_tel.pop('identificacion')]

    # 4.6 Asignar id_correo a las relaciones correos_proyecto
    for rel_corr in correos_proyecto_para_insertar:
        rel_corr['id_correo'] = map_correo_a_id[rel_corr['correo']]
        rel_corr['id_persona'] = map_identificacion_a_id[rel_corr.pop('identificacion')]

    # ============================================================
    # PASO 5: Insertar por LOTES en BigQuery (4-6 consultas)
    # ============================================================
    
    # 5.1 Insertar personas NUEVAS (una sola consulta)
    if personas_nuevas:
        valores_personas = [f"('{p['id_persona']}', '{p['identificacion']}', '{p['nombre']}')" for p in personas_nuevas]
        insert_personas = f"""
            INSERT INTO `proyecto-css-panama.cobranza.personas`
            (id_persona, identificacion, nombre)
            VALUES {', '.join(valores_personas)}
        """
        ejecutar_query(insert_personas)

    # 5.2 Insertar cuentas (una sola consulta)
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
                {f"'{c['empresa']}'" if c['empresa'] else 'NULL'},
                {f"'{c['direccion']}'" if c['direccion'] else 'NULL'},
                {f"'{c['ocupacion']}'" if c['ocupacion'] else 'NULL'},
                {c['dias_mora'] if c['dias_mora'] is not None else 'NULL'},
                {f"'{c['cartera']}'" if c['cartera'] else 'NULL'},
                {f"'{c['observaciones']}'" if c['observaciones'] else 'NULL'}
            )""")
        
        if valores_cuentas:
            insert_cuentas = f"""
                INSERT INTO `proyecto-css-panama.cobranza.cuentas`
                (id_cuenta, id_persona, id_proyecto, cuenta, obligacion, saldo, 
                 fecha_ultimo_pago, empresa, direccion, ocupacion, dias_mora, cartera, observaciones)
                VALUES {', '.join(valores_cuentas)}
            """
            ejecutar_query(insert_cuentas)

    # 5.3 Insertar teléfonos NUEVOS (una sola consulta)
    if telefonos_nuevos:
        valores_telefonos = [f"('{t['id_telefono']}', '{t['numero']}')" for t in telefonos_nuevos]
        insert_telefonos = f"""
            INSERT INTO `proyecto-css-panama.cobranza.telefonos`
            (id_telefono, numero)
            VALUES {', '.join(valores_telefonos)}
        """
        ejecutar_query(insert_telefonos)

    # 5.4 Insertar relaciones telefonos_proyecto (una sola consulta)
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

    # 5.5 Insertar correos NUEVOS (una sola consulta)
    if correos_nuevos:
        valores_correos = [f"('{c['id_correo']}', '{c['correo']}')" for c in correos_nuevos]
        insert_correos = f"""
            INSERT INTO `proyecto-css-panama.cobranza.correos`
            (id_correo, correo)
            VALUES {', '.join(valores_correos)}
        """
        ejecutar_query(insert_correos)

    # 5.6 Insertar relaciones correos_proyecto (una sola consulta)
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

    # ============================================================
    # PASO 6: Resultado
    # ============================================================
    
    procesados = total - errores
    elapsed_time = time.time() - start_time
    detalle = f"{procesados} procesados, {errores} errores. Tiempo: {elapsed_time:.2f}s"
    if detalles:
        detalle += f" | Primeros errores: {', '.join(detalles[:3])}"
    
    return total, procesados, errores, detalle# ============================================================
# GENERAR PLANTILLA
# ============================================================

def generar_plantilla():
    """Genera un archivo Excel con el formato oficial de Hexagon"""
    # Crear DataFrame con columnas y ejemplos
    data = {
        'identificacion': ['8-123-456', '8-789-012', '1-234-567'],
        'nombre': ['JUAN PEREZ GONZALEZ', 'MARIA LOPEZ', 'CARLOS RUIZ'],
        'cuenta': ['001-123456-7', '002-789012-3', '003-345678-9'],
        'obligacion': ['HIP-98765', '', 'TAR-001'],
        'saldo': [1250.00, 850.50, 3200.00],
        'telefono': ['61234567, 67891234', '69998877', '63322110, 65544332, 67788990'],
        'correo': ['juan@gmail.com', 'maria@hotmail.com', 'carlos@gmail.com, carlos@trabajo.com'],
        'empresa': ['INMOBILIARIA DON ANTONIO, S.A.', '', 'CORP. NIKOS CAFE'],
        'direccion': ['CALLE 5, PANAMÁ', '', 'VIA ESPAÑA, PANAMÁ'],
        'ocupacion': ['CONDUCTOR', '', 'GERENTE'],
        'fecha_ultimo_pago': ['2026-06-01', '2026-05-15', '2026-04-30'],
        'dias_mora': [30, 45, 60],
        'cartera': ['PREDEMANDA', 'INCOBRABLE', 'PREDEMANDA'],
        'observaciones': ['Promesa de pago para el 15/08', '', 'Cliente con orden de descuento']
    }
    
    df = pd.DataFrame(data)
    
    # Crear Excel con dos hojas: datos e instrucciones
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Carga', index=False)
        
        # Hoja de instrucciones
        instrucciones = pd.DataFrame({
            'Instrucciones': [
                'FORMATO DE CARGA HEXAGON - COBRANZA',
                '',
                '📌 COLUMNAS OBLIGATORIAS (deben tener datos):',
                '  • identificacion: Cédula o identificación del cliente',
                '  • nombre: Nombre completo del cliente',
                '  • cuenta: Número de cuenta o préstamo',
                '  • saldo: Monto de la deuda (número)',
                '',
                '📌 COLUMNAS OPCIONALES:',
                '  • obligacion: Identificador adicional de la obligación',
                '  • telefono: Todos los teléfonos separados por coma (ej: 61234567, 67891234)',
                '  • correo: Todos los correos separados por coma',
                '  • empresa: Empresa donde labora',
                '  • direccion: Dirección del cliente',
                '  • ocupacion: Ocupación del cliente',
                '  • fecha_ultimo_pago: Última fecha de pago (YYYY-MM-DD)',
                '  • dias_mora: Días de mora (número)',
                '  • cartera: Tipo de cartera (ej: PREDEMANDA, INCOBRABLE)',
                '  • observaciones: Notas adicionales',
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
        
        # Ajustar ancho de columnas en la hoja de carga
        worksheet = writer.sheets['Carga']
        for i, col in enumerate(df.columns):
            worksheet.set_column(i, i, 20)
    
    return output.getvalue()

# ============================================================
# VISTA PRINCIPAL
# ============================================================

def render():
    """Punto de entrada para la vista de Carga de Cartera"""
    
    st.markdown("""
    <style>
        .main-header {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
        }
        .sub-header {
            font-size: 14px;
            color: #6b6b6b;
            margin-bottom: 24px;
        }
        .card {
            background-color: #ffffff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
            border: 1px solid #f0f0f0;
            margin-bottom: 16px;
        }
        .card-title {
            font-size: 16px;
            font-weight: 500;
            color: #1a1a1a;
            margin-bottom: 12px;
        }
        .upload-area {
            border: 2px dashed #d1d5db;
            border-radius: 12px;
            padding: 40px 24px;
            text-align: center;
            background-color: #fafafa;
            transition: border-color 0.2s;
        }
        .upload-area:hover {
            border-color: #dc2626;
            background-color: #fef2f2;
        }
        .upload-area .icon {
            font-size: 40px;
            color: #9ca3af;
            margin-bottom: 8px;
        }
        .upload-area .text-primary {
            font-size: 15px;
            font-weight: 500;
            color: #1a1a1a;
        }
        .upload-area .text-secondary {
            font-size: 13px;
            color: #6b6b6b;
        }
        .btn-primary {
            background-color: #dc2626;
            color: white;
            border: none;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
            width: 100%;
        }
        .btn-primary:hover {
            background-color: #b91c1c;
        }
        .btn-primary:disabled {
            background-color: #9ca3af;
            cursor: not-allowed;
        }
        .btn-outline {
            background-color: transparent;
            color: #dc2626;
            border: 1px solid #dc2626;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn-outline:hover {
            background-color: #fef2f2;
        }
        .status-success {
            color: #16a34a;
            font-weight: 500;
        }
        .status-warning {
            color: #ea580c;
            font-weight: 500;
        }
        .status-error {
            color: #dc2626;
            font-weight: 500;
        }
        .history-item {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #f3f4f6;
        }
        .history-item:last-child {
            border-bottom: none;
        }
        .history-date {
            color: #6b6b6b;
            font-size: 13px;
        }
        .history-count {
            font-weight: 500;
        }
        .selected-file {
            background-color: #f0fdf4;
            border: 1px solid #86efac;
            border-radius: 8px;
            padding: 12px 16px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .selected-file .file-name {
            font-weight: 500;
            color: #166534;
        }
        .selected-file .file-size {
            color: #6b6b6b;
            font-size: 13px;
        }
        .project-selector {
            margin-bottom: 16px;
        }
        .project-selector label {
            font-weight: 500;
            color: #1a1a1a;
            font-size: 14px;
        }
        .helper-text {
            font-size: 13px;
            color: #6b6b6b;
            margin-top: 4px;
        }
    </style>
    """, unsafe_allow_html=True)

    # ============================================================
    # HEADER
    # ============================================================
    
    st.markdown('<div class="main-header">📥 Carga de Cartera</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Sube el archivo con la cartera de clientes para procesar en Hexagon. El sistema validará, normalizará y distribuirá la información automáticamente.</div>', unsafe_allow_html=True)

    # ============================================================
    # CARD PRINCIPAL: PROYECTO + ARCHIVO
    # ============================================================
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # ---- Selector de Proyecto ----
    proyectos_df = obtener_proyectos_activos()
    
    if len(proyectos_df) == 0:
        st.warning("⚠️ No hay proyectos activos en el sistema. Contacta al administrador.")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Crear opciones para el selectbox
    opciones_proyectos = {row['nombre']: row['id_proyecto'] for _, row in proyectos_df.iterrows()}
    nombres_proyectos = list(opciones_proyectos.keys())
    
    st.markdown('<div class="project-selector">', unsafe_allow_html=True)
    proyecto_seleccionado_nombre = st.selectbox(
        "🏢 Proyecto",
        nombres_proyectos,
        index=0 if nombres_proyectos else None,
        help="Selecciona el proyecto al que pertenece esta cartera"
    )
    proyecto_seleccionado = opciones_proyectos.get(proyecto_seleccionado_nombre)
    st.markdown('<div class="helper-text">La cartera se asignará a este proyecto. Los clientes, cuentas y contactos se vincularán automáticamente.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ---- Botón Descargar Plantilla ----
    col1, col2 = st.columns([4, 1])
    with col2:
        plantilla_bytes = generar_plantilla()
        st.download_button(
            label="📄 Descargar Plantilla",
            data=plantilla_bytes,
            file_name="FORMATO_CARGA_CARTERA_HEXAGON.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    # ============================================================
    # ÁREA DE SUBIDA DE ARCHIVO (SIMPLIFICADA)
    # ============================================================
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
        key="carga_cartera_uploader"
    )
    
    # ---- Mostrar archivo seleccionado ----
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

    # ============================================================
    # PROCESAMIENTO DEL ARCHIVO
    # ============================================================
    
    if uploaded_file is not None:
        with st.spinner("📊 Procesando archivo..."):
            try:
                df = leer_excel(uploaded_file)
                
                faltantes = validar_columnas(df, COLUMNAS_REQUERIDAS)
                
                if faltantes:
                    st.error(f"⚠️ Faltan columnas obligatorias: {', '.join(faltantes)}")
                    st.stop()
                
                # Vista previa
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.markdown('<div class="card-title">📊 Vista previa del archivo</div>', unsafe_allow_html=True)
                
                st.dataframe(df.head(10), use_container_width=True)
                
                # Estadísticas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total registros", f"{len(df):,}")
                with col2:
                    st.metric("Teléfonos", f"{df['telefono'].notna().sum() if 'telefono' in df.columns else 0:,}")
                with col3:
                    st.metric("Correos", f"{df['correo'].notna().sum() if 'correo' in df.columns else 0:,}")
                with col4:
                    st.metric("Empresas", f"{df['empresa'].notna().sum() if 'empresa' in df.columns else 0:,}")
                
                # Botón Procesar
                if st.button("🚀 Procesar carga", type="primary", use_container_width=True):
                    with st.spinner("🔄 Procesando carga en BigQuery..."):
                        total, procesados, errores, detalle = procesar_carga(df, proyecto_seleccionado)
                        
                        estado = "completada" if errores == 0 else "con_errores"
                        registrar_carga_en_bigquery(proyecto_seleccionado, total, procesados, errores, estado, detalle)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📊 Total", f"{total:,}")
                        with col2:
                            st.metric("✅ Procesados", f"{procesados:,}", delta=f"{procesados/total*100:.1f}%")
                        with col3:
                            st.metric("❌ Errores", f"{errores:,}", delta=f"{-errores/total*100:.1f}%" if errores > 0 else "0%")
                        
                        if errores == 0:
                            st.success("🎉 Carga completada exitosamente. Todos los registros fueron procesados.")
                        else:
                            st.warning(f"⚠️ Carga completada con {errores} errores. Revisa el detalle: {detalle}")
                        
                        if st.button("📊 Ver Dashboard", use_container_width=True):
                            st.session_state['pagina_actual'] = "Dashboard Cobranza"
                            st.rerun()
                
                st.markdown('</div>', unsafe_allow_html=True)

            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.exception(e)

    # ============================================================
    # HISTORIAL DE CARGAS
    # ============================================================
    
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

    # ============================================================
    # FOOTER
    # ============================================================
    
    st.markdown("""
    <div style="text-align: center; margin-top: 32px; font-size: 12px; color: #9ca3af; border-top: 1px solid #f0f0f0; padding-top: 16px;">
        Hexagon · Cobranza · Versión 1.0
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# EJECUCIÓN DIRECTA (para pruebas)
# ============================================================

if __name__ == "__main__":
    render()
