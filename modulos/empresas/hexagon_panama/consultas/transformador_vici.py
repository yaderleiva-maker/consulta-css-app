"""
Transformador VICI -> CRM
Motor generico que procesa archivos VICI segun configuracion YAML
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union
import logging
import io
import streamlit as st

# BigQuery
from google.cloud import bigquery
from google.oauth2 import service_account

# Servicios existentes
try:
    from services.archivos import leer_excel
except ImportError:
    leer_excel = None

try:
    from services.fechas import normalizar_fecha_vici, formatear_fecha_vtiger
except ImportError:
    # Funciones de respaldo
    def normalizar_fecha_vici(valor):
        if pd.isna(valor):
            return pd.NaT
        if isinstance(valor, (pd.Timestamp, datetime)):
            return pd.Timestamp(valor)
        try:
            return pd.to_datetime(valor)
        except:
            return pd.NaT
    
    def formatear_fecha_vtiger(fecha):
        if pd.isna(fecha):
            return ''
        if isinstance(fecha, (pd.Timestamp, datetime)):
            return fecha.strftime('%d/%m/%Y')
        return str(fecha)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bigquery_client():
    """Obtiene el cliente de BigQuery"""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        return client
    except Exception as e:
        logger.error(f"Error al conectar a BigQuery: {e}")
        raise


class TransformadorVICI:
    """Clase principal que orquesta la transformacion"""
    
    def __init__(self, proyecto: str):
        self.proyecto = proyecto
        self.config = self._cargar_configuracion(proyecto)
        self.df_vici = None
        self.df_crm = None
        self.resultados = []
        self.errores = []
        self.logs = []
        self._client = None
        
    def _get_client(self):
        if self._client is None:
            self._client = get_bigquery_client()
        return self._client
    
    def _cargar_configuracion(self, proyecto: str) -> Dict:
        """Carga el YAML del proyecto"""
        ruta = Path(__file__).parent / "proyectos" / f"{proyecto}.yaml"
        if not ruta.exists():
            raise FileNotFoundError(f"Configuracion no encontrada: {ruta}")
        
        with open(ruta, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        required = ['proyecto', 'vici', 'tipologia']
        for req in required:
            if req not in config:
                raise ValueError(f"Configuracion incompleta: falta '{req}'")
        
        return config
    
    def cargar_vici(self, archivo) -> pd.DataFrame:
        """Carga archivo VICI con manejo especial para 'NA'"""
        if hasattr(archivo, 'read'):
            contenido = archivo.read()
            if isinstance(contenido, bytes):
                contenido = contenido.decode('utf-8')
            
            # Leer todo como string para evitar que NA se convierta a NaN
            df = pd.read_csv(
                io.StringIO(contenido), 
                sep=None, 
                engine='python',
                dtype=str,
                keep_default_na=False,
                na_values=['']
            )
        elif isinstance(archivo, (str, Path)):
            df = pd.read_csv(
                archivo, 
                sep=None, 
                engine='python',
                dtype=str,
                keep_default_na=False,
                na_values=['']
            )
        elif isinstance(archivo, pd.DataFrame):
            df = archivo.copy()
        else:
            raise ValueError("Tipo de archivo no soportado")
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.lower().str.strip()
        
        # NORMALIZAR LA COLUMNA DE RESULTADO (status)
        col_codigo = self.config['vici']['codigo_resultado']
        if col_codigo in df.columns:
            # Limpiar valores: eliminar espacios, convertir a string, mayúsculas
            df[col_codigo] = df[col_codigo].astype(str).str.strip().str.upper()
            # Reemplazar valores vacíos o 'nan' por None
            df[col_codigo] = df[col_codigo].replace(['nan', 'None', ''], None)
            
            # CONTAR VALORES ÚNICOS PARA DEBUG
            valores_unicos = df[col_codigo].unique().tolist()
            self.logs.append(f"Valores unicos en '{col_codigo}': {valores_unicos[:20]}")
            
            # VERIFICAR SI 'NA' ESTÁ EN LOS VALORES
            if 'NA' in valores_unicos:
                self.logs.append("'NA' encontrado en la columna status")
            else:
                self.logs.append("'NA' NO encontrado en la columna status")
        
        # Normalizar columna de asesor
        col_asesor = self.config['vici']['asesor']
        if col_asesor in df.columns:
            df[col_asesor] = df[col_asesor].astype(str).str.strip()
            df[col_asesor] = df[col_asesor].replace(['nan', 'None', ''], None)
        
        # Validar columna de cuenta
        col_cuenta = self.config['vici']['cuenta']
        if col_cuenta not in df.columns:
            raise ValueError(f"Columna '{col_cuenta}' no encontrada en VICI")
        
        self.df_vici = df
        logger.info(f"VICI cargado: {len(df)} registros")
        self.logs.append(f"VICI cargado: {len(df)} registros")
        return df
    
    def cargar_crm(self, archivo) -> pd.DataFrame:
        """Carga archivo CRM (Excel)"""
        if hasattr(archivo, 'read'):
            df = pd.read_excel(archivo)
        elif isinstance(archivo, (str, Path)):
            df = pd.read_excel(archivo)
        elif isinstance(archivo, pd.DataFrame):
            df = archivo.copy()
        else:
            raise ValueError("Tipo de archivo CRM no soportado")
        
        df.columns = df.columns.str.lower().str.strip()
        self.df_crm = df
        logger.info(f"CRM cargado: {len(df)} registros")
        self.logs.append(f"CRM cargado: {len(df)} registros")
        return df
    
    def validar_cuentas(self) -> pd.DataFrame:
        """Valida cuentas contra BigQuery"""
        if self.df_vici is None:
            raise ValueError("Primero cargar VICI")
        
        if self.df_vici.empty:
            return self.df_vici
        
        col_cuenta = self.config['vici']['cuenta']
        cuentas = self.df_vici[col_cuenta].dropna().astype(str).unique().tolist()
        
        if not cuentas:
            self.logs.append("No hay cuentas en el archivo VICI")
            self.df_vici['_valida'] = False
            return self.df_vici
        
        try:
            client = self._get_client()
            lote_size = 500
            cuentas_validas = set()
            
            for i in range(0, len(cuentas), lote_size):
                lote = cuentas[i:i+lote_size]
                cuentas_str = "', '".join([str(c).replace("'", "''") for c in lote])
                query = f"""
                    SELECT DISTINCT 
                        CAST(numero_cuenta AS STRING) as cuenta
                    FROM `proyecto-css-panama.cobranza.cuentas`
                    WHERE proyecto = '{self.config['proyecto']}'
                      AND CAST(numero_cuenta AS STRING) IN ('{cuentas_str}')
                """
                try:
                    df_existentes = client.query(query).to_dataframe()
                    if 'cuenta' in df_existentes.columns and not df_existentes.empty:
                        cuentas_validas.update(df_existentes['cuenta'].astype(str).tolist())
                except Exception as e:
                    self.logs.append(f"Error en validacion de lote: {str(e)[:100]}")
                    continue
            
            self.df_vici['_valida'] = self.df_vici[col_cuenta].astype(str).isin(cuentas_validas)
            
        except Exception as e:
            self.logs.append(f"Error en validacion: {str(e)[:100]}")
            self.logs.append("Procesando todas las cuentas")
            self.df_vici['_valida'] = True
            return self.df_vici
        
        df_validas = self.df_vici[self.df_vici['_valida']].copy()
        df_invalidas = self.df_vici[~self.df_vici['_valida']].copy()
        
        for _, row in df_invalidas.iterrows():
            self.errores.append({
                'cuenta': row[col_cuenta],
                'motivo': 'Cuenta no encontrada en cartera'
            })
        
        self.logs.append(f"Cuentas validas: {len(df_validas)}")
        self.logs.append(f"Cuentas invalidas: {len(df_invalidas)}")
        
        self.df_vici = df_validas.drop(columns=['_valida']) if not df_validas.empty else df_validas
        return self.df_vici
    
    def aplicar_tipologia(self):
        """Aplica el mapeo de codigos a resultados descriptivos Y FILTRA"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_codigo = self.config['vici']['codigo_resultado']
        tipologia = self.config['tipologia']
        
        # DEBUG: Mostrar valores antes del mapeo
        valores_antes = self.df_vici[col_codigo].unique().tolist()
        self.logs.append(f"Valores ANTES del mapeo: {valores_antes[:20]}")
        
        # FILTRAR: Solo registrar códigos que están en la tipología
        codigos_validos = list(tipologia.keys())
        registros_originales = len(self.df_vici)
        
        # Filtrar solo los códigos válidos
        self.df_vici = self.df_vici[self.df_vici[col_codigo].isin(codigos_validos)].copy()
        
        registros_filtrados = len(self.df_vici)
        eliminados = registros_originales - registros_filtrados
        
        self.logs.append(f"Registros originales: {registros_originales}")
        self.logs.append(f"Registros validos (con tipologia): {registros_filtrados}")
        self.logs.append(f"Registros eliminados (sin tipologia): {eliminados}")
        
        if self.df_vici.empty:
            self.logs.append("No hay registros validos para procesar")
            return
        
        # Aplicar el mapeo
        self.df_vici['_resultado_desc'] = self.df_vici[col_codigo].map(tipologia)
        
        # DEBUG: Mostrar cuántos quedaron sin mapear
        no_mapeados = self.df_vici['_resultado_desc'].isna().sum()
        self.logs.append(f"Registros sin mapeo: {no_mapeados}")
        
        # DEBUG: Mostrar valores que no se mapearon
        if no_mapeados > 0:
            valores_sin_mapeo = self.df_vici[self.df_vici['_resultado_desc'].isna()][col_codigo].unique().tolist()
            self.logs.append(f"Valores sin mapeo: {valores_sin_mapeo}")
        
        # Rellenar los que no tienen mapeo con el código original
        self.df_vici['_resultado_desc'] = self.df_vici['_resultado_desc'].fillna(
            self.df_vici[col_codigo]
        )
        
        self.logs.append("Tipologia aplicada")
    
    def aplicar_reglas(self):
        """Aplica reglas de negocio especificas del proyecto"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        config = self.config
        reglas = config.get('reglas', {})
        
        if 'asesores' in reglas:
            asesores_map = reglas['asesores']
            col_asesor = config['vici']['asesor']
            self.df_vici[col_asesor] = self.df_vici[col_asesor].map(
                lambda x: asesores_map.get(x, x) if pd.notna(x) else x
            )
            self.logs.append("Mapeo de asesores aplicado")
        
        if 'campo_proyecto' in reglas:
            campo_conf = reglas['campo_proyecto']
            col_cuenta = config['vici']['cuenta']
            self.df_vici['_campo_proyecto'] = self.df_vici[col_cuenta].apply(
                lambda x: campo_conf['formato'].format(
                    proyecto=campo_conf['nombre'],
                    cuenta=x
                ) if pd.notna(x) else ''
            )
            self.logs.append("Campo proyecto creado")
        
        if 'comentario' in reglas:
            formato = reglas['comentario']['formato']
            col_telefono = config['vici']['telefono']
            self.df_vici['_comentario'] = self.df_vici.apply(
                lambda row: formato.format(
                    telefono=row[col_telefono],
                    resultado=row['_resultado_desc']
                ) if pd.notna(row[col_telefono]) and pd.notna(row['_resultado_desc']) else '',
                axis=1
            )
            self.logs.append("Comentario generado")
    
    def enriquecer_con_crm(self):
        """Enriquece los datos con informacion del CRM"""
        if self.df_crm is None:
            self.logs.append("CRM no cargado, usando valores por defecto")
            self._aplicar_fallbacks()
            return
        
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_cuenta = self.config['vici']['cuenta']
        crm_cols = {col.lower(): col for col in self.df_crm.columns}
        
        col_cuenta_crm = None
        for posible in ['cuenta', 'numero cuenta', 'num_cuenta', 'account']:
            if posible in crm_cols:
                col_cuenta_crm = crm_cols[posible]
                break
        
        if col_cuenta_crm is None:
            self.logs.append("No se encontro columna de cuenta en CRM")
            self._aplicar_fallbacks()
            return
        
        columnas_crm = [col_cuenta_crm]
        
        col_fecha_repro = None
        for posible in ['fecha de reprogramacion', 'fecha repro', 'fecha_reprogramacion']:
            if posible in crm_cols:
                col_fecha_repro = crm_cols[posible]
                columnas_crm.append(col_fecha_repro)
                break
        
        col_estado = None
        for posible in ['estado de la cuenta', 'estado cuenta', 'estado']:
            if posible in crm_cols:
                col_estado = crm_cols[posible]
                columnas_crm.append(col_estado)
                break
        
        self.df_vici = self.df_vici.merge(
            self.df_crm[columnas_crm],
            left_on=col_cuenta,
            right_on=col_cuenta_crm,
            how='left'
        )
        
        if col_fecha_repro and 'fecha de reprogramacion' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_fecha_repro: 'fecha de reprogramacion'}, inplace=True)
        if col_estado and 'estado de la cuenta' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_estado: 'estado de la cuenta'}, inplace=True)
        
        self._aplicar_fallbacks()
        self.logs.append("CRM enriquecido")
    
    def _aplicar_fallbacks(self):
        """Aplica valores por defecto cuando faltan datos del CRM"""
        fallbacks = self.config.get('fallbacks', {})
        
        if 'fecha de reprogramacion' not in self.df_vici.columns:
            self.df_vici['fecha de reprogramacion'] = pd.NaT
        
        if 'fecha de reprogramacion' in self.df_vici.columns:
            self.df_vici['fecha de reprogramacion'] = pd.to_datetime(
                self.df_vici['fecha de reprogramacion'], 
                errors='coerce'
            )
            
            if '_fecha_normalizada' in self.df_vici.columns:
                self.df_vici['_fecha_normalizada'] = pd.to_datetime(
                    self.df_vici['_fecha_normalizada'],
                    errors='coerce'
                )
                
                mask = self.df_vici['fecha de reprogramacion'].isna()
                if mask.any():
                    self.df_vici.loc[mask, 'fecha de reprogramacion'] = (
                        self.df_vici.loc[mask, '_fecha_normalizada'] + pd.Timedelta(days=1)
                    )
        
        if 'estado de la cuenta' not in self.df_vici.columns:
            self.df_vici['estado de la cuenta'] = fallbacks.get('estado_cuenta', 'No contacto')
        else:
            self.df_vici['estado de la cuenta'] = self.df_vici['estado de la cuenta'].fillna(
                fallbacks.get('estado_cuenta', 'No contacto')
            )
    
    def normalizar_fechas(self):
        """Normaliza fechas del VICI"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_fecha = self.config['vici']['fecha_gestion']
        self.df_vici['_fecha_normalizada'] = self.df_vici[col_fecha].apply(
            normalizar_fecha_vici
        )
        self.df_vici['created_at'] = self.df_vici['_fecha_normalizada']
        
        self.logs.append("Fechas normalizadas")
    
    def generar_csv_vtiger(self) -> pd.DataFrame:
        """Genera el DataFrame final en formato VTIGER"""
        if self.df_vici is None or self.df_vici.empty:
            raise ValueError("No hay datos para generar CSV")
        
        config = self.config
        col_cuenta = config['vici']['cuenta']
        col_telefono = config['vici']['telefono']
        col_asesor = config['vici']['asesor']
        proyecto_nombre = config['proyecto'].upper()
        
        def formatear_fecha_access(fecha):
            if pd.isna(fecha):
                return ''
            if isinstance(fecha, (pd.Timestamp, datetime)):
                return fecha.strftime('%-d/%-m/%Y')
            return str(fecha)
        
        def formatear_fecha_created_at(fecha):
            if pd.isna(fecha):
                return ''
            if isinstance(fecha, (pd.Timestamp, datetime)):
                return fecha.strftime('%Y-%m-%d %H:%M:%S')
            return str(fecha)
        
        df_final = pd.DataFrame({
            'Created At': self.df_vici['created_at'].apply(formatear_fecha_created_at),
            'Fecha de Reprogramación': self.df_vici['fecha de reprogramacion'].apply(formatear_fecha_access),
            'Assigned To': self.df_vici[col_asesor].astype(str),
            'Num de Contacto': self.df_vici[col_telefono].astype(str),
            'Cuenta': self.df_vici[col_cuenta].astype(str),
            proyecto_nombre: self.df_vici['_campo_proyecto'].astype(str) if '_campo_proyecto' in self.df_vici.columns else '',
            'Resultado de la llamada': self.df_vici['_resultado_desc'].astype(str) if '_resultado_desc' in self.df_vici.columns else '',
            'Comentario': self.df_vici['_comentario'].astype(str) if '_comentario' in self.df_vici.columns else '',
            'Estado de la cuenta': self.df_vici['estado de la cuenta'].astype(str),
        })
        
        df_final = df_final.fillna('')
        self.resultados = df_final.to_dict('records')
        
        self.logs.append(f"CSV generado: {len(df_final)} registros")
        return df_final
    
    def procesar(self, archivo_vici, archivo_crm=None, validar=False) -> pd.DataFrame:
        """Pipeline completo de transformacion"""
        self.logs = []
        self.errores = []
        
        self.logs.append(f"Iniciando transformacion para: {self.proyecto.upper()}")
        self.logs.append(f"Usando configuracion: proyectos/{self.proyecto}.yaml")
        self.logs.append(f"Tipologia: {len(self.config['tipologia'])} codigos mapeados")
        
        self.cargar_vici(archivo_vici)
        
        if self.df_vici.empty:
            self.logs.append("No hay datos en el archivo VICI")
            return pd.DataFrame()
        
        if validar:
            self.logs.append("Validando cuentas contra BigQuery...")
            self.validar_cuentas()
            if self.df_vici.empty:
                self.logs.append("No hay cuentas validas para procesar")
                return pd.DataFrame()
        else:
            self.logs.append("Validacion omitida (se procesan todas las cuentas)")
        
        self.normalizar_fechas()
        self.aplicar_tipologia()
        
        if archivo_crm:
            self.cargar_crm(archivo_crm)
        
        self.enriquecer_con_crm()
        self.aplicar_reglas()
        
        df_final = self.generar_csv_vtiger()
        
        if self.errores:
            self.logs.append(f"{len(self.errores)} cuentas no fueron procesadas")
        
        self.logs.append("Transformacion completada exitosamente")
        return df_final
    
    def obtener_log(self) -> Dict:
        """Devuelve el log de procesamiento"""
        return {
            'total_registros': len(self.resultados),
            'errores': self.errores[:100],
            'proyecto': self.proyecto,
            'config': self.config.get('proyecto'),
            'logs': self.logs
        }


# =====================
# FUNCION PARA STREAMLIT
# =====================

def render_transformador_vici():
    """Funcion que se llama desde app.py"""
    
    st.title("Transformador VICI -> CRM")
    st.caption("Convierte archivos de VICI al formato que espera VTIGER")
    
    # ============================================================
    # SELECTOR DE PROYECTO EN EL ÁREA PRINCIPAL
    # ============================================================
    st.markdown("---")
    
    col_proy1, col_proy2, col_proy3 = st.columns([2, 2, 1])
    
    with col_proy1:
        # Cargar proyectos disponibles
        proyectos_dir = Path(__file__).parent / "proyectos"
        proyectos_disponibles = []
        for yaml_file in proyectos_dir.glob("*.yaml"):
            with open(yaml_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                proyectos_disponibles.append({
                    'nombre': config.get('proyecto', yaml_file.stem),
                    'archivo': yaml_file.stem,
                    'descripcion': config.get('descripcion', '')
                })
        
        if not proyectos_disponibles:
            st.error("No hay proyectos configurados")
            return
        
        proyecto_seleccionado = st.selectbox(
            "Selecciona el proyecto",
            options=[p['archivo'] for p in proyectos_disponibles],
            format_func=lambda x: next((p['nombre'].upper() for p in proyectos_disponibles if p['archivo'] == x), x),
            key="proyecto_selector"
        )
    
    with col_proy2:
        # Mostrar información del proyecto seleccionado
        proyecto_info = next((p for p in proyectos_disponibles if p['archivo'] == proyecto_seleccionado), None)
        if proyecto_info and proyecto_info.get('descripcion'):
            st.caption(f"📋 {proyecto_info['descripcion']}")
    
    with col_proy3:
        # Opción de validación
        validar = st.checkbox(
            "Validar cuentas",
            value=False,
            help="Verifica que las cuentas existan en BigQuery"
        )
    
    st.markdown("---")
    
    # ============================================================
    # ÁREA DE ARCHIVOS
    # ============================================================
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Archivo VICI (.txt)")
        archivo_vici = st.file_uploader(
            "Subir archivo de VICI",
            type=['txt', 'csv'],
            help="Archivo exportado de VICIDIAL",
            key="vici_upload"
        )
        if archivo_vici:
            st.success(f"{archivo_vici.name} - {archivo_vici.size} bytes")
    
    with col2:
        st.subheader("Archivo CRM (.xlsx)")
        archivo_crm = st.file_uploader(
            "Subir archivo de CRM (opcional)",
            type=['xlsx', 'xls'],
            help="Archivo con fechas de reprogramacion y estados",
            key="crm_upload"
        )
        if archivo_crm:
            st.success(f"{archivo_crm.name} - {archivo_crm.size} bytes")
    
    # ============================================================
    # BOTÓN DE PROCESAMIENTO
    # ============================================================
    
    if st.button("Transformar", type="primary", use_container_width=True):
        if not archivo_vici:
            st.error("Debes subir al menos el archivo VICI")
            st.stop()
        
        with st.spinner("Procesando..."):
            try:
                # Mostrar qué proyecto se está usando
                st.info(f"Procesando proyecto: {proyecto_seleccionado.upper()}")
                
                transformer = TransformadorVICI(proyecto_seleccionado)
                df_resultado = transformer.procesar(
                    archivo_vici=archivo_vici,
                    archivo_crm=archivo_crm if archivo_crm else None,
                    validar=validar
                )
                
                if df_resultado.empty:
                    st.warning("No se generaron datos. Revisa el log.")
                else:
                    st.success(f"Transformacion completada: {len(df_resultado)} registros")
                
                tab1, tab2, tab3 = st.tabs(["Datos", "Log", "Descargar"])
                
                with tab1:
                    if not df_resultado.empty:
                        st.dataframe(df_resultado, use_container_width=True, height=400)
                        st.caption(f"Total: {len(df_resultado)} registros, {len(df_resultado.columns)} columnas")
                    else:
                        st.info("No hay datos para mostrar")
                
                with tab2:
                    log = transformer.obtener_log()
                    
                    if log['errores']:
                        st.warning(f"{len(log['errores'])} errores encontrados")
                        with st.expander("Ver errores"):
                            st.json(log['errores'][:50])
                    else:
                        st.info("No se encontraron errores")
                    
                    if log['logs']:
                        st.subheader("Detalle del proceso")
                        for msg in log['logs']:
                            st.text(msg)
                    
                    st.json({
                        'proyecto': log['proyecto'],
                        'total_registros': log['total_registros']
                    })
                
                with tab3:
                    if not df_resultado.empty:
                        # CSV para VTIGER
                        csv = df_resultado.to_csv(index=False, encoding='utf-8')
                        st.download_button(
                            label="Descargar CSV para VTIGER",
                            data=csv,
                            file_name=f"{proyecto_seleccionado.upper()}_VICI_transformado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        # Excel para verificar
                        from io import BytesIO
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df_resultado.to_excel(writer, index=False, sheet_name='VICI Transformado')
                        excel_data = output.getvalue()
                        
                        st.download_button(
                            label="Descargar Excel para verificar",
                            data=excel_data,
                            file_name=f"{proyecto_seleccionado.upper()}_VICI_transformado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        
                        st.caption("Usa el CSV para importar en VTIGER. Usa Excel para verificar los datos.")
                    else:
                        st.info("No hay datos para descargar")
                
            except Exception as e:
                st.error(f"Error durante la transformacion: {str(e)}")
                st.exception(e)
