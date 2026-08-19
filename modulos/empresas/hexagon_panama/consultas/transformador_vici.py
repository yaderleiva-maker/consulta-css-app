"""
Transformador VICI → CRM
Motor genérico que procesa archivos VICI según configuración YAML
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union
import logging
import io
import streamlit as st

# ✅ IMPORTAR BIGQUERY CORRECTAMENTE
from google.cloud import bigquery
from google.oauth2 import service_account

# Servicios existentes
from services.archivos import leer_excel
from services.fechas import normalizar_fecha_vici, formatear_fecha_vtiger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bigquery_client():
    """
    Obtiene el cliente de BigQuery usando el mismo patrón que consultas.py
    """
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
        logger.error(f"❌ Error al conectar a BigQuery: {e}")
        raise


def ejecutar_query_bigquery(query):
    """Ejecuta una query en BigQuery"""
    try:
        client = get_bigquery_client()
        result = client.query(query).to_dataframe()
        return result
    except Exception as e:
        logger.error(f"❌ Error en BigQuery: {e}")
        raise


class TransformadorVICI:
    """Clase principal que orquesta la transformación"""
    
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
        ruta = Path(__file__).parent / "proyectos" / f"{proyecto}.yaml"
        if not ruta.exists():
            raise FileNotFoundError(f"Configuración no encontrada: {ruta}")
        
        with open(ruta, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        required = ['proyecto', 'vici', 'tipologia']
        for req in required:
            if req not in config:
                raise ValueError(f"Configuración incompleta: falta '{req}'")
        
        return config
    
    def cargar_vici(self, archivo) -> pd.DataFrame:
        """Carga archivo VICI"""
        if hasattr(archivo, 'read'):
            contenido = archivo.read()
            if isinstance(contenido, bytes):
                contenido = contenido.decode('utf-8')
            df = pd.read_csv(io.StringIO(contenido), sep=None, engine='python')
        elif isinstance(archivo, (str, Path)):
            df = pd.read_csv(archivo, sep=None, engine='python')
        elif isinstance(archivo, pd.DataFrame):
            df = archivo.copy()
        else:
            raise ValueError("Tipo de archivo no soportado")
        
        df.columns = df.columns.str.lower().str.strip()
        
        col_cuenta = self.config['vici']['cuenta']
        if col_cuenta not in df.columns:
            raise ValueError(f"Columna '{col_cuenta}' no encontrada en VICI")
        
        self.df_vici = df
        logger.info(f"VICI cargado: {len(df)} registros")
        self.logs.append(f"✅ VICI cargado: {len(df)} registros")
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
        self.logs.append(f"✅ CRM cargado: {len(df)} registros")
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
            self.logs.append("⚠️ No hay cuentas en el archivo VICI")
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
                    self.logs.append(f"⚠️ Error en validación de lote: {str(e)[:100]}")
                    continue
            
            self.df_vici['_valida'] = self.df_vici[col_cuenta].astype(str).isin(cuentas_validas)
            
            if not cuentas_validas and cuentas:
                try:
                    test_query = f"""
                        SELECT COUNT(*) as total
                        FROM `proyecto-css-panama.cobranza.cuentas`
                        WHERE proyecto = '{self.config['proyecto']}'
                        LIMIT 1
                    """
                    test_df = client.query(test_query).to_dataframe()
                    if test_df is not None and not test_df.empty:
                        self.logs.append("⚠️ Las cuentas no coinciden con la cartera")
                    else:
                        self.logs.append("⚠️ No se pudo verificar la tabla")
                except Exception as e:
                    self.logs.append(f"⚠️ Error al verificar tabla: {str(e)[:100]}")
                    self.df_vici['_valida'] = True
                    return self.df_vici
            
        except Exception as e:
            self.logs.append(f"⚠️ Error en validación: {str(e)[:100]}")
            self.logs.append("⚠️ Procesando todas las cuentas")
            self.df_vici['_valida'] = True
            return self.df_vici
        
        df_validas = self.df_vici[self.df_vici['_valida']].copy()
        df_invalidas = self.df_vici[~self.df_vici['_valida']].copy()
        
        for _, row in df_invalidas.iterrows():
            self.errores.append({
                'cuenta': row[col_cuenta],
                'motivo': 'Cuenta no encontrada en cartera'
            })
        
        self.logs.append(f"✅ Cuentas válidas: {len(df_validas)}")
        self.logs.append(f"⚠️ Cuentas inválidas: {len(df_invalidas)}")
        
        self.df_vici = df_validas.drop(columns=['_valida']) if not df_validas.empty else df_validas
        return self.df_vici
    
    def aplicar_tipologia(self):
        """Aplica el mapeo de códigos a resultados descriptivos"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_codigo = self.config['vici']['codigo_resultado']
        tipologia = self.config['tipologia']
        
        self.df_vici['_resultado_desc'] = self.df_vici[col_codigo].map(tipologia)
        self.df_vici['_resultado_desc'] = self.df_vici['_resultado_desc'].fillna(
            self.df_vici[col_codigo]
        )
        
        self.logs.append(f"✅ Tipología aplicada")
    
    def aplicar_reglas(self):
        """Aplica reglas de negocio específicas del proyecto"""
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
            self.logs.append(f"✅ Mapeo de asesores aplicado")
        
        if 'campo_proyecto' in reglas:
            campo_conf = reglas['campo_proyecto']
            col_cuenta = config['vici']['cuenta']
            self.df_vici['_campo_proyecto'] = self.df_vici[col_cuenta].apply(
                lambda x: campo_conf['formato'].format(
                    proyecto=campo_conf['nombre'],
                    cuenta=x
                ) if pd.notna(x) else ''
            )
            self.logs.append(f"✅ Campo proyecto creado")
        
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
            self.logs.append(f"✅ Comentario generado")
    
    def enriquecer_con_crm(self):
        """Enriquece los datos con información del CRM"""
        if self.df_crm is None:
            self.logs.append("⚠️ CRM no cargado, usando valores por defecto")
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
            self.logs.append("⚠️ No se encontró columna de cuenta en CRM")
            self._aplicar_fallbacks()
            return
        
        columnas_crm = [col_cuenta_crm]
        
        col_fecha_repro = None
        for posible in ['fecha de reprogramación', 'fecha repro', 'fecha_reprogramacion']:
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
        
        if col_fecha_repro and 'fecha de reprogramación' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_fecha_repro: 'fecha de reprogramación'}, inplace=True)
        if col_estado and 'estado de la cuenta' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_estado: 'estado de la cuenta'}, inplace=True)
        
        self._aplicar_fallbacks()
        self.logs.append(f"✅ CRM enriquecido")
    
    def _aplicar_fallbacks(self):
        """Aplica valores por defecto cuando faltan datos del CRM"""
        fallbacks = self.config.get('fallbacks', {})
        
        # Asegurar que la columna existe
        if 'fecha de reprogramación' not in self.df_vici.columns:
            self.df_vici['fecha de reprogramación'] = pd.NaT
        
        # ✅ CONVERTIR A DATETIME ANTES DE ASIGNAR
        if 'fecha de reprogramación' in self.df_vici.columns:
            # Asegurar que la columna es datetime
            self.df_vici['fecha de reprogramación'] = pd.to_datetime(
                self.df_vici['fecha de reprogramación'], 
                errors='coerce'
            )
            
            # Asegurar que '_fecha_normalizada' es datetime
            if '_fecha_normalizada' in self.df_vici.columns:
                self.df_vici['_fecha_normalizada'] = pd.to_datetime(
                    self.df_vici['_fecha_normalizada'],
                    errors='coerce'
                )
                
                # Aplicar fallback solo donde es nulo
                mask = self.df_vici['fecha de reprogramación'].isna()
                if mask.any():
                    self.df_vici.loc[mask, 'fecha de reprogramación'] = (
                        self.df_vici.loc[mask, '_fecha_normalizada'] + pd.Timedelta(days=1)
                    )
        
        # Estado de cuenta
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
        
        self.logs.append(f"✅ Fechas normalizadas")
    
    def generar_csv_vtiger(self) -> pd.DataFrame:
        """Genera el DataFrame final en formato VTIGER"""
        if self.df_vici is None or self.df_vici.empty:
            raise ValueError("No hay datos para generar CSV")
        
        config = self.config
        col_cuenta = config['vici']['cuenta']
        col_telefono = config['vici']['telefono']
        col_asesor = config['vici']['asesor']
        proyecto_nombre = config['proyecto'].upper()
        
        # ✅ ASEGURAR QUE LAS FECHAS SON STRING PARA EL CSV
        df_final = pd.DataFrame({
            'Created At': self.df_vici['created_at'].apply(
                lambda x: x.strftime('%m/%d/%Y %H:%M:%S') if pd.notna(x) else ''
            ),
            'Fecha de Reprogramación': self.df_vici['fecha de reprogramación'].apply(
                lambda x: x.strftime('%m/%d/%Y %H:%M:%S') if pd.notna(x) else ''
            ),
            'Assigned To': self.df_vici[col_asesor].astype(str),
            'Num de Contacto': self.df_vici[col_telefono].astype(str),
            'Cuenta': self.df_vici[col_cuenta].astype(str),
            proyecto_nombre: self.df_vici['_campo_proyecto'].astype(str),
            'Resultado de la llamada': self.df_vici['_resultado_desc'].astype(str),
            'Comentario': self.df_vici['_comentario'].astype(str),
            'Estado de la cuenta': self.df_vici['estado de la cuenta'].astype(str),
        })
        
        df_final = df_final.fillna('')
        self.resultados = df_final.to_dict('records')
        
        self.logs.append(f"✅ CSV generado: {len(df_final)} registros")
        return df_final
    
    def procesar(self, archivo_vici, archivo_crm=None, validar=False) -> pd.DataFrame:
        """Pipeline completo de transformación"""
        self.logs = []
        self.errores = []
        
        self.logs.append(f"🚀 Iniciando transformación para: {self.proyecto.upper()}")
        
        self.cargar_vici(archivo_vici)
        
        if self.df_vici.empty:
            self.logs.append("⚠️ No hay datos en el archivo VICI")
            return pd.DataFrame()
        
        if validar:
            self.logs.append("🔍 Validando cuentas contra BigQuery...")
            self.validar_cuentas()
            if self.df_vici.empty:
                self.logs.append("⚠️ No hay cuentas válidas para procesar")
                return pd.DataFrame()
        else:
            self.logs.append("⏭️ Validación omitida (se procesan todas las cuentas)")
        
        self.normalizar_fechas()
        self.aplicar_tipologia()
        
        if archivo_crm:
            self.cargar_crm(archivo_crm)
        
        self.enriquecer_con_crm()
        self.aplicar_reglas()
        
        df_final = self.generar_csv_vtiger()
        
        if self.errores:
            self.logs.append(f"⚠️ {len(self.errores)} cuentas no fueron procesadas")
        
        self.logs.append("✅ Transformación completada exitosamente")
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
# FUNCIÓN PARA STREAMLIT
# =====================

def render_transformador_vici():
    """Función que se llama desde app.py"""
    
    st.title("🔄 Transformador VICI → CRM")
    st.caption("Convierte archivos de VICI al formato que espera VTIGER")
    
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        proyectos_dir = Path(__file__).parent / "proyectos"
        proyectos_disponibles = []
        for yaml_file in proyectos_dir.glob("*.yaml"):
            with open(yaml_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                proyectos_disponibles.append({
                    'nombre': config.get('proyecto', yaml_file.stem),
                    'archivo': yaml_file.stem,
                })
        
        if not proyectos_disponibles:
            st.error("❌ No hay proyectos configurados")
            return
        
        proyecto_seleccionado = st.selectbox(
            "Proyecto",
            options=[p['archivo'] for p in proyectos_disponibles],
            format_func=lambda x: next((p['nombre'].upper() for p in proyectos_disponibles if p['archivo'] == x), x)
        )
        
        validar = st.checkbox(
            "✅ Validar cuentas contra BigQuery",
            value=False,
            help="⚠️ Solo activar si tienes permisos a la tabla cobranza.cuentas"
        )
        
        st.divider()
        st.caption("📌 El archivo VICI debe tener columnas:")
        st.caption("• city (cuenta)")
        st.caption("• phone_number (teléfono)")
        st.caption("• user (asesor)")
        st.caption("• modify_date (fecha)")
        st.caption("• status (código de gestión)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📁 Archivo VICI (.txt)")
        archivo_vici = st.file_uploader(
            "Subir archivo de VICI",
            type=['txt', 'csv'],
            key="vici_upload"
        )
        if archivo_vici:
            st.success(f"✅ {archivo_vici.name} - {archivo_vici.size} bytes")
    
    with col2:
        st.subheader("📊 Archivo CRM (.xlsx)")
        archivo_crm = st.file_uploader(
            "Subir archivo de CRM (opcional)",
            type=['xlsx', 'xls'],
            key="crm_upload"
        )
        if archivo_crm:
            st.success(f"✅ {archivo_crm.name} - {archivo_crm.size} bytes")
    
    if st.button("🚀 Transformar", type="primary", use_container_width=True):
        if not archivo_vici:
            st.error("❌ Debes subir al menos el archivo VICI")
            st.stop()
        
        with st.spinner("🔄 Procesando..."):
            try:
                transformer = TransformadorVICI(proyecto_seleccionado)
                df_resultado = transformer.procesar(
                    archivo_vici=archivo_vici,
                    archivo_crm=archivo_crm if archivo_crm else None,
                    validar=validar
                )
                
                if df_resultado.empty:
                    st.warning("⚠️ No se generaron datos. Revisa el log.")
                else:
                    st.success(f"✅ Transformación completada: {len(df_resultado)} registros")
                
                tab1, tab2, tab3 = st.tabs(["📊 Datos", "📋 Log", "📥 Descargar"])
                
                with tab1:
                    if not df_resultado.empty:
                        st.dataframe(df_resultado, use_container_width=True, height=400)
                        st.caption(f"Total: {len(df_resultado)} registros, {len(df_resultado.columns)} columnas")
                    else:
                        st.info("No hay datos para mostrar")
                
                with tab2:
                    log = transformer.obtener_log()
                    
                    if log['errores']:
                        st.warning(f"⚠️ {len(log['errores'])} errores encontrados")
                        with st.expander("Ver errores"):
                            st.json(log['errores'][:50])
                    else:
                        st.info("✅ No se encontraron errores")
                    
                    if log['logs']:
                        st.subheader("📝 Detalle del proceso")
                        for msg in log['logs']:
                            st.text(msg)
                    
                    st.json({
                        'proyecto': log['proyecto'],
                        'total_registros': log['total_registros']
                    })
                
                with tab3:
                    if not df_resultado.empty:
                        csv = df_resultado.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📥 Descargar CSV para VTIGER",
                            data=csv,
                            file_name=f"{proyecto_seleccionado.upper()}_VICI_transformado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        st.caption("📌 Este CSV está listo para importar en VTIGER")
                    else:
                        st.info("No hay datos para descargar")
                
            except Exception as e:
                st.error(f"❌ Error durante la transformación: {str(e)}")
                st.exception(e)
