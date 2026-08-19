"""
Transformador VICI → CRM
Motor genérico que procesa archivos VICI según configuración YAML
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Union
import logging
import io

# Servicios existentes
from services.bigquery import ejecutar_query
from services.archivos import leer_excel
from services.fechas import normalizar_fecha_vici, formatear_fecha_vtiger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransformadorVICI:
    """Clase principal que orquesta la transformación"""
    
    def __init__(self, proyecto: str):
        """
        Args:
            proyecto: Nombre del proyecto (ej: 'jamar', 'sol', 'ifx')
        """
        self.proyecto = proyecto
        self.config = self._cargar_configuracion(proyecto)
        self.df_vici = None
        self.df_crm = None
        self.df_cartera = None
        self.resultados = []
        self.errores = []
        self.logs = []
        
    def _cargar_configuracion(self, proyecto: str) -> Dict:
        """Carga el YAML del proyecto"""
        ruta = Path(__file__).parent / "proyectos" / f"{proyecto}.yaml"
        if not ruta.exists():
            raise FileNotFoundError(f"Configuración no encontrada: {ruta}")
        
        with open(ruta, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Validar configuración mínima
        required = ['proyecto', 'vici', 'tipologia']
        for req in required:
            if req not in config:
                raise ValueError(f"Configuración incompleta: falta '{req}'")
        
        return config
    
    def cargar_vici(self, archivo) -> pd.DataFrame:
        """
        Carga archivo VICI (puede ser ruta, archivo subido o contenido)
        Detecta automáticamente separador
        """
        # Si es archivo subido (Streamlit)
        if hasattr(archivo, 'read'):
            contenido = archivo.read()
            if isinstance(contenido, bytes):
                contenido = contenido.decode('utf-8')
            df = pd.read_csv(io.StringIO(contenido), sep=None, engine='python')
        # Si es ruta
        elif isinstance(archivo, (str, Path)):
            df = pd.read_csv(archivo, sep=None, engine='python')
        # Si ya es DataFrame
        elif isinstance(archivo, pd.DataFrame):
            df = archivo.copy()
        else:
            raise ValueError("Tipo de archivo no soportado")
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.lower().str.strip()
        
        # Validar columnas requeridas
        col_cuenta = self.config['vici']['cuenta']
        if col_cuenta not in df.columns:
            raise ValueError(f"Columna '{col_cuenta}' no encontrada en VICI")
        
        self.df_vici = df
        logger.info(f"VICI cargado: {len(df)} registros, {len(df.columns)} columnas")
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
        """
        Valida que las cuentas existan en BigQuery (cartera)
        Filtra solo las que existen
        """
        if self.df_vici is None:
            raise ValueError("Primero cargar VICI")
        
        if self.df_vici.empty:
            logger.warning("DataFrame VICI vacío")
            return self.df_vici
        
        # Obtener mapeo de columna de cuenta
        col_cuenta = self.config['vici']['cuenta']
        
        # Limpiar y obtener cuentas únicas
        cuentas = self.df_vici[col_cuenta].dropna().astype(str).unique().tolist()
        
        if not cuentas:
            logger.warning("No hay cuentas en el archivo VICI")
            self.logs.append("⚠️ No hay cuentas en el archivo VICI")
            self.df_vici['_valida'] = False
            return self.df_vici
        
        # Construir query segura
        # Dividir en lotes para evitar consultas muy largas
        lote_size = 500
        cuentas_validas = set()
        
        for i in range(0, len(cuentas), lote_size):
            lote = cuentas[i:i+lote_size]
            cuentas_str = "', '".join([str(c).replace("'", "''") for c in lote])
            query = f"""
                SELECT DISTINCT 
                    CAST(numero_cuenta AS STRING) as cuenta
                FROM `hexagon-453418.cobranza.cuentas`
                WHERE proyecto = '{self.config['proyecto']}'
                  AND CAST(numero_cuenta AS STRING) IN ('{cuentas_str}')
            """
            try:
                df_existentes = ejecutar_query(query)
                cuentas_validas.update(df_existentes['cuenta'].astype(str).tolist())
            except Exception as e:
                logger.error(f"Error en validación de cuentas: {e}")
                self.logs.append(f"❌ Error en validación: {str(e)}")
                continue
        
        # Filtrar VICI
        self.df_vici['_valida'] = self.df_vici[col_cuenta].astype(str).isin(cuentas_validas)
        
        # Separar válidas e inválidas
        df_validas = self.df_vici[self.df_vici['_valida']].copy()
        df_invalidas = self.df_vici[~self.df_vici['_valida']].copy()
        
        # Guardar errores
        for _, row in df_invalidas.iterrows():
            self.errores.append({
                'cuenta': row[col_cuenta],
                'motivo': 'Cuenta no encontrada en cartera'
            })
        
        logger.info(f"Cuentas válidas: {len(df_validas)}, inválidas: {len(df_invalidas)}")
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
        
        # Si no tiene mapeo, queda el código original
        self.df_vici['_resultado_desc'] = self.df_vici['_resultado_desc'].fillna(
            self.df_vici[col_codigo]
        )
        
        logger.info(f"Tipología aplicada a {len(self.df_vici)} registros")
        self.logs.append(f"✅ Tipología aplicada")
    
    def aplicar_reglas(self):
        """Aplica reglas de negocio específicas del proyecto"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        config = self.config
        reglas = config.get('reglas', {})
        
        # 1. Mapeo de asesores
        if 'asesores' in reglas:
            asesores_map = reglas['asesores']
            col_asesor = config['vici']['asesor']
            self.df_vici[col_asesor] = self.df_vici[col_asesor].map(
                lambda x: asesores_map.get(x, x) if pd.notna(x) else x
            )
            self.logs.append(f"✅ Mapeo de asesores aplicado")
        
        # 2. Crear campo de proyecto (ej: JAMAR::::918020)
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
        
        # 3. Crear comentario
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
        """Enriquece los datos con información del CRM (fechas, estados)"""
        if self.df_crm is None:
            logger.warning("No hay CRM cargado, usando fallbacks")
            self.logs.append("⚠️ CRM no cargado, usando valores por defecto")
            self._aplicar_fallbacks()
            return
        
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_cuenta = config['vici']['cuenta']
        
        # Normalizar columnas del CRM
        crm_cols = {col.lower(): col for col in self.df_crm.columns}
        
        # Buscar columna de cuenta en CRM
        col_cuenta_crm = None
        for posible in ['cuenta', 'numero cuenta', 'num_cuenta', 'account', 'numero de cuenta']:
            if posible in crm_cols:
                col_cuenta_crm = crm_cols[posible]
                break
        
        if col_cuenta_crm is None:
            logger.warning("No se encontró columna de cuenta en CRM, usando fallbacks")
            self.logs.append("⚠️ No se encontró columna de cuenta en CRM")
            self._aplicar_fallbacks()
            return
        
        # Identificar columnas del CRM que nos interesan
        columnas_crm = [col_cuenta_crm]
        
        # Buscar fecha de reprogramación
        col_fecha_repro = None
        for posible in ['fecha de reprogramación', 'fecha repro', 'fecha_reprogramacion']:
            if posible in crm_cols:
                col_fecha_repro = crm_cols[posible]
                columnas_crm.append(col_fecha_repro)
                break
        
        # Buscar estado de cuenta
        col_estado = None
        for posible in ['estado de la cuenta', 'estado cuenta', 'estado']:
            if posible in crm_cols:
                col_estado = crm_cols[posible]
                columnas_crm.append(col_estado)
                break
        
        # Unir con CRM
        self.df_vici = self.df_vici.merge(
            self.df_crm[columnas_crm],
            left_on=col_cuenta,
            right_on=col_cuenta_crm,
            how='left'
        )
        
        # Renombrar columnas si es necesario
        if col_fecha_repro and 'fecha de reprogramación' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_fecha_repro: 'fecha de reprogramación'}, inplace=True)
        if col_estado and 'estado de la cuenta' not in self.df_vici.columns:
            self.df_vici.rename(columns={col_estado: 'estado de la cuenta'}, inplace=True)
        
        # Aplicar fallbacks donde no hay datos
        self._aplicar_fallbacks()
        
        logger.info(f"CRM enriquecido: {len(self.df_vici)} registros")
        self.logs.append(f"✅ CRM enriquecido")
    
    def _aplicar_fallbacks(self):
        """Aplica valores por defecto cuando faltan datos del CRM"""
        fallbacks = self.config.get('fallbacks', {})
        
        # Fecha de reprogramación
        if 'fecha de reprogramación' not in self.df_vici.columns:
            self.df_vici['fecha de reprogramación'] = pd.NaT
        
        # Si no hay fecha, usar created_at + 1 día
        if 'fecha de reprogramación' in self.df_vici.columns:
            mask = self.df_vici['fecha de reprogramación'].isna()
            if '_fecha_normalizada' in self.df_vici.columns:
                self.df_vici.loc[mask, 'fecha de reprogramación'] = self.df_vici.loc[mask, '_fecha_normalizada'] + pd.Timedelta(days=1)
            else:
                self.df_vici.loc[mask, 'fecha de reprogramación'] = pd.NaT
        
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
        
        # Usar servicio de fechas
        self.df_vici['_fecha_normalizada'] = self.df_vici[col_fecha].apply(
            normalizar_fecha_vici
        )
        
        # También crear el campo 'Created At' que espera VTIGER
        self.df_vici['created_at'] = self.df_vici['_fecha_normalizada']
        
        logger.info(f"Fechas normalizadas: {len(self.df_vici)} registros")
        self.logs.append(f"✅ Fechas normalizadas")
    
    def generar_csv_vtiger(self) -> pd.DataFrame:
        """
        Genera el DataFrame final en formato VTIGER
        Devuelve el DataFrame listo para exportar
        """
        if self.df_vici is None or self.df_vici.empty:
            raise ValueError("No hay datos para generar CSV")
        
        config = self.config
        col_cuenta = config['vici']['cuenta']
        col_telefono = config['vici']['telefono']
        col_asesor = config['vici']['asesor']
        proyecto_nombre = config['proyecto'].upper()
        
        # Construir DataFrame final con las columnas que espera VTIGER
        df_final = pd.DataFrame({
            'Created At': self.df_vici['created_at'],
            'Fecha de Reprogramación': self.df_vici['fecha de reprogramación'],
            'Assigned To': self.df_vici[col_asesor],
            'Num de Contacto': self.df_vici[col_telefono],
            'Cuenta': self.df_vici[col_cuenta],
            proyecto_nombre: self.df_vici['_campo_proyecto'],
            'Resultado de la llamada': self.df_vici['_resultado_desc'],
            'Comentario': self.df_vici['_comentario'],
            'Estado de la cuenta': self.df_vici['estado de la cuenta'],
        })
        
        # Limpiar valores NaN
        df_final = df_final.fillna('')
        
        # Guardar resultados
        self.resultados = df_final.to_dict('records')
        
        logger.info(f"CSV generado: {len(df_final)} registros")
        self.logs.append(f"✅ CSV generado: {len(df_final)} registros")
        return df_final
    
    def procesar(self, archivo_vici, archivo_crm=None, validar=True) -> pd.DataFrame:
        """
        Pipeline completo de transformación
        
        Args:
            archivo_vici: Ruta o archivo subido de VICI
            archivo_crm: Ruta o archivo subido de CRM (opcional)
            validar: Si debe validar contra BigQuery
            
        Returns:
            DataFrame listo para exportar a CSV
        """
        self.logs = []
        self.errores = []
        
        logger.info(f"Iniciando transformación para proyecto: {self.proyecto}")
        self.logs.append(f"🚀 Iniciando transformación para: {self.proyecto.upper()}")
        
        # 1. Cargar VICI
        self.cargar_vici(archivo_vici)
        
        if self.df_vici.empty:
            self.logs.append("⚠️ No hay datos en el archivo VICI")
            return pd.DataFrame()
        
        # 2. Validar cuentas (opcional)
        if validar:
            self.validar_cuentas()
            
            if self.df_vici.empty:
                self.logs.append("⚠️ No hay cuentas válidas para procesar")
                return pd.DataFrame()
        
        # 3. Normalizar fechas
        self.normalizar_fechas()
        
        # 4. Aplicar tipología
        self.aplicar_tipologia()
        
        # 5. Cargar CRM si se proporcionó
        if archivo_crm:
            self.cargar_crm(archivo_crm)
        
        # 6. Enriquecer con CRM (o fallbacks)
        self.enriquecer_con_crm()
        
        # 7. Aplicar reglas de negocio
        self.aplicar_reglas()
        
        # 8. Generar CSV final
        df_final = self.generar_csv_vtiger()
        
        # 9. Log de errores
        if self.errores:
            logger.warning(f"Se encontraron {len(self.errores)} errores durante el proceso")
            self.logs.append(f"⚠️ {len(self.errores)} cuentas no fueron procesadas")
        
        self.logs.append("✅ Transformación completada exitosamente")
        return df_final
    
    def obtener_log(self) -> Dict:
        """Devuelve el log de procesamiento"""
        return {
            'total_registros': len(self.resultados),
            'errores': self.errores[:100],  # Limitar a 100 errores
            'proyecto': self.proyecto,
            'config': self.config.get('proyecto'),
            'logs': self.logs
        }


# =====================
# FUNCIÓN PARA STREAMLIT
# =====================

def render_transformador_vici():
    """Función que se llama desde app.py para renderizar el módulo"""
    import streamlit as st
    
    st.title("🔄 Transformador VICI → CRM")
    st.caption("Convierte archivos de VICI al formato que espera VTIGER")
    
    # Sidebar - Configuración
    with st.sidebar:
        st.header("⚙️ Configuración")
        
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
            st.error("❌ No hay proyectos configurados")
            return
        
        proyecto_seleccionado = st.selectbox(
            "Proyecto",
            options=[p['archivo'] for p in proyectos_disponibles],
            format_func=lambda x: next((p['nombre'].upper() for p in proyectos_disponibles if p['archivo'] == x), x)
        )
        
        validar = st.checkbox("✅ Validar cuentas contra BigQuery", value=True)
        
        st.divider()
        st.caption("📌 El archivo VICI debe tener columnas:")
        st.caption("• city (cuenta)")
        st.caption("• phone_number (teléfono)")
        st.caption("• user (asesor)")
        st.caption("• modify_date (fecha)")
        st.caption("• status (código de gestión)")
    
    # Main - Área de archivos
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📁 Archivo VICI (.txt)")
        archivo_vici = st.file_uploader(
            "Subir archivo de VICI",
            type=['txt', 'csv'],
            help="Archivo exportado de VICIDIAL",
            key="vici_upload"
        )
        
        if archivo_vici:
            st.success(f"✅ {archivo_vici.name} - {archivo_vici.size} bytes")
    
    with col2:
        st.subheader("📊 Archivo CRM (.xlsx)")
        archivo_crm = st.file_uploader(
            "Subir archivo de CRM (opcional)",
            type=['xlsx', 'xls'],
            help="Archivo con fechas de reprogramación y estados",
            key="crm_upload"
        )
        
        if archivo_crm:
            st.success(f"✅ {archivo_crm.name} - {archivo_crm.size} bytes")
    
    # Botón de procesamiento
    if st.button("🚀 Transformar", type="primary", use_container_width=True):
        if not archivo_vici:
            st.error("❌ Debes subir al menos el archivo VICI")
            st.stop()
        
        with st.spinner("🔄 Procesando... esto puede tomar unos segundos"):
            try:
                # Inicializar transformador
                transformer = TransformadorVICI(proyecto_seleccionado)
                
                # Procesar
                df_resultado = transformer.procesar(
                    archivo_vici=archivo_vici,
                    archivo_crm=archivo_crm if archivo_crm else None,
                    validar=validar
                )
                
                if df_resultado.empty:
                    st.warning("⚠️ No se generaron datos. Revisa el log.")
                else:
                    st.success(f"✅ Transformación completada: {len(df_resultado)} registros")
                
                # Mostrar resultados
                tab1, tab2, tab3 = st.tabs(["📊 Datos", "📋 Log", "📥 Descargar"])
                
                with tab1:
                    if not df_resultado.empty:
                        st.dataframe(
                            df_resultado,
                            use_container_width=True,
                            height=400
                        )
                        st.caption(f"Total: {len(df_resultado)} registros, {len(df_resultado.columns)} columnas")
                    else:
                        st.info("No hay datos para mostrar")
                
                with tab2:
                    log = transformer.obtener_log()
                    
                    if log['errores']:
                        st.warning(f"⚠️ {len(log['errores'])} errores encontrados")
                        with st.expander("Ver errores"):
                            st.json(log['errores'][:50])  # Mostrar hasta 50 errores
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
                        # Convertir a CSV
                        csv = df_resultado.to_csv(index=False, encoding='utf-8-sig')
                        nombre_proyecto = proyecto_seleccionado.upper()
                        
                        st.download_button(
                            label="📥 Descargar CSV para VTIGER",
                            data=csv,
                            file_name=f"{nombre_proyecto}_VICI_transformado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.caption("📌 Este CSV está listo para importar en VTIGER")
                    else:
                        st.info("No hay datos para descargar")
                
            except Exception as e:
                st.error(f"❌ Error durante la transformación: {str(e)}")
                st.exception(e)
