"""
Transformador VICI → CRM
Motor genérico que procesa archivos VICI según configuración YAML
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging

from services.bigquery import ejecutar_query
from services.archivos import leer_excel
from services.fechas import normalizar_fecha_vici

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransformadorVICI:
    """Clase principal que orquesta la transformación"""
    
    def __init__(self, proyecto: str):
        """
        Args:
            proyecto: Nombre del proyecto (ej: 'jamar')
        """
        self.proyecto = proyecto
        self.config = self._cargar_configuracion(proyecto)
        self.df_vici = None
        self.df_crm = None
        self.df_cartera = None
        self.resultados = []
        self.errores = []
        
    def _cargar_configuracion(self, proyecto: str) -> Dict:
        """Carga el YAML del proyecto"""
        ruta = Path(__file__).parent / "proyectos" / f"{proyecto}.yaml"
        if not ruta.exists():
            raise FileNotFoundError(f"Configuración no encontrada: {ruta}")
        
        with open(ruta, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def cargar_vici(self, archivo_txt) -> pd.DataFrame:
        """
        Carga archivo VICI (puede ser ruta o archivo subido)
        Detecta automáticamente separador
        """
        # Si es archivo subido (Streamlit), leemos
        if hasattr(archivo_txt, 'read'):
            import io
            contenido = archivo_txt.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(contenido), sep=None, engine='python')
        else:
            # Es ruta de archivo
            df = pd.read_csv(archivo_txt, sep=None, engine='python')
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.lower().str.strip()
        
        self.df_vici = df
        logger.info(f"VICI cargado: {len(df)} registros")
        return df
    
    def cargar_crm(self, archivo_crm) -> pd.DataFrame:
        """Carga archivo CRM (Excel)"""
        if hasattr(archivo_crm, 'read'):
            df = pd.read_excel(archivo_crm)
        else:
            df = pd.read_excel(archivo_crm)
        
        df.columns = df.columns.str.lower().str.strip()
        self.df_crm = df
        logger.info(f"CRM cargado: {len(df)} registros")
        return df
    
    def validar_cuentas(self) -> pd.DataFrame:
        """
        Valida que las cuentas existan en BigQuery (cartera)
        Filtra solo las que existen
        """
        if self.df_vici is None:
            raise ValueError("Primero cargar VICI")
        
        # Obtener mapeo de columna de cuenta
        col_cuenta = self.config['vici']['cuenta']
        cuentas = self.df_vici[col_cuenta].dropna().unique().tolist()
        
        if not cuentas:
            raise ValueError("No hay cuentas en el archivo VICI")
        
        # Query a BigQuery - cuentas existentes
        cuentas_str = "', '".join([str(c) for c in cuentas])
        query = f"""
            SELECT DISTINCT 
                CAST(numero_cuenta AS STRING) as cuenta
            FROM `hexagon-453418.cobranza.cuentas`
            WHERE proyecto = '{self.config['proyecto']}'
              AND CAST(numero_cuenta AS STRING) IN ('{cuentas_str}')
        """
        
        df_existentes = ejecutar_query(query)
        cuentas_validas = set(df_existentes['cuenta'].astype(str))
        
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
        
        self.df_vici = df_validas.drop(columns=['_valida'])
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
    
    def aplicar_reglas(self):
        """Aplica reglas de negocio específicas del proyecto"""
        if self.df_vici is None or self.df_vici.empty:
            return
        
        config = self.config
        
        # 1. Mapeo de asesores
        if 'asesores' in config.get('reglas', {}):
            asesores_map = config['reglas']['asesores']
            col_asesor = config['vici']['asesor']
            self.df_vici[col_asesor] = self.df_vici[col_asesor].map(
                lambda x: asesores_map.get(x, x) if pd.notna(x) else x
            )
        
        # 2. Crear campo de proyecto (ej: JAMAR::::918020)
        if 'campo_proyecto' in config.get('reglas', {}):
            campo_conf = config['reglas']['campo_proyecto']
            col_cuenta = config['vici']['cuenta']
            self.df_vici['_campo_proyecto'] = self.df_vici[col_cuenta].apply(
                lambda x: campo_conf['formato'].format(
                    proyecto=campo_conf['nombre'],
                    cuenta=x
                )
            )
        
        # 3. Crear comentario
        if 'comentario' in config.get('reglas', {}):
            formato = config['reglas']['comentario']['formato']
            col_telefono = config['vici']['telefono']
            self.df_vici['_comentario'] = self.df_vici.apply(
                lambda row: formato.format(
                    telefono=row[col_telefono],
                    resultado=row['_resultado_desc']
                ) if pd.notna(row[col_telefono]) and pd.notna(row['_resultado_desc']) else '',
                axis=1
            )
    
    def enriquecer_con_crm(self):
        """Enriquece los datos con información del CRM (fechas, estados)"""
        if self.df_crm is None:
            logger.warning("No hay CRM cargado, usando fallbacks")
            self._aplicar_fallbacks()
            return
        
        if self.df_vici is None or self.df_vici.empty:
            return
        
        col_cuenta = self.config['vici']['cuenta']
        
        # Normalizar columnas del CRM
        crm_cols = {col.lower(): col for col in self.df_crm.columns}
        
        # Buscar columna de cuenta en CRM
        col_cuenta_crm = None
        for posible in ['cuenta', 'numero cuenta', 'num_cuenta', 'account']:
            if posible in crm_cols:
                col_cuenta_crm = crm_cols[posible]
                break
        
        if col_cuenta_crm is None:
            logger.warning("No se encontró columna de cuenta en CRM, usando fallbacks")
            self._aplicar_fallbacks()
            return
        
        # Unir con CRM
        self.df_vici = self.df_vici.merge(
            self.df_crm[[col_cuenta_crm, 'fecha de reprogramación', 'estado de la cuenta']],
            left_on=col_cuenta,
            right_on=col_cuenta_crm,
            how='left'
        )
        
        # Aplicar fallbacks donde no hay datos
        self._aplicar_fallbacks()
        
        logger.info(f"CRM enriquecido: {len(self.df_vici)} registros")
    
    def _aplicar_fallbacks(self):
        """Aplica valores por defecto cuando faltan datos del CRM"""
        fallbacks = self.config.get('fallbacks', {})
        col_cuenta = self.config['vici']['cuenta']
        
        # Fecha de reprogramación
        if 'fecha_reprogramacion' in self.df_vici.columns:
            mask = self.df_vici['fecha de reprogramación'].isna()
            self.df_vici.loc[mask, 'fecha de reprogramación'] = self.df_vici.loc[mask, '_fecha_normalizada'] + pd.Timedelta(days=1)
        else:
            self.df_vici['fecha de reprogramación'] = self.df_vici['_fecha_normalizada'] + pd.Timedelta(days=1)
        
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
        
        # Construir DataFrame final con las columnas que espera VTIGER
        # (Según el Access: Created At, Fecha de Reprogramación, Assigned To, etc.)
        df_final = pd.DataFrame({
            'Created At': self.df_vici['created_at'],
            'Fecha de Reprogramación': self.df_vici['fecha de reprogramación'],
            'Assigned To': self.df_vici[col_asesor],
            'Num de Contacto': self.df_vici[col_telefono],
            'Cuenta': self.df_vici[col_cuenta],
            'JAMAR': self.df_vici['_campo_proyecto'],
            'Resultado de la llamada': self.df_vici['_resultado_desc'],
            'Comentario': self.df_vici['_comentario'],
            'Estado de la cuenta': self.df_vici['estado de la cuenta'],
        })
        
        # Limpiar valores NaN
        df_final = df_final.fillna('')
        
        # Guardar resultados
        self.resultados = df_final.to_dict('records')
        
        logger.info(f"CSV generado: {len(df_final)} registros")
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
        logger.info(f"Iniciando transformación para proyecto: {self.proyecto}")
        
        # 1. Cargar VICI
        self.cargar_vici(archivo_vici)
        
        # 2. Validar cuentas (opcional)
        if validar:
            self.validar_cuentas()
        
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
        
        return df_final
    
    def obtener_log(self) -> Dict:
        """Devuelve el log de procesamiento"""
        return {
            'total_registros': len(self.resultados),
            'errores': self.errores,
            'proyecto': self.proyecto,
            'config': self.config.get('proyecto')
        }
