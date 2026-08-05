"""
Modelos de datos para IFX
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List
import pandas as pd


@dataclass
class GestionIFX:
    """Representa una gestión comercial"""
    id_gestion: str
    fecha: date
    fecha_hora: Optional[datetime] = None
    nombre: str = ""
    producto_ofrecido: Optional[str] = None
    resultado: Optional[str] = None
    motivo_no: Optional[str] = None
    canal: Optional[str] = None
    agente: Optional[str] = None
    contacto: int = 0
    no_contacto: int = 0
    estado_contacto: Optional[int] = None
    flag_cita: int = 0
    flag_cita_atendida: int = 0
    flag_interesado: int = 0
    flag_contacto: int = 0
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['GestionIFX']:
        """Crea lista de objetos desde DataFrame"""
        return [cls(**row) for row in df.to_dict('records')]


@dataclass
class ClienteIFX:
    """Representa un cliente de IFX"""
    id_cliente: Optional[str] = None
    nombre: str = ""
    nombre_normalizado: str = ""
    cedula: Optional[str] = None
    celular: Optional[str] = None
    correo: Optional[str] = None
    origen: Optional[str] = None
    estado_cuenta: Optional[str] = None
    cantidad_toques: int = 0
    nivel_actividad: Optional[str] = None
    etapa_embudo: Optional[str] = None


@dataclass
class ResumenKPI:
    """Resumen de KPIs para dashboard"""
    total_gestiones: int = 0
    total_contactos: int = 0
    total_citas: int = 0
    total_citas_atendidas: int = 0
    total_interesados: int = 0
    tasa_conversion: float = 0.0
    agentes_activos: int = 0
    clientes_unicos: int = 0
    periodo: str = ""
    
    def to_dict(self):
        return {
            'total_gestiones': self.total_gestiones,
            'total_contactos': self.total_contactos,
            'total_citas': self.total_citas,
            'total_citas_atendidas': self.total_citas_atendidas,
            'total_interesados': self.total_interesados,
            'tasa_conversion': self.tasa_conversion,
            'agentes_activos': self.agentes_activos,
            'clientes_unicos': self.clientes_unicos,
            'periodo': self.periodo
        }


@dataclass
class EmbudoEtapa:
    """Etapa del embudo comercial"""
    etapa: str
    prioridad: int
    clientes: int
    porcentaje: float = 0.0
    
    def to_dict(self):
        return {
            'etapa': self.etapa,
            'prioridad': self.prioridad,
            'clientes': self.clientes,
            'porcentaje': self.porcentaje
        }
