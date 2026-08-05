# modulos/empresas/hexagon_panama/ifx/__init__.py
"""
Módulo IFX - Dashboard Comercial y Análisis de Gestión
"""

from .dashboard import DashboardIFX
from .embudo import EmbudoIFX
from .reportes import ReportesIFX
from .models import (
    GestionIFX,
    ClienteIFX,
    ResumenKPI,
    EmbudoEtapa
)

__all__ = [
    'DashboardIFX',
    'EmbudoIFX',
    'ReportesIFX',
    'GestionIFX',
    'ClienteIFX',
    'ResumenKPI',
    'EmbudoEtapa'
]
