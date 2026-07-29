# services/__init__.py
"""Servicios de acceso a datos para NexoPeople"""

from services.empleados import (
    obtener_empleado,
    obtener_lista_empleados,
    buscar_empleados,
    obtener_empleados_activos,
    obtener_empleados_por_supervisor,
    obtener_estadisticas_rapidas
)
