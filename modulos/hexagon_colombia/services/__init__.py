# services/__init__.py
"""Servicios de acceso a datos para NexoPeople"""

from services.bigquery import (
    get_client,
    ejecutar_query,
    leer_sql,
    probar_conexion
)

from services.empleados import (
    obtener_empleado,
    buscar_empleados,
    obtener_estadisticas_rapidas
)
