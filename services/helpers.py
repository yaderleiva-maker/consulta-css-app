# services/helpers.py
"""
Funciones auxiliares para el sistema NEXO People.
"""

def formatear_numero(valor):
    """
    Formatea un número para mostrar sin decimales innecesarios.
    """
    if valor is None:
        return "0"
    
    # Si es entero, mostrar sin decimales
    if valor % 1 == 0:
        return f"{int(valor)}"
    else:
        # Si tiene decimales, mostrar con 2 decimales
        return f"{valor:.2f}"
