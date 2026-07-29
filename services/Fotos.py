# services/fotos.py
"""
Servicio de Fotos (Simplificado)
Ahora usa el servicio genérico de archivos.
"""

import streamlit as st
from services.archivos import mostrar_foto_desde_ruta


def mostrar_foto_sidebar(ruta_relativa, nombre, size=50):
    """
    Mostrar foto en el sidebar.
    """
    if ruta_relativa:
        # Intentar mostrar la foto desde Drive
        imagen_data = mostrar_foto_desde_ruta(ruta_relativa, width=size)
        if imagen_data:
            return
    
    # Si no hay foto, mostrar avatar
    from services.fotos import obtener_url_avatar
    st.image(obtener_url_avatar(nombre), width=size)


def mostrar_foto_ficha(ruta_relativa, nombre, size=150):
    """
    Mostrar foto en la ficha del empleado.
    """
    if ruta_relativa:
        imagen_data = mostrar_foto_desde_ruta(ruta_relativa, width=size)
        if imagen_data:
            return
    
    # Si no hay foto, mostrar avatar
    from services.fotos import obtener_url_avatar
    st.image(obtener_url_avatar(nombre), width=size)


def obtener_url_avatar(nombre):
    """
    Generar avatar con iniciales.
    """
    if nombre and nombre != '':
        nombre_url = nombre.replace(' ', '+')
        return f"https://ui-avatars.com/api/?name={nombre_url}&size=200&background=4A90E2&color=white&rounded=true"
    return "https://ui-avatars.com/api/?name=Usuario&size=200&background=4A90E2&color=white&rounded=true"
