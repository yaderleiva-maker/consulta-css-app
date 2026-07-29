# services/fotos.py
"""
Servicio de Fotos
Manejo de imágenes desde Google Drive y generación de avatares.
"""

import streamlit as st
from services.archivos import obtener_imagen_desde_ruta

# ============================================================
# FUNCIONES PRINCIPALES
# ============================================================

def obtener_url_avatar(nombre):
    """
    Generar avatar con iniciales usando ui-avatars.com
    """
    if nombre and nombre != '':
        nombre_url = nombre.replace(' ', '+')
        return f"https://ui-avatars.com/api/?name={nombre_url}&size=200&background=4A90E2&color=white&rounded=true"
    return "https://ui-avatars.com/api/?name=Usuario&size=200&background=4A90E2&color=white&rounded=true"


def mostrar_foto_sidebar(foto_ruta, nombre, size=50):
    """
    Mostrar foto en el sidebar con tamaño reducido.
    """
    # Si es una URL pública, mostrarla directamente
    if foto_ruta and isinstance(foto_ruta, str) and foto_ruta.startswith('http'):
        st.image(foto_ruta, width=size)
        return
    
    # Si es una ruta de AppSheet, intentar descargar desde Drive
    if foto_ruta:
        imagen_data = obtener_imagen_desde_ruta(foto_ruta)
        if imagen_data:
            st.image(imagen_data, width=size)
            return
    
    # Si todo falla, mostrar avatar
    avatar = obtener_url_avatar(nombre)
    st.image(avatar, width=size)


def mostrar_foto_ficha(foto_ruta, nombre, size=150):
    """
    Mostrar foto en la ficha del empleado.
    """
    # Si es una URL pública, mostrarla directamente
    if foto_ruta and isinstance(foto_ruta, str) and foto_ruta.startswith('http'):
        st.image(foto_ruta, width=size)
        return
    
    # Si es una ruta de AppSheet, intentar descargar desde Drive
    if foto_ruta:
        imagen_data = obtener_imagen_desde_ruta(foto_ruta)
        if imagen_data:
            st.image(imagen_data, width=size)
            return
    
    # Si todo falla, mostrar avatar
    avatar = obtener_url_avatar(nombre)
    st.image(avatar, width=size)
