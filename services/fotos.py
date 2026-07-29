# services/fotos.py
"""
Servicio de Fotos
Manejo de imágenes desde Google Drive y generación de avatares.
"""

import streamlit as st

# ============================================================
# CONFIGURACIÓN DE GOOGLE DRIVE
# ============================================================

def obtener_url_foto(id_drive):
    """
    Construir URL de Google Drive a partir del ID del archivo.
    """
    if id_drive and id_drive != '' and id_drive != 'None':
        return f"https://drive.google.com/uc?export=view&id={id_drive}"
    return None


def obtener_url_avatar(nombre):
    """
    Generar avatar con iniciales usando ui-avatars.com
    """
    if nombre and nombre != '':
        nombre_url = nombre.replace(' ', '+')
        return f"https://ui-avatars.com/api/?name={nombre_url}&size=200&background=4A90E2&color=white&rounded=true"
    return "https://ui-avatars.com/api/?name=Usuario&size=200&background=4A90E2&color=white&rounded=true"


def mostrar_foto_sidebar(id_drive, nombre, size=50):
    """
    Mostrar foto en el sidebar con tamaño reducido.
    """
    url = obtener_url_foto(id_drive)
    if url:
        st.image(url, width=size)
    else:
        avatar = obtener_url_avatar(nombre)
        st.image(avatar, width=size)


def mostrar_foto_ficha(id_drive, nombre, size=150):
    """
    Mostrar foto en la ficha del empleado.
    """
    url = obtener_url_foto(id_drive)
    if url:
        st.image(url, width=size)
    else:
        avatar = obtener_url_avatar(nombre)
        st.image(avatar, width=size)
