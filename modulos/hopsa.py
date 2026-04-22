# modulos/hopsa.py - VERSIÓN MÍNIMA DE PRUEBA
import streamlit as st

def run(usuario):
    st.title("🎯 HOPSA - PRUEBA")
    st.write(f"Usuario: {usuario}")
    st.success("Si ves esto, el módulo se importó correctamente")
