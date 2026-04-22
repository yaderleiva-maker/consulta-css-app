import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "proyecto-css-panama"
DATASET_HOPSA = "hopsa"

def run(usuario):
    st.title("HOPSA - Gestion de Ventas")
    st.write(f"Usuario: {usuario}")
    
    opcion = st.radio("Opciones", ["Agentes", "Informacion", "Reportes"])
    
    if opcion == "Agentes":
        st.subheader("Cargar Agentes")
        archivo = st.file_uploader("Subir archivo", type=['csv', 'xlsx'])
        if archivo:
            st.success("Archivo cargado")
    
    elif opcion == "Informacion":
        st.subheader("Subir Informacion")
        st.info("En construccion")
    
    else:
        st.subheader("Descargar Reportes")
        st.info("En construccion")
