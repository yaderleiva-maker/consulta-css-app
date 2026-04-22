# modulos/hopsa.py - PASO 3
import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def run(usuario):
    st.title("🎯 HOPSA - PRUEBA")
    st.write(f"Usuario: {usuario}")
    st.write("BigQuery importado correctamente")
    st.success("Todas las importaciones OK")
