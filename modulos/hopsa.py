import streamlit as st
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def run(usuario):
    st.title("HOPSA - PRUEBA 2")
    st.write(f"Usuario: {usuario}")
    st.write("Pandas version:", pd.__version__)
    st.write("BigQuery importado correctamente")
    st.success("Todas las importaciones OK")
