# modulos/hopsa.py - PASO 2
import streamlit as st
import pandas as pd
import datetime

def run(usuario):
    st.title("🎯 HOPSA - PRUEBA")
    st.write(f"Usuario: {usuario}")
    st.write("Pandas version:", pd.__version__)
    st.success("Importaciones básicas OK")
