import streamlit as st
from modulos import login, consultas

# LOGIN
login.login()

# BOTÓN LOGOUT
login.logout()

# MENÚ
opcion = st.sidebar.selectbox(
    "Selecciona módulo",
    ["Consultas"]
)

# NAVEGACIÓN
if opcion == "Consultas":
    consultas.run(st.session_state.usuario)
