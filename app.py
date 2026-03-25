import streamlit as st
from modulos import login, consultas

# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO ejecutar consultas si está logueado
if st.session_state.get("login_ok"):
    consultas.run(st.session_state.get("usuario"))
# MENÚ
opcion = st.sidebar.selectbox(
    "Selecciona módulo",
    ["Consultas"]
)

# NAVEGACIÓN
if opcion == "Consultas":
    consultas.run(st.session_state.usuario)
