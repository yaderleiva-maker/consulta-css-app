import streamlit as st
from modulos import login, consultas

# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO si está logueado mostramos el sistema
if st.session_state.get("login_ok"):

    # MENÚ
    opcion = st.sidebar.selectbox(
        "Selecciona módulo",
        ["Consultas"]
    )

    # NAVEGACIÓN
    if opcion == "Consultas":
        consultas.run(st.session_state.get("usuario"))
