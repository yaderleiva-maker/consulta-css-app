import streamlit as st
from modulos import login, consultas

# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO si está logueado mostramos el sistema
if st.session_state.get("login_ok"):

    roles = {
    "yaderleiva@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
    "contenalfa@gmail.com":  ["TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
    }

    # MENÚ
    opcion = st.sidebar.selectbox(
        "Selecciona módulo",
        ["Consultas"]
    )

    # NAVEGACIÓN
    if opcion == "Consultas":
        usuario = st.session_state.get("usuario")
        permisos = roles.get(usuario, [])
