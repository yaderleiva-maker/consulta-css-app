import streamlit as st
from modulos import login, consultas

# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO si está logueado mostramos el sistema
if st.session_state.get("login_ok"):

    usuario = st.session_state.get("usuario")

    # -----------------------
    # ROLES
    # -----------------------
    roles = {
        "yaderleiva@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "contenalfa@gmail.com": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "arismaytte@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "sgonzalez.hex@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yesturainhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yfalconhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "delcarmenyamileth99@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
    }

    permisos = roles.get(usuario, [])

    # -----------------------
    # MENÚ PRINCIPAL
    # -----------------------
    modulo = st.sidebar.selectbox(
        "Módulos",
        ["Consultas"]
    )

    # -----------------------
    # SUBMENÚ CONSULTAS
    # -----------------------
    if modulo == "Consultas":

        if not permisos:
            st.error("❌ No tienes permisos asignados")
            st.stop()

        tipo_consulta = st.sidebar.radio(
            "Opciones",
            permisos
        )

        consultas.run(usuario, tipo_consulta)
