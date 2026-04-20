import streamlit as st
from modulos import login, consultas, carga_documentos  # Agregar carga_documentos

# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO si está logueado mostramos el sistema
if st.session_state.get("login_ok"):

    usuario = st.session_state.get("usuario")

    # -----------------------
    # ROLES (actualizado con nuevos permisos)
    # -----------------------
    roles = {
        "yaderleiva@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "contenalfa@gmail.com": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "arismaytte@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "sgonzalez.hex@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "yesturainhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "yfalconhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
        "delcarmenyamileth99@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS"],
    }

    permisos = roles.get(usuario, [])

    # -----------------------
    # MENÚ PRINCIPAL
    # -----------------------
    modulo = st.sidebar.selectbox(
        "Módulos",
        ["Consultas", "Carga de Documentos"]  # Agregar nueva opción
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
            [p for p in permisos if p != "subir"]
        )
        consultas.run(usuario, tipo_consulta)
    
    # -----------------------
    # NUEVO: SUBMENÚ CARGA DE DOCUMENTOS
    # -----------------------
    elif modulo == "Carga de Documentos":
        if "subir" not in permisos:
            st.error("❌ No tienes permisos para acceder a este módulo")
            st.stop()
        
        # Permisos específicos para carga
        tipo_carga = st.sidebar.radio(
            "Tipo de carga",
            ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
        )
        
        if tipo_carga not in permisos:
            st.error(f"❌ No tienes permiso para: {tipo_carga}")
            st.stop()
        
        carga_documentos.run(usuario, tipo_carga)
