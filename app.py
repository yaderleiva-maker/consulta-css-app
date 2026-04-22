import streamlit as st
from modulos import login, consultas, carga_documentos
from modulos import hopsa
# LOGIN
login.login()

# LOGOUT
login.logout()

# SOLO si está logueado mostramos el sistema
if st.session_state.get("login_ok"):

    usuario = st.session_state.get("usuario")

    # -----------------------
    # ROLES (agregamos HOPSA sin tocar lo demás)
    # -----------------------
    roles = {
        "yaderleiva@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS", "HOPSA"],
        "contenalfa@gmail.com": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS", "HOPSA"],
        "arismaytte@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "sgonzalez.hex@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yesturainhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yfalconhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "delcarmenyamileth99@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
    }

    permisos = roles.get(usuario, [])

    # -----------------------
    # MENÚ PRINCIPAL (solo agregamos HOPSA si tiene permiso)
    # -----------------------
    modulos_base = ["Consultas", "Carga de Documentos"]
    
    # Agregar HOPSA solo si tiene el permiso
    if "HOPSA" in permisos:
        modulos_base.append("HOPSA")
    
    modulo = st.sidebar.selectbox("Módulos", modulos_base)

    # -----------------------
    # CONSULTAS (sin cambios)
    # -----------------------
    if modulo == "Consultas":
        if not permisos:
            st.error("❌ No tienes permisos asignados")
            st.stop()
        # Filtrar solo los permisos que no son especiales
        opciones_consulta = [p for p in permisos if p not in ["CARGA_DOCUMENTOS", "HOPSA"]]
        if not opciones_consulta:
            st.error("❌ No tienes permisos para consultas")
            st.stop()
        tipo_consulta = st.sidebar.radio("Opciones", opciones_consulta)
        consultas.run(usuario, tipo_consulta)
    
    # -----------------------
    # CARGA DE DOCUMENTOS (EXACTAMENTE IGUAL, sin tocar)
    # -----------------------
    elif modulo == "Carga de Documentos":
        if "CARGA_DOCUMENTOS" not in permisos:
            st.error("❌ No tienes permisos para acceder a este módulo")
            st.stop()
        
        tipo_carga = st.sidebar.radio(
            "Tipo de carga",
            ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
        )
        
        if tipo_carga not in permisos:
            st.error(f"❌ No tienes permiso para: {tipo_carga}")
            st.stop()
        
        # LLAMADA EXISTENTE - NO TOCAMOS ESTE ARCHIVO
        carga_documentos.run(usuario, tipo_carga)
    
    # -----------------------
    # HOPSA - NUEVO MÓDULO (completamente independiente)
    # -----------------------
    elif modulo == "HOPSA":
        # Verificar permiso específico
        if "HOPSA" not in permisos:
            st.error("❌ No tienes permisos para acceder a HOPSA")
            st.stop()
        
        # LLAMAR AL NUEVO MÓDULO
        hopsa.run(usuario)
