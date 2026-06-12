import streamlit as st
from modulos import login, consultas, carga_documentos
from modulos import hopsa
from modulos import inventario
from modulos.hexagon_panama.hopsa import control_almuerzos  # ← NUEVO

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
        "yaderleiva@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS", "HOPSA", "INVENTARIO", "CONTROL_ALMUERZOS"],
        "contenalfa@gmail.com": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS", "CARGA_DOCUMENTOS", "HOPSA"],
        "arismaytte@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "sgonzalez.hex@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yesturainhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "yfalconhexagon@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "delcarmenyamileth99@gmail.com": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
        "mariachacon@hopsa.com": ["HOPSA", "CONTROL_ALMUERZOS"],
        "gabrielamorales@hopsa.com": ["HOPSA", "CONTROL_ALMUERZOS"],
    }

    permisos = roles.get(usuario, [])

    # ========== SIDEBAR ==========
    with st.sidebar:
        st.image("assets/NEXO.jpeg", width=150)
        st.markdown("---")
        modulos_base = ["Consultas", "Carga de Documentos"]
        
        if "HOPSA" in permisos:
            modulos_base.append("HOPSA")
        if "INVENTARIO" in permisos:
            modulos_base.append("INVENTARIO")
        if "CONTROL_ALMUERZOS" in permisos:
            modulos_base.append("Control de Almuerzos")  # ← NUEVO

        modulo = st.selectbox("Módulos", modulos_base)
        st.caption("NEXO CRM | by DolaAI")

    # =============================================
    # CONSULTAS
    # =============================================
    if modulo == "Consultas":
        if not permisos:
            st.error("❌ No tienes permisos asignados")
            st.stop()
        opciones_consulta = [p for p in permisos if p not in ["CARGA_DOCUMENTOS", "HOPSA", "INVENTARIO", "CONTROL_ALMUERZOS"]]
        if not opciones_consulta:
            st.error("❌ No tienes permisos para consultas")
            st.stop()
        tipo_consulta = st.sidebar.radio("Opciones", opciones_consulta)
        consultas.run(usuario, tipo_consulta)
    
    # =============================================
    # CARGA DE DOCUMENTOS
    # =============================================
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
        
        carga_documentos.run(usuario, tipo_carga)
    
    # =============================================
    # INVENTARIO
    # =============================================
    elif modulo == "INVENTARIO":
        if "INVENTARIO" not in permisos:
            st.error("❌ No tienes permisos para acceder a INVENTARIO")
            st.stop()
        inventario.run(usuario)
    
    # =============================================
    # HOPSA
    # =============================================
    elif modulo == "HOPSA":
        if "HOPSA" not in permisos:
            st.error("❌ No tienes permisos para acceder a HOPSA")
            st.stop()
        hopsa.run(usuario)
    
    # =============================================
    # CONTROL DE ALMUERZOS
    # =============================================
    elif modulo == "Control de Almuerzos":
        if "CONTROL_ALMUERZOS" not in permisos:
            st.error("❌ No tienes permisos para acceder a Control de Almuerzos")
            st.stop()
        control_almuerzos.run(usuario, "Control de Almuerzos")
