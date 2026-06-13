# app.py
import streamlit as st
from modulos.core import login
from modulos.hexagon_panama.consultas import consultas
from modulos.hexagon_panama.hopsa import hopsa
from modulos.hexagon_panama.hopsa import control_almuerzos
from modulos.crm import carga_documentos
from modulos.inventarios import inventario

# =====================
# CONFIGURACIÓN DE PÁGINA
# =====================
st.set_page_config(
    page_title="NEXO CRM",
    page_icon="🤝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================
# FUNCIONES AUXILIARES (definidas ANTES de usarse)
# =====================

def ejecutar_consultas(usuario, consultas_permitidas):
    """Ejecuta el módulo de consultas con submenú"""
    if not consultas_permitidas:
        st.error("❌ No tienes permisos para consultas")
        return
    
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🔍 Consultas disponibles")
        tipo_consulta = st.radio(
            "Selecciona una consulta",
            consultas_permitidas,
            key="consulta_select"
        )
    
    consultas.run(usuario, tipo_consulta)

def ejecutar_carga_documentos(usuario, cargas_permitidas):
    """Ejecuta el módulo de carga de documentos con submenú"""
    if not cargas_permitidas:
        st.error("❌ No tienes permisos para cargar documentos")
        return
    
    # Mapeo de nombres amigables
    nombre_cargas = {
        "CSS": "📊 CSS",
        "TELÉFONOS NUEVOS": "📞 TELÉFONOS NUEVOS",
        "CORREOS NUEVOS": "📧 CORREOS NUEVOS"
    }
    
    opciones_carga = [nombre_cargas.get(c, c) for c in cargas_permitidas]
    
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 📤 Tipo de carga")
        seleccion_carga = st.radio(
            "Selecciona una opción",
            opciones_carga,
            key="carga_select"
        )
        
        # Mapear de vuelta al valor original
        tipo_carga = next(
            (k for k, v in nombre_cargas.items() if v == seleccion_carga),
            seleccion_carga
        )
    
    carga_documentos.run(usuario, tipo_carga)

def cerrar_sesion():
    """Cierra la sesión del usuario"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# =====================
# LOGIN (un solo lugar)
# =====================
login.login()

# Solo mostrar logout si está logueado
if st.session_state.get("login_ok"):
    with st.sidebar:
        if st.button("🚪 Cerrar sesión", use_container_width=True):
            cerrar_sesion()

# =====================
# SESIÓN INICIADA
# =====================
if st.session_state.get("login_ok"):
    usuario = st.session_state.get("usuario")
    
    # =====================
    # CONFIGURACIÓN DE ROLES (separada)
    # =====================
    ROLES = {
        "yaderleiva@gmail.com": {
            "modulos": ["consultas", "carga_documentos", "hopsa", "inventario", "control_almuerzos"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
        },
        "contenalfa@gmail.com": {
            "modulos": ["consultas", "carga_documentos", "hopsa"],
            "consultas": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": ["TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
        },
        "arismaytte@gmail.com": {
            "modulos": ["consultas"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": []
        },
        "sgonzalez.hex@gmail.com": {
            "modulos": ["consultas"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": []
        },
        "yesturainhexagon@gmail.com": {
            "modulos": ["consultas"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": []
        },
        "yfalconhexagon@gmail.com": {
            "modulos": ["consultas"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": []
        },
        "delcarmenyamileth99@gmail.com": {
            "modulos": ["consultas"],
            "consultas": ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            "cargas": []
        },
        "mariachacon@hopsa.com": {
            "modulos": ["hopsa", "control_almuerzos"],
            "consultas": [],
            "cargas": []
        },
        "gabrielamorales@hopsa.com": {
            "modulos": ["hopsa", "control_almuerzos"],
            "consultas": [],
            "cargas": []
        },
    }
    
    # Rol por defecto
    rol_usuario = ROLES.get(usuario, {
        "modulos": ["consultas"],
        "consultas": [],
        "cargas": []
    })
    
    modulos_permitidos = rol_usuario.get("modulos", [])
    consultas_permitidas = rol_usuario.get("consultas", [])
    cargas_permitidas = rol_usuario.get("cargas", [])
    
    # =====================
    # MAPEO DE MÓDULOS
    # =====================
    MODULOS = {
        "consultas": {
            "nombre": "📊 Consultas",
            "icono": "🔍",
            "funcion": lambda: ejecutar_consultas(usuario, consultas_permitidas)
        },
        "carga_documentos": {
            "nombre": "📁 Carga de Documentos",
            "icono": "📤",
            "funcion": lambda: ejecutar_carga_documentos(usuario, cargas_permitidas)
        },
        "hopsa": {
            "nombre": "🏥 HOPSA",
            "icono": "🏥",
            "funcion": lambda: hopsa.run(usuario)
        },
        "inventario": {
            "nombre": "📦 Inventario",
            "icono": "📦",
            "funcion": lambda: inventario.run(usuario)
        },
        "control_almuerzos": {
            "nombre": "🍽️ Control de Almuerzos",
            "icono": "🍽️",
            "funcion": lambda: control_almuerzos.run(usuario, "Control de Almuerzos")
        }
    }
    
    # Filtrar módulos permitidos
    modulos_activos = {k: MODULOS[k] for k in modulos_permitidos if k in MODULOS}
    
    # =====================
    # SIDEBAR
    # =====================
    with st.sidebar:
        # Logo
        st.image("assets/NEXO.jpeg", width=150)
        st.markdown("---")
        
        # Selector de módulo
        if modulos_activos:
            opciones_modulo = [f"{mod['icono']} {mod['nombre']}" for mod in modulos_activos.values()]
            seleccion = st.selectbox("📌 Módulo", opciones_modulo, index=0)
            
            # Extraer clave del módulo seleccionado
            modulo_seleccionado = list(modulos_activos.keys())[opciones_modulo.index(seleccion)]
        else:
            st.error("❌ No tienes módulos asignados")
            st.stop()
        
        st.markdown("---")
        st.caption(f"👤 **Usuario:** {usuario}")
        st.caption("🤝 **NEXO CRM** | by DolaAI")
    
    # =====================
    # EJECUTAR MÓDULO
    # =====================
    if modulos_activos:
        modulos_activos[modulo_seleccionado]["funcion"]()
