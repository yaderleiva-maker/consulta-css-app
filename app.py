# app.py
import streamlit as st
from modulos.core import login
from modulos.hexagon_panama.consultas import consultas
from modulos.hexagon_panama.hopsa import hopsa
from modulos.hexagon_panama.hopsa import control_almuerzos
from modulos.crm import carga_documentos
from modulos.inventarios import inventario

# =====================
# CONFIGURACIÓN
# =====================
st.set_page_config(page_title="NEXO CRM", page_icon="🤝", layout="wide")

# =====================
# LOGIN
# =====================
login.login()

if not st.session_state.get("login_ok"):
    st.stop()

usuario = st.session_state.get("usuario")

# =====================
# CONFIGURACIÓN DE ROLES Y MÓDULOS
# =====================

# Estructura jerárquica de módulos
MODULOS = {
    "🤝 NEXO CRM": {
        "icono": "🤝",
        "modulos": {
            "📁 Carga de Documentos": {
                "funcion": lambda: carga_documentos.run(usuario, tipo_carga_seleccionada()),
                "permiso": "CARGA_DOCUMENTOS"
            }
        }
    },
    "🏢 Hexagon Panamá": {
        "icono": "🏢",
        "modulos": {
            "📊 Consultas": {
                "funcion": lambda: ejecutar_consultas(),
                "permiso": "CONSULTAS"
            },
            "🏥 HOPSA": {
                "icono": "🏥",
                "submodulos": {
                    "📋 Gestión HOPSA": {
                        "funcion": lambda: hopsa.run(usuario),
                        "permiso": "HOPSA"
                    },
                    "🍽️ Control de Almuerzos": {
                        "funcion": lambda: control_almuerzos.run(usuario, "Control de Almuerzos"),
                        "permiso": "CONTROL_ALMUERZOS"
                    }
                }
            }
        }
    },
    "🏢 Hexagon Colombia": {
        "icono": "🏢",
        "modulos": {}
    },
    "💊 Farmazone": {
        "icono": "💊",
        "modulos": {}
    },
    "📦 Inventarios": {
        "icono": "📦",
        "modulos": {
            "📊 Inventario": {
                "funcion": lambda: inventario.run(usuario),
                "permiso": "INVENTARIO"
            }
        }
    }
}

# Permisos por usuario
ROLES = {
    "yaderleiva@gmail.com": {
        "CONSULTAS": True,
        "CARGA_DOCUMENTOS": True,
        "HOPSA": True,
        "CONTROL_ALMUERZOS": True,
        "INVENTARIO": True
    },
    "mariachacon@hopsa.com": {
        "HOPSA": True,
        "CONTROL_ALMUERZOS": True
    },
    # ... más usuarios
}

# Helper para mostrar submódulos en sidebar
def mostrar_submenu(nombre_modulo, contenido, nivel=0):
    """Muestra menú jerárquico en sidebar"""
    indent = "  " * nivel
    
    # Si tiene submodulos, mostrar expander
    if "submodulos" in contenido:
        with st.expander(f"{indent}{contenido.get('icono', '📁')} {nombre_modulo}"):
            for subnombre, subcontenido in contenido["submodulos"].items():
                permiso = subcontenido.get("permiso")
                if not permiso or ROLES.get(usuario, {}).get(permiso, False):
                    if st.button(f"   {subcontenido.get('icono', '•')} {subnombre}", key=f"{nombre_modulo}_{subnombre}"):
                        st.session_state["modulo_activo"] = subcontenido["funcion"]
    
    # Si tiene módulos directos
    elif "modulos" in contenido:
        with st.expander(f"{indent}{contenido.get('icono', '📁')} {nombre_modulo}"):
            for modnombre, modcontenido in contenido["modulos"].items():
                permiso = modcontenido.get("permiso")
                if not permiso or ROLES.get(usuario, {}).get(permiso, False):
                    if st.button(f"   {modcontenido.get('icono', '•')} {modnombre}", key=f"{nombre_modulo}_{modnombre}"):
                        st.session_state["modulo_activo"] = modcontenido["funcion"]
    
    # Si es un módulo hoja
    else:
        permiso = contenido.get("permiso")
        if not permiso or ROLES.get(usuario, {}).get(permiso, False):
            if st.button(f"{indent}{contenido.get('icono', '•')} {nombre_modulo}", key=nombre_modulo):
                st.session_state["modulo_activo"] = contenido["funcion"]

def tipo_carga_seleccionada():
    """Helper para obtener el tipo de carga seleccionado en el sidebar"""
    # Esta función se puede expandir según necesidad
    return "CSS"  # o leer de session_state

def ejecutar_consultas():
    """Maneja el submenú de consultas"""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🔍 Tipo de consulta")
        tipo = st.radio(
            "Selecciona",
            ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"],
            key="tipo_consulta"
        )
    consultas.run(usuario, tipo)

# =====================
# SIDEBAR
# =====================
with st.sidebar:
    st.image("assets/NEXO.jpeg", width=150)
    st.markdown("---")
    
    # Menú jerárquico
    for nombre_modulo, contenido in MODULOS.items():
        mostrar_submenu(nombre_modulo, contenido)
    
    st.markdown("---")
    st.caption(f"👤 {usuario}")
    st.caption("🤝 NEXO CRM | by DolaAI")
    
    if st.button("🚪 Cerrar sesión", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# =====================
# CONTENIDO PRINCIPAL
# =====================
if "modulo_activo" in st.session_state:
    st.session_state["modulo_activo"]()
else:
    # Pantalla de bienvenida
    st.title("🤝 NEXO CRM")
    st.markdown("---")
    st.info("Selecciona un módulo del menú lateral para comenzar")
