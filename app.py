# app.py
import streamlit as st
from modulos.core import login
from modulos.hexagon_panama.consultas import consultas
from modulos.hexagon_panama.hopsa import hopsa
from modulos.hexagon_panama.hopsa import control_almuerzos
from modulos.crm import carga_documentos
from modulos.inventarios import inventario
from modulos.inventarios import inventario
from modulos.farmazone import carga_reportes

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
        "tipo": "categoria",
        "modulos": {
            "📁 Carga de Documentos": {
                "tipo": "modulo",
                "funcion": lambda: carga_documentos.run(usuario, "CSS"),
                "permiso": "CARGA_DOCUMENTOS"
            }
        }
    },
    "🏢 Hexagon Panamá": {
        "icono": "🏢",
        "tipo": "categoria",
        "modulos": {
            "📊 Consultas": {
                "tipo": "modulo",
                "funcion": lambda: ejecutar_consultas(),
                "permiso": "CONSULTAS"
            },
            "🏥 HOPSA": {
                "tipo": "categoria",
                "icono": "🏥",
                "modulos": {
                    "📋 Gestión HOPSA": {
                        "tipo": "modulo",
                        "funcion": lambda: hopsa.run(usuario),
                        "permiso": "HOPSA"
                    },
                    "🍽️ Control de Almuerzos": {
                        "tipo": "modulo",
                        "funcion": lambda: control_almuerzos.run(usuario, "Control de Almuerzos"),
                        "permiso": "CONTROL_ALMUERZOS"
                    }
                }
            }
        }
    },
    "🏢 Hexagon Colombia": {
        "icono": "🏢",
        "tipo": "categoria",
        "modulos": {}
    },
    "💊 Farmazone": {
        "icono": "💊",
        "tipo": "categoria",
        "modulos": {
                    "📋 Cargar  documentos": {
                        "tipo": "modulo",
                        "funcion": lambda: carga_reportes.run(usuario),
                        "permiso": "CARGA_REPORTES"
                    }
        }
    },
    "📦 Inventarios": {
        "icono": "📦",
        "tipo": "categoria",
        "modulos": {
            "📊 Inventario": {
                "tipo": "modulo",
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

def mostrar_menu(modulos_dict, nivel=0):
    """Muestra menú jerárquico recursivo"""
    for nombre, contenido in modulos_dict.items():
        # Verificar permisos en el nivel actual
        tiene_permiso = True
        if "permiso" in contenido:
            tiene_permiso = ROLES.get(usuario, {}).get(contenido["permiso"], False)
        
        if not tiene_permiso:
            continue
        
        # Calcular indentación visual
        indent = "  " * nivel
        
        # Si es categoría con submódulos
        if contenido.get("tipo") == "categoria" and contenido.get("modulos"):
            with st.expander(f"{contenido.get('icono', '📁')} {nombre}"):
                mostrar_menu(contenido["modulos"], nivel + 1)
        
        # Si es módulo hoja (tiene función)
        elif contenido.get("tipo") == "modulo" and "funcion" in contenido:
            if st.button(f"{indent}{contenido.get('icono', '•')} {nombre}", key=f"btn_{nombre}_{nivel}"):
                st.session_state["modulo_activo"] = contenido["funcion"]

def tipo_carga_seleccionada():
    """Helper para obtener el tipo de carga seleccionado en el sidebar"""
    # Esto se puede mejorar según necesidad
    return "CSS"

# =====================
# SIDEBAR
# =====================
with st.sidebar:
    # Logo (sin duplicación)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("assets/NEXO.jpeg", width=120)
    st.markdown("---")
    
    # Mostrar menú jerárquico
    mostrar_menu(MODULOS)
    
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
    # Limpiar después de ejecutar para evitar duplicados? No, mantener
    st.session_state["modulo_activo"]()
else:
    # Pantalla de bienvenida
    st.title("🤝 NEXO CRM")
    st.markdown("---")
    
    # Tarjetas de bienvenida
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("📊 **Consultas**\n\nConsulta bases de datos CSS")
    with col2:
        st.info("📁 **Carga de Documentos**\n\nCarga masiva de clientes")
    with col3:
        st.info("🏥 **HOPSA**\n\nGestión de HOPSA y control de almuerzos")
