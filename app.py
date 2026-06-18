# app.py
import streamlit as st
from modulos.core import login
from modulos.hexagon_panama.consultas import consultas
from modulos.hexagon_panama.hopsa import hopsa
from modulos.hexagon_panama.hopsa import control_almuerzos
from modulos.crm import carga_documentos
from modulos.inventarios import inventario
from modulos.farmazone import carga_reportes

# =====================
# CONFIGURACIÓN
# =====================
st.set_page_config(page_title="NEXO CRM", page_icon="💻", layout="wide")

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
            "📋 Cargar documentos": {
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
        "INVENTARIO": True,
        "CARGA_REPORTES": True
    },
    "mariachacon@hopsa.com": {
        "HOPSA": True,
        "CONTROL_ALMUERZOS": True
    },
    "arismaytte@gmail.com": {
        "CONSULTAS": True
    },
    "condadodelreyfarmazone@gmail.com": {
    "CARGA_REPORTES": True
    },
    "tmksolutionspty@gmail.com": {
    "CARGA_REPORTES": True
    },
    
    # Agrega más usuarios aquí
}

# =====================
# FUNCIONES AUXILIARES
# =====================

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

def tiene_modulos_visibles(modulos_dict):
    """Verifica si existe al menos un módulo visible para el usuario"""
    for nombre, contenido in modulos_dict.items():
        # Si es un módulo hoja (tiene función)
        if contenido.get("tipo") == "modulo":
            permiso = contenido.get("permiso")
            if permiso and ROLES.get(usuario, {}).get(permiso, False):
                return True
            # Si no tiene permiso explícito, se asume visible (módulo público)
            elif not permiso:
                return True
        
        # Si es una categoría con submódulos
        elif contenido.get("tipo") == "categoria" and contenido.get("modulos"):
            if tiene_modulos_visibles(contenido["modulos"]):
                return True
    
    return False

def mostrar_menu(modulos_dict, nivel=0):
    """Muestra menú jerárquico recursivo - SOLO módulos visibles"""
    for nombre, contenido in modulos_dict.items():
        # Si es categoría con submódulos
        if contenido.get("tipo") == "categoria" and contenido.get("modulos"):
            # Solo mostrar categoría si tiene al menos un módulo visible
            if tiene_modulos_visibles(contenido["modulos"]):
                with st.expander(f"{contenido.get('icono', '📁')} {nombre}"):
                    mostrar_menu(contenido["modulos"], nivel + 1)
        
        # Si es módulo hoja (tiene función)
        elif contenido.get("tipo") == "modulo" and "funcion" in contenido:
            # Verificar permisos
            tiene_permiso = True
            if "permiso" in contenido:
                permiso = contenido["permiso"]
                tiene_permiso = ROLES.get(usuario, {}).get(permiso, False)
            
            if tiene_permiso:
                indent = "  " * nivel
                if st.button(f"{indent}{contenido.get('icono', '•')} {nombre}", 
                            key=f"btn_{nombre}_{nivel}"):
                    st.session_state["modulo_activo"] = contenido["funcion"]

# =====================
# SIDEBAR
# =====================
with st.sidebar:
    # Logo
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
# =====================
# CONTENIDO PRINCIPAL
# =====================
if "modulo_activo" in st.session_state:
    st.session_state["modulo_activo"]()
else:
    # Pantalla de bienvenida
    st.title("🤝 NEXO CRM")
    st.markdown("---")
    
    st.markdown(f"### ¡Bienvenido, {usuario}! 👋")
    st.markdown("Selecciona un módulo del menú lateral para comenzar.")
    st.markdown("---")
    
    # Recorrer módulos de primer nivel
    for categoria_nombre, categoria_contenido in MODULOS.items():
        if categoria_contenido.get("tipo") == "categoria" and categoria_contenido.get("modulos"):
            # Verificar si la categoría tiene módulos visibles
            if tiene_modulos_visibles(categoria_contenido["modulos"]):
                st.subheader(f"{categoria_contenido.get('icono', '📁')} {categoria_nombre}")
                
                # Mostrar módulos de esta categoría en filas de 3
                modulos_visibles = []
                for modulo_nombre, modulo_contenido in categoria_contenido["modulos"].items():
                    if modulo_contenido.get("tipo") == "modulo":
                        permiso = modulo_contenido.get("permiso")
                        if permiso and ROLES.get(usuario, {}).get(permiso, False):
                            modulos_visibles.append({
                                "nombre": modulo_nombre,
                                "icono": modulo_contenido.get("icono", "•")
                            })
                        elif not permiso:
                            modulos_visibles.append({
                                "nombre": modulo_nombre,
                                "icono": modulo_contenido.get("icono", "•")
                            })
                
                if modulos_visibles:
                    cols = st.columns(min(3, len(modulos_visibles)))
                    for idx, modulo in enumerate(modulos_visibles):
                        with cols[idx % 3]:
                            st.info(f"{modulo['icono']} **{modulo['nombre']}**")
                st.markdown("---")
