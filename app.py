# app.py
import streamlit as st
from modulos.core import login
from modulos.empresas.hexagon_panama.consultas import consultas
from modulos.empresas.hexagon_panama.hopsa import hopsa
from modulos.empresas.hexagon_panama.hopsa import control_almuerzos
from modulos.empresas.hexagon_panama.ifx import main as ifx  # 🆕 NUEVO IMPORT
from modulos.productos.crm import carga_documentos
from modulos.productos.inventarios import inventario
from modulos.empresas.farmazone import carga_reportes
from modulos.productos.nexo_people import nexo_people
from modulos.empresas.hexagon_panama.cobranza.carga_cartera import render as render_carga_cartera
# =====================
# CONFIGURACIÓN
# =====================
st.set_page_config(page_title="NEXO SUITE", page_icon="💻", layout="wide")

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
            "📈 IFX Network": {
                "tipo": "modulo",
                "funcion": lambda: ifx.run(usuario),
                "permiso": "IFX",
                "icono": "📈"
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
    "💰 Cobranza": {  # 🆕 NUEVO MÓDULO DE COBRANZA
        "icono": "💰",
        "tipo": "categoria",
        "modulos": {
            "📥 Carga de Cartera": {
                "tipo": "modulo",
                "funcion": lambda: render_carga_cartera(),
                "permiso": "COBRANZA",
                "icono": "📥"
            }
        }
    },
    "🏢 Hexagon Colombia": {
        "icono": "🏢",
        "tipo": "categoria",
        "modulos": {
            "📊 In & Out": {
                "tipo": "modulo",
                "funcion": lambda: nexo_people.run_in_out(usuario),
                "permiso": "NEXO_PEOPLE"
            },
            "👤 Ficha de Empleados": {
                "tipo": "modulo",
                "funcion": lambda: nexo_people.run_ficha(usuario),
                "permiso": "NEXO_PEOPLE"
            },
            "📋 Reporte de Vacaciones": {
                "tipo": "modulo",
                "funcion": lambda: nexo_people.run_reporte_vacaciones(usuario),
                "permiso": "NEXO_PEOPLE"
            }
        }
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
# =====================
# PERMISOS POR USUARIO
# =====================

ROLES = {
    "yaderleiva@gmail.com": {
        "CONSULTAS": True,
        "CARGA_DOCUMENTOS": True,
        "HOPSA": True,
        "CONTROL_ALMUERZOS": True,
        "INVENTARIO": True,
        "CARGA_REPORTES": True,
        "NEXO_PEOPLE": True,
        "IFX": True,
        "COBRANZA": True
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
    "aburgos2630@gmail.com": {
        "CARGA_REPORTES": True
    },
    "contenalfa@gmail.com": {
        "NEXO_PEOPLE": True,
        "IFX": True  # 🆕 Acceso a IFX
    },
    "contenbeta@gmail.com": {
        "CARGA_REPORTES": True
    },
    "yfalconhexagon@gmail.com": {
        "CONSULTAS": True
    },
    "yesturainhexagon@gmail.com": {
        "CONSULTAS": True
    },
    "sgonzalez.hex@gmail.com": {
        "CONSULTAS": True
    },
    "nalvaradohexagon@gmail.com": {
        "CONSULTAS": True
    },
    "hexagonclaudia@gmail.com": {
        "CONSULTAS": True,
        "IFX": True  # 🆕 Acceso a IFX
    },
    "clautotini1224@gmail.com": {
        "NEXO_PEOPLE": True
    },
}

# =====================
# FUNCIONES AUXILIARES
# =====================

def ejecutar_consultas():
    """Maneja el submenú de consultas"""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🔍 Tipo de consulta")
        
        consultas_config = ROLES.get(usuario, {}).get("CONSULTAS", [])
        
        if consultas_config is True or consultas_config == []:
            opciones = ["CSS", "TELÉFONOS NUEVOS", "CORREOS NUEVOS"]
        elif isinstance(consultas_config, list) and consultas_config:
            opciones = consultas_config
        else:
            st.error("❌ No tienes permisos para consultas")
            return
        
        st.markdown("**Selecciona el tipo de consulta:**")
        tipo = st.radio(
            "",
            opciones,
            key="tipo_consulta",
            index=0,
            horizontal=True
        )
        
        st.markdown("---")
    
    consultas.run(usuario, tipo)

def tiene_modulos_visibles(modulos_dict):
    """Verifica si existe al menos un módulo visible para el usuario"""
    for nombre, contenido in modulos_dict.items():
        if contenido.get("tipo") == "modulo":
            permiso = contenido.get("permiso")
            if permiso and ROLES.get(usuario, {}).get(permiso, False):
                return True
            elif not permiso:
                return True
        elif contenido.get("tipo") == "categoria" and contenido.get("modulos"):
            if tiene_modulos_visibles(contenido["modulos"]):
                return True
    return False

def mostrar_menu(modulos_dict, nivel=0):
    """Muestra menú jerárquico recursivo - SOLO módulos visibles"""
    for nombre, contenido in modulos_dict.items():
        if contenido.get("tipo") == "categoria" and contenido.get("modulos"):
            if tiene_modulos_visibles(contenido["modulos"]):
                with st.expander(f"{contenido.get('icono', '📁')} {nombre}"):
                    mostrar_menu(contenido["modulos"], nivel + 1)
        elif contenido.get("tipo") == "modulo" and "funcion" in contenido:
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
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("assets/NEXO.jpeg", width=120)
    st.markdown("---")
    
    mostrar_menu(MODULOS)
    
    if st.button("🚪 Cerrar sesión", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    st.markdown("---")
    st.caption(f"👤 {usuario}")
    st.caption("🏢 NEXO SUITE")

# =====================
# CONTENIDO PRINCIPAL
# =====================
if "modulo_activo" in st.session_state:
    st.session_state["modulo_activo"]()
else:
    st.title("🏢 NEXO SUITE")
    st.markdown("---")
    
    st.markdown(f"### ¡Bienvenido, {usuario}! 👋")
    st.markdown("Selecciona un módulo del menú lateral para comenzar.")
    st.markdown("---")
    
    for categoria_nombre, categoria_contenido in MODULOS.items():
        if categoria_contenido.get("tipo") == "categoria" and categoria_contenido.get("modulos"):
            if tiene_modulos_visibles(categoria_contenido["modulos"]):
                st.subheader(f"{categoria_contenido.get('icono', '📁')} {categoria_nombre}")
                
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
