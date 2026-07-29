# modulos/hexagon_colombia/nexo_people.py
"""
Módulo NEXO People - Gestión de Talento Humano para Hexagon Colombia.
"""

import streamlit as st
import pandas as pd
from services.empleados import (
    obtener_empleado,
    buscar_empleados,
    obtener_estadisticas_rapidas
)
from services.bigquery import probar_conexion


def run(usuario):
    """
    Página principal de NEXO People.
    """
    st.markdown("## 👥 NEXO People")
    st.markdown("---")
    
    # ----------------------------------------------------------
    # SIDEBAR: Buscador de empleados
    # ----------------------------------------------------------
    with st.sidebar:
        st.markdown("### 🔍 Buscar Colaborador")
        termino = st.text_input("Nombre o cédula", placeholder="Ej: Juan Pérez")
        
        if termino and len(termino) >= 2:
            resultados = buscar_empleados(termino)
            if resultados:
                # Mostrar resultados como tarjetas
                for emp in resultados:
                    with st.container():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            # Foto o avatar
                            if emp.get('foto'):
                                st.image(
                                    f"https://drive.google.com/uc?export=view&id={emp['foto']}",
                                    width=50
                                )
                            else:
                                st.image(
                                    f"https://ui-avatars.com/api/?name={emp['nombre_completo']}&size=50",
                                    width=50
                                )
                        with col2:
                            st.markdown(f"**{emp['nombre_completo']}**")
                            st.caption(f"📌 {emp.get('cargo', 'Sin cargo')}")
                            st.caption(f"🟢 {emp.get('estado', 'Desconocido')}")
                        
                        # Botón para ver ficha
                        if st.button(f"Ver ficha", key=f"btn_{emp['id_empleado']}"):
                            st.session_state['empleado_seleccionado'] = emp['id_empleado']
                            st.rerun()
                        
                        st.markdown("---")
            else:
                st.info("No se encontraron empleados")
        else:
            st.info("Escribe al menos 2 caracteres")
    
    # ----------------------------------------------------------
    # CONTENIDO PRINCIPAL: Ficha del empleado
    # ----------------------------------------------------------
    if 'empleado_seleccionado' in st.session_state:
        id_empleado = st.session_state['empleado_seleccionado']
        mostrar_ficha_empleado(id_empleado)
    else:
        # Mostrar dashboard de bienvenida
        mostrar_dashboard_inicio()


def mostrar_ficha_empleado(id_empleado):
    """
    Mostrar la ficha completa de un empleado.
    """
    empleado = obtener_empleado(id_empleado)
    
    if not empleado:
        st.error("❌ Empleado no encontrado")
        return
    
    # ----------------------------------------------------------
    # CABECERA: Foto + Datos principales
    # ----------------------------------------------------------
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if empleado.get('foto'):
            url_foto = f"https://drive.google.com/uc?export=view&id={empleado['foto']}"
            st.image(url_foto, width=200)
        else:
            avatar = f"https://ui-avatars.com/api/?name={empleado['nombre_completo']}&size=200"
            st.image(avatar, width=200)
    
    with col2:
        st.markdown(f"## {empleado['nombre_completo']}")
        st.markdown(f"**{empleado.get('cargo', 'Sin cargo')}**")
        st.markdown(f"🏢 **Empresa:** {empleado.get('empresa', '-')}")
        st.markdown(f"📁 **Proyecto:** {empleado.get('proyecto', '-')}")
        
        # Estado con color
        estado = empleado.get('estado', 'INACTIVO')
        color = {
            'ACTIVO': '🟢',
            'VACACIONES': '🟡',
            'INACTIVO': '🔴',
            'LICENCIA': '🔵'
        }.get(estado, '⚪')
        st.markdown(f"{color} **Estado:** {estado}")
    
    st.markdown("---")
    
    # ----------------------------------------------------------
    # TABS: Organizar la información
    # ----------------------------------------------------------
    tabs = st.tabs([
        "👤 Personal",
        "💼 Laboral",
        "📞 Contactos",
        "👨‍👩‍👧 Dependientes",
        "📂 Documentos",
        "📈 Historial"
    ])
    
    with tabs[0]:
        st.markdown("### Información Personal")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Cédula:** {empleado.get('cedula', '-')}")
            st.markdown(f"**Fecha Nacimiento:** {empleado.get('fecha_nacimiento', '-')}")
            # Calcular edad
            if empleado.get('fecha_nacimiento'):
                from datetime import date
                nac = empleado['fecha_nacimiento']
                if isinstance(nac, str):
                    from datetime import datetime
                    nac = datetime.strptime(nac, '%Y-%m-%d').date()
                edad = date.today().year - nac.year - ((date.today().month, date.today().day) < (nac.month, nac.day))
                st.markdown(f"**Edad:** {edad} años")
        with col2:
            st.markdown(f"**Teléfono:** {empleado.get('telefono', '-')}")
            st.markdown(f"**Correo:** {empleado.get('email_corporativo', '-')}")
            st.markdown(f"**Correo Personal:** {empleado.get('email_personal', '-')}")
    
    with tabs[1]:
        st.markdown("### Información Laboral")
        st.markdown(f"**Ingreso a la empresa:** {empleado.get('fecha_ingreso_empresa', '-')}")
        
        # Calcular antigüedad
        if empleado.get('fecha_ingreso_empresa'):
            from datetime import date
            ing = empleado['fecha_ingreso_empresa']
            if isinstance(ing, str):
                from datetime import datetime
                ing = datetime.strptime(ing, '%Y-%m-%d').date()
            delta = date.today() - ing
            años = delta.days // 365
            meses = (delta.days % 365) // 30
            st.markdown(f"**Antigüedad:** {años} años, {meses} meses")
        
        st.markdown(f"**Departamento:** {empleado.get('departamento', '-')}")
        st.markdown(f"**Supervisor:** {empleado.get('supervisor_nombre', '-')}")
    
    with tabs[2]:
        st.markdown("### Contactos de Emergencia")
        # TODO: Implementar consulta a contactos_emergencia
        st.info("📞 Próximamente: Lista de contactos de emergencia")
    
    with tabs[3]:
        st.markdown("### Dependientes")
        # TODO: Implementar consulta a dependientes
        st.info("👨‍👩‍👧 Próximamente: Lista de dependientes")
    
    with tabs[4]:
        st.markdown("### Documentos")
        # TODO: Implementar consulta a documentos
        st.info("📂 Próximamente: Lista de documentos")
    
    with tabs[5]:
        st.markdown("### Historial Laboral")
        # TODO: Implementar consulta a historial_laboral
        st.info("📈 Próximamente: Línea de tiempo laboral")


def mostrar_dashboard_inicio():
    """
    Dashboard de bienvenida para NEXO People.
    """
    st.markdown("### 📊 Resumen de Empleados")
    
    stats = obtener_estadisticas_rapidas()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Empleados",
            value=stats.get('total_empleados', 0)
        )
    
    with col2:
        st.metric(
            label="🟢 Activos",
            value=stats.get('activos', 0)
        )
    
    with col3:
        st.metric(
            label="🔴 Inactivos",
            value=stats.get('inactivos', 0)
        )
    
    with col4:
        st.metric(
            label="🟡 Vacaciones",
            value=stats.get('vacaciones', 0)
        )
    
    st.markdown("---")
    st.info("🔍 Usa el buscador en el panel izquierdo para ver la ficha de un empleado.")
