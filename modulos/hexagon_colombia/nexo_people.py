# modulos/hexagon_colombia/nexo_people.py

import streamlit as st
import pandas as pd
from io import BytesIO
from services.empleados import (
    obtener_empleado,
    buscar_empleados,
    obtener_activos_inactivos,
    generar_excel_activos_inactivos
)
from services.bigquery import probar_conexion


def run(usuario):
    """
    Página principal de NEXO People.
    """
    st.markdown("## 👥 NEXO People")
    st.markdown("---")
    
    # Inicializar estado de navegación
    if 'pagina_actual' not in st.session_state:
        st.session_state['pagina_actual'] = 'in_out'
    if 'empleado_seleccionado' not in st.session_state:
        st.session_state['empleado_seleccionado'] = None
    
    # ============================================================
    # SIDEBAR: Buscador de empleados
    # ============================================================
    with st.sidebar:
        st.markdown("### 🔍 Buscar Colaborador")
        termino = st.text_input("Nombre o cédula", placeholder="Ej: Juan Pérez")
        
        if termino and len(termino) >= 2:
            resultados = buscar_empleados(termino)
            if resultados:
                for emp in resultados:
                    with st.container():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if emp.get('foto'):
                                st.image(
                                    f"https://drive.google.com/uc?export=view&id={emp['foto']}",
                                    width=50
                                )
                            else:
                                st.image(
                                    f"https://ui-avatars.com/api/?name={emp['nombre_completo']}&size=50&background=4A90E2&color=white",
                                    width=50
                                )
                        with col2:
                            st.markdown(f"**{emp['nombre_completo']}**")
                            st.caption(f"📌 {emp.get('cargo', 'Sin cargo')}")
                            estado = emp.get('estado', 'DESCONOCIDO')
                            color = {
                                'ACTIVO': '🟢',
                                'INACTIVO': '🔴'
                            }.get(estado, '⚪')
                            st.caption(f"{color} {estado}")
                        
                        if st.button(f"Ver ficha", key=f"btn_{emp['id_empleado']}"):
                            st.session_state['empleado_seleccionado'] = emp['id_empleado']
                            st.session_state['pagina_actual'] = 'ficha'
                            st.rerun()
                        
                        st.markdown("---")
            else:
                st.info("No se encontraron empleados")
        else:
            st.info("Escribe al menos 2 caracteres")
    
    # ============================================================
    # CONTENIDO PRINCIPAL
    # ============================================================
    
    # Si hay un empleado seleccionado, mostrar ficha
    if st.session_state['empleado_seleccionado']:
        mostrar_ficha_empleado(st.session_state['empleado_seleccionado'])
        return
    
    # Si no, mostrar In & Out
    mostrar_in_out()


# modulos/hexagon_colombia/nexo_people.py

# modulos/hexagon_colombia/nexo_people.py

# modulos/hexagon_colombia/nexo_people.py

# modulos/hexagon_colombia/nexo_people.py

def mostrar_in_out():
    """
    Módulo In & Out: Lista de activos e inactivos.
    """
    st.markdown("### 📊 In & Out - Personal Activo / Inactivo")
    st.caption("Lista de empleados activos e inactivos. Ordenados de más antiguos a más recientes.")
    
    empleados = obtener_activos_inactivos()
    
    if not empleados:
        st.info("No hay empleados registrados")
        return
    
    # Separar activos e inactivos usando estado_nombre
    inactivos = [e for e in empleados if e.get('estado_nombre') == 'Inactivo']
    activos = [e for e in empleados if e.get('estado_nombre') == 'Activo']
    
    # ============================================================
    # BOTÓN PARA DESCARGAR EXCEL
    # ============================================================
    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        if st.button("📥 Descargar Excel", use_container_width=True):
            df = generar_excel_activos_inactivos()
            if not df.empty:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, sheet_name='In_Out', index=False)
                    
                    # Ajustar ancho de columnas automáticamente
                    workbook = writer.book
                    worksheet = writer.sheets['In_Out']
                    
                    for i, col in enumerate(df.columns):
                        # Calcular longitud máxima de la columna
                        series = df[col].astype(str)
                        # Reemplazar 'nan' por cadena vacía para el cálculo
                        series = series.replace('nan', '')
                        max_len = series.str.len().max() if not series.empty else 0
                        col_len = max(max_len, len(col)) + 2
                        worksheet.set_column(i, i, min(col_len, 50))
                
                st.download_button(
                    label="✅ Descargar Excel",
                    data=output.getvalue(),
                    file_name="in_out_empleados.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    # ============================================================
    # INACTIVOS
    # ============================================================
    with st.expander(f"🔴 Inactivos ({len(inactivos)})", expanded=True):
        if inactivos:
            for emp in inactivos:
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                with col1:
                    st.markdown(f"**{emp['nombre_completo']}**")
                with col2:
                    cargo = emp.get('cargo_nombre', 'Sin cargo')
                    st.caption(f"📌 {cargo}")
                with col3:
                    fecha_salida = emp.get('fecha_terminacion', '')
                    st.caption(f"📅 Salida: {fecha_salida}")
                with col4:
                    if st.button(f"Ver", key=f"ver_{emp['id_empleado']}"):
                        st.session_state['empleado_seleccionado'] = emp['id_empleado']
                        st.session_state['pagina_actual'] = 'ficha'
                        st.rerun()
                st.divider()
        else:
            st.success("🎉 No hay empleados inactivos")
    
    # ============================================================
    # ACTIVOS
    # ============================================================
    with st.expander(f"🟢 Activos ({len(activos)})", expanded=False):
        if activos:
            for emp in activos:
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                with col1:
                    st.markdown(f"**{emp['nombre_completo']}**")
                with col2:
                    cargo = emp.get('cargo_nombre', 'Sin cargo')
                    st.caption(f"📌 {cargo}")
                with col3:
                    fecha_ingreso = emp.get('fecha_ingreso_empresa', '')
                    st.caption(f"📅 Ingreso: {fecha_ingreso}")
                with col4:
                    if st.button(f"Ver", key=f"ver_{emp['id_empleado']}"):
                        st.session_state['empleado_seleccionado'] = emp['id_empleado']
                        st.session_state['pagina_actual'] = 'ficha'
                        st.rerun()
                st.divider()
        else:
            st.info("No hay empleados activos")
    
    st.info("👆 Haz clic en 'Ver' para abrir la ficha del empleado.")
def mostrar_ficha_empleado(id_empleado):
    """
    Mostrar la ficha completa de un empleado.
    """
    empleado = obtener_empleado(id_empleado)
    
    if not empleado:
        st.error("❌ Empleado no encontrado")
        return
    
    # ============================================================
    # Botón para volver al In & Out
    # ============================================================
    if st.button("← Volver a In & Out"):
        st.session_state['empleado_seleccionado'] = None
        st.session_state['pagina_actual'] = 'in_out'
        st.rerun()
    
    # ============================================================
    # CABECERA: Foto + Datos principales
    # ============================================================
    with st.container():
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if empleado.get('foto'):
                url_foto = f"https://drive.google.com/uc?export=view&id={empleado['foto']}"
                st.image(url_foto, width=150)
            else:
                avatar = f"https://ui-avatars.com/api/?name={empleado['nombre_completo']}&size=150&background=4A90E2&color=white"
                st.image(avatar, width=150)
        
        with col2:
            st.markdown(f"## {empleado['nombre_completo']}")
            st.markdown(f"**{empleado.get('cargo', 'Sin cargo')}**")
            
            col_badges = st.columns(4)
            with col_badges[0]:
                estado = empleado.get('estado', 'DESCONOCIDO')
                color = {'ACTIVO': '🟢', 'INACTIVO': '🔴', 'VACACIONES': '🟡', 'LICENCIA': '🔵'}.get(estado, '⚪')
                st.markdown(f"{color} **{estado}**")
            with col_badges[1]:
                if empleado.get('fecha_ingreso_empresa'):
                    from datetime import date
                    ing = empleado['fecha_ingreso_empresa']
                    if isinstance(ing, str):
                        from datetime import datetime
                        ing = datetime.strptime(ing, '%Y-%m-%d').date()
                    delta = date.today() - ing
                    años = delta.days // 365
                    st.markdown(f"📅 **{años} años**")
            with col_badges[2]:
                st.markdown(f"🏢 {empleado.get('empresa', '-')}")
            with col_badges[3]:
                st.markdown(f"📁 {empleado.get('proyecto', '-')}")
    
    st.markdown("---")
    
    # ============================================================
    # TABS: Información detallada
    # ============================================================
    tabs = st.tabs([
        "👤 Personal",
        "💼 Laboral",
        "📞 Contactos",
        "👨‍👩‍👧 Dependientes",
        "📂 Documentos",
        "📈 Historial"
    ])
    
    # ... (resto igual que antes)
