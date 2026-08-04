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
from services.fotos import mostrar_foto_sidebar, mostrar_foto_ficha
from services.bigquery import probar_conexion
from services.vacaciones import (
    obtener_historial_vacaciones,
    obtener_saldo_vacaciones,
    generar_excel_vacaciones_empleado,
    obtener_tipos_incidencia,
    obtener_incidencias_empleado,
    obtener_resumen_incidencias
)


# ============================================================
# MÓDULO 1: IN & OUT
# ============================================================

def run_in_out(usuario):
    """
    Módulo In & Out
    """
    st.markdown("## 📊 In & Out - Personal Activo / Inactivo")
    st.caption("Lista de empleados activos e inactivos. Ordenados de más antiguos a más recientes.")
    
    empleados = obtener_activos_inactivos()
    
    if not empleados:
        st.info("No hay empleados registrados")
        return
    
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
                    workbook = writer.book
                    worksheet = writer.sheets['In_Out']
                    for i, col in enumerate(df.columns):
                        series = df[col].astype(str).replace('nan', '')
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
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    st.markdown(f"**{emp['nombre_completo']}**")
                with col2:
                    st.caption(f"📌 {emp.get('cargo_nombre', 'Sin cargo')}")
                with col3:
                    st.caption(f"📅 Salida: {emp.get('fecha_terminacion', '-')}")
                st.divider()
        else:
            st.success("🎉 No hay empleados inactivos")
    
    # ============================================================
    # ACTIVOS
    # ============================================================
    with st.expander(f"🟢 Activos ({len(activos)})", expanded=False):
        if activos:
            for emp in activos:
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    st.markdown(f"**{emp['nombre_completo']}**")
                with col2:
                    st.caption(f"📌 {emp.get('cargo_nombre', 'Sin cargo')}")
                with col3:
                    st.caption(f"📅 Ingreso: {emp.get('fecha_ingreso_empresa', '-')}")
                st.divider()
        else:
            st.info("No hay empleados activos")


# ============================================================
# MÓDULO 2: FICHA DE EMPLEADOS
# ============================================================

def run_ficha(usuario):
    """
    Módulo Ficha de Empleados (con buscador y ficha)
    """
    st.markdown("## 👤 Ficha de Empleados")
    st.caption("Busca un colaborador para ver su información completa.")
    
    # Inicializar estado
    if 'empleado_seleccionado_ficha' not in st.session_state:
        st.session_state['empleado_seleccionado_ficha'] = None
    
    # ============================================================
    # SIDEBAR: Buscador de empleados
    # ============================================================
    with st.sidebar:
        st.markdown("### 🔍 Buscar Colaborador")
        termino = st.text_input("Nombre o cédula", placeholder="Ej: Juan Pérez", key="busqueda_ficha")
        
        if termino and len(termino) >= 2:
            resultados = buscar_empleados(termino)
            if resultados:
                for emp in resultados:
                    with st.container():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            mostrar_foto_sidebar(emp.get('foto'), emp['nombre_completo'], size=50)
                        with col2:
                            st.markdown(f"**{emp['nombre_completo']}**")
                            st.caption(f"📌 {emp.get('cargo', 'Sin cargo')}")
                            estado = emp.get('estado', 'Desconocido')
                            color = {'Activo': '🟢', 'Inactivo': '🔴'}.get(estado, '⚪')
                            st.caption(f"{color} {estado}")
                        
                        if st.button(f"Ver ficha", key=f"btn_ficha_{emp['id_empleado']}"):
                            st.session_state['empleado_seleccionado_ficha'] = emp['id_empleado']
                            st.rerun()
                        
                        st.markdown("---")
            else:
                st.info("No se encontraron empleados")
        else:
            st.info("Escribe al menos 2 caracteres")
    
    # ============================================================
    # CONTENIDO PRINCIPAL
    # ============================================================
    if st.session_state['empleado_seleccionado_ficha']:
        mostrar_ficha_empleado(st.session_state['empleado_seleccionado_ficha'])
    else:
        st.info("🔍 Busca un colaborador en el panel izquierdo para ver su ficha.")


# ============================================================
# FUNCIÓN PARA MOSTRAR LA FICHA DEL EMPLEADO
# ============================================================

def mostrar_ficha_empleado(id_empleado):
    """
    Mostrar la ficha completa de un empleado.
    Sin restricciones de rol (acceso público dentro de la empresa).
    """
    empleado = obtener_empleado(id_empleado)
    
    if not empleado:
        st.error("❌ Empleado no encontrado")
        return
    # modulos/hexagon_colombia/nexo_people.py (al inicio de mostrar_ficha_empleado)

def mostrar_ficha_empleado(id_empleado):
    """
    Mostrar la ficha completa de un empleado.
    """
    empleado = obtener_empleado(id_empleado)
    
    if not empleado:
        st.error("❌ Empleado no encontrado")
        return
    
    # ============================================================
    # CSS PERSONALIZADO (DISEÑO PROFESIONAL)
    # ============================================================
    st.markdown("""
    <style>
        /* Tarjetas de métricas */
        .metric-card {
            background-color: #F8F9FA;
            border-radius: 12px;
            padding: 20px 15px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            border: 1px solid #EEEEEE;
            transition: all 0.2s ease;
        }
        .metric-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            border-color: #E63946;
        }
        .metric-card .value {
            font-size: 28px;
            font-weight: 700;
            color: #1D3557;
            margin: 5px 0;
        }
        .metric-card .label {
            font-size: 14px;
            color: #6C757D;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .metric-card .sub {
            font-size: 12px;
            color: #6C757D;
            margin-top: 4px;
        }
        .metric-card.primary .value { color: #1D3557; }
        .metric-card.success .value { color: #2E7D32; }
        .metric-card.warning .value { color: #F57C00; }
        .metric-card.danger .value { color: #C62828; }
        
        /* Título de sección */
        .section-title {
            font-size: 18px;
            font-weight: 600;
            color: #1D3557;
            margin-bottom: 16px;
            padding-bottom: 8px;
            border-bottom: 2px solid #E63946;
            display: inline-block;
        }
        
        /* Botones */
        .btn-download {
            background-color: #E63946;
            color: white;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn-download:hover {
            background-color: #C62828;
        }
    </style>
    """, unsafe_allow_html=True)
    

    # ============================================================
    # BOTÓN PARA VOLVER
    # ============================================================
    if st.button("← Volver a la búsqueda"):
        st.session_state['empleado_seleccionado_ficha'] = None
        st.rerun()
    
    # ============================================================
    # CABECERA: Foto + Datos principales
    # ============================================================
    with st.container():
        col1, col2 = st.columns([1, 3])
        
        with col1:
            mostrar_foto_ficha(empleado.get('foto'), empleado['nombre_completo'], size=150)
        
        with col2:
            st.markdown(f"## {empleado['nombre_completo']}")
            st.markdown(f"**{empleado.get('cargo', 'Sin cargo')}**")
            
            col_badges = st.columns(4)
            with col_badges[0]:
                estado = empleado.get('estado', 'Desconocido')
                color = {'Activo': '🟢', 'Inactivo': '🔴', 'Vacaciones': '🟡', 'Licencia': '🔵'}.get(estado, '⚪')
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
        "📈 Historial",
        "📝 Incidencias"
    ])

    # ============================================================
    # TAB 1: Personal
    # ============================================================
    with tabs[0]:
        st.markdown("### Información Personal")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Cédula:** {empleado.get('cedula', '-')}")
            
            fecha_nac = empleado.get('fecha_nacimiento')
            if fecha_nac and str(fecha_nac) != 'NaT':
                st.markdown(f"**Fecha Nacimiento:** {fecha_nac}")
                from datetime import date
                if isinstance(fecha_nac, str):
                    from datetime import datetime
                    fecha_nac = datetime.strptime(fecha_nac, '%Y-%m-%d').date()
                edad = date.today().year - fecha_nac.year - ((date.today().month, date.today().day) < (fecha_nac.month, fecha_nac.day))
                st.markdown(f"**Edad:** {edad} años")
            else:
                st.markdown(f"**Fecha Nacimiento:** -")
                st.markdown(f"**Edad:** -")
        
        with col2:
            st.markdown(f"**Teléfono:** {empleado.get('telefono', '-')}")
            st.markdown(f"**Correo:** {empleado.get('email_corporativo', '-')}")
            st.markdown(f"**Correo Personal:** {empleado.get('email_personal', '-')}")
    
    # ============================================================
    # TAB 2: Laboral
    # ============================================================
    with tabs[1]:
        st.markdown("### Información Laboral")
        
        fecha_ingreso = empleado.get('fecha_ingreso_empresa')
        if fecha_ingreso and str(fecha_ingreso) != 'NaT':
            st.markdown(f"**Ingreso a la empresa:** {fecha_ingreso}")
            from datetime import date
            if isinstance(fecha_ingreso, str):
                from datetime import datetime
                fecha_ingreso = datetime.strptime(fecha_ingreso, '%Y-%m-%d').date()
            delta = date.today() - fecha_ingreso
            años = delta.days // 365
            meses = (delta.days % 365) // 30
            st.markdown(f"**Antigüedad:** {años} años, {meses} meses")
        else:
            st.markdown(f"**Ingreso a la empresa:** -")
            st.markdown(f"**Antigüedad:** -")
        
        st.markdown(f"**Departamento:** {empleado.get('departamento', '-')}")
        st.markdown(f"**Supervisor:** {empleado.get('supervisor_nombre', '-')}")
    
    # ============================================================
    # TAB 3: Contactos (Próximamente)
    # ============================================================
    with tabs[2]:
        st.info("📞 Contactos de emergencia - Próximamente")
    
    # ============================================================
    # TAB 4: Dependientes (Próximamente)
    # ============================================================
    with tabs[3]:
        st.info("👨‍👩‍👧 Dependientes - Próximamente")
    
    # ============================================================
    # TAB 5: Documentos (Próximamente)
    # ============================================================
    with tabs[4]:
        st.info("📂 Documentos - Próximamente")
    
    # ============================================================
    # TAB 6: Historial (Próximamente)
    # ============================================================
    with tabs[5]:
        st.info("📈 Historial laboral - Próximamente")

    # ============================================================
    # TAB 7: Incidencias
    # ============================================================
    with tabs[6]:
        st.markdown("### 📝 Incidencias")
        
        # ============================================================
        # 1. FILTROS DE TIPO
        # ============================================================
        tipos = obtener_tipos_incidencia()
        opciones = ["📋 Todas"] + [f"🏖️ {t['nombre']}" for t in tipos if t['nombre'] != 'Vacaciones']
        
        # Vacaciones siempre primero
        opciones.insert(1, "🏖️ Vacaciones")
        
        filtro_seleccionado = st.radio(
            "Selecciona el tipo de incidencia",
            options=opciones,
            horizontal=True,
            key=f"filtro_incidencias_{id_empleado}"
        )
        
        # Limpiar el nombre del filtro
        if filtro_seleccionado == "📋 Todas":
            tipo_filtro = "Todas"
        else:
            # Quitar el emoji y el espacio
            tipo_filtro = filtro_seleccionado.split(" ")[1] if " " in filtro_seleccionado else filtro_seleccionado
        
        # ============================================================
        # 2. RESUMEN DE INCIDENCIAS
        # ============================================================
        resumen = obtener_resumen_incidencias(id_empleado)
        
        if resumen:
            # Mostrar tarjetas de resumen
            cols = st.columns(min(len(resumen), 4))
            for idx, item in enumerate(resumen):
                col_idx = idx % len(cols)
                with cols[col_idx]:
                    st.metric(
                        label=f"{item['tipo']}",
                        value=f"{item['total']} casos",
                        delta=f"{item['aprobadas']} aprobadas"
                    )
            st.markdown("---")
        
        # ============================================================
        # 3. HISTORIAL DE INCIDENCIAS
        # ============================================================
        st.markdown("#### 📋 Historial de Incidencias")
        
        incidencias = obtener_incidencias_empleado(id_empleado, tipo_filtro)
        
        if incidencias:
            df_incidencias = pd.DataFrame(incidencias)
            
            # Seleccionar columnas a mostrar
            columnas = ['fecha_inicio', 'fecha_fin', 'tipo', 'dias_calculados', 'estado']
            columnas_existentes = [col for col in columnas if col in df_incidencias.columns]
            
            if columnas_existentes:
                st.dataframe(
                    df_incidencias[columnas_existentes],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No hay datos para mostrar")
        else:
            st.info(f"No hay incidencias de tipo '{tipo_filtro}' registradas para este empleado")
        
        # ============================================================
        # 4. BOTÓN PARA DESCARGAR REPORTE
        # ============================================================
        if st.button("📥 Descargar historial de incidencias", key=f"descargar_incidencias_{id_empleado}"):
            excel_data = generar_excel_vacaciones_empleado(id_empleado)
            if excel_data:
                st.download_button(
                    label="✅ Descargar Excel",
                    data=excel_data,
                    file_name=f"incidencias_{empleado['nombre_completo']}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No hay datos para descargar")
