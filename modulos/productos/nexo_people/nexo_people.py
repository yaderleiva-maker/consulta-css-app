# modulos/hexagon_colombia/nexo_people.py

import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import date, datetime
from calendar import monthrange
from services.empleados import (
    obtener_empleado,
    buscar_empleados,
    obtener_activos_inactivos,
    generar_excel_activos_inactivos,
    obtener_lista_empleados
)
from services.fotos import mostrar_foto_sidebar, mostrar_foto_ficha
from services.bigquery import probar_conexion
from services.helpers import formatear_numero

# 🔥 UNA SOLA IMPORTACIÓN DE VACACIONES
from services.vacaciones import (
    obtener_historial_vacaciones,
    obtener_saldo_vacaciones,
    generar_excel_vacaciones_empleado,
    obtener_tipos_incidencia,
    obtener_incidencias_empleado,
    obtener_resumen_incidencias,
    ejecutar_merge_calculo,
    contar_incidencias_pendientes,
    obtener_reporte_vacaciones,
    generar_excel_reporte_vacaciones,
    obtener_dashboard_vacaciones
)

# ============================================================
# MÓDULO 1: IN & OUT
# ============================================================

# modulos/hexagon_colombia/nexo_people.py

def run_in_out(usuario):
    """
    Módulo In & Out
    """
    st.markdown("## 📊 In & Out - Personal Activo / Inactivo")
    st.caption("Lista de empleados activos e inactivos. Ordenados de más antiguos a más recientes.")
    
    # ============================================================
    # BOTONES DE DESCARGA
    # ============================================================
    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        if st.button("📥 In & Out Excel", use_container_width=True):
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
    
    with col2:
        if st.button("📋 INFOEQUIPO Humano", use_container_width=True):
            with st.spinner("Generando reporte INFOEQUIPO..."):
                from services.empleados import obtener_reporte_infoequipo
                df = obtener_reporte_infoequipo()
                if not df.empty:
                    # 🔥 AÑADIR ENUMERACIÓN Y SEPARADORES
                    df_procesado = procesar_infoequipo(df)
                    excel_data = generar_excel_infoequipo(df_procesado)
                    if excel_data:
                        st.download_button(
                            label="✅ Descargar Excel",
                            data=excel_data,
                            file_name=f"infoequipo_humano_{date.today().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.warning("No hay datos para generar el reporte")
    
    # ============================================================
    # LISTA DE EMPLEADOS
    # ============================================================
    empleados = obtener_activos_inactivos()
    
    if not empleados:
        st.info("No hay empleados registrados")
        return
    
    inactivos = [e for e in empleados if e.get('estado_nombre') == 'Inactivo']
    activos = [e for e in empleados if e.get('estado_nombre') == 'Activo']
    
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
    #                           
    # ============================================================
def generar_excel_infoequipo(df):
    """
    Generar Excel para el reporte INFOEQUIPO HUMANO.
    """
    from io import BytesIO
    import pandas as pd
    import numpy as np
    
    if df.empty:
        return None
    
    df_excel = df.copy()
    
    # 🔥 LIMPIAR VALORES NaN Y NONE
    df_excel = df_excel.replace({np.nan: '', None: ''})
    
    # 🔥 FORMATO DE FECHAS
    for col in ['Fecha de inicio del contrato', 'Fecha de terminación de contrato']:
        if col in df_excel.columns:
            df_excel[col] = pd.to_datetime(df_excel[col], errors='coerce')
            df_excel[col] = df_excel[col].apply(
                lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else ''
            )
    
    # 🔥 ELIMINAR COLUMNA AUXILIAR
    if 'orden_estado' in df_excel.columns:
        df_excel = df_excel.drop(columns=['orden_estado'])
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_excel.to_excel(writer, sheet_name='INFOEQUIPO', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['INFOEQUIPO']
        
        formato_moneda = workbook.add_format({'num_format': '$ #,##0.00'})
        formato_texto = workbook.add_format({'num_format': '@'})
        
        for i, col in enumerate(df_excel.columns):
            max_len = max(df_excel[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
            
            if col == 'Salario':
                worksheet.set_column(i, i, 15, formato_moneda)
            elif col in ['Identificación', 'Teléfono']:
                worksheet.set_column(i, i, 15, formato_texto)
    
    return output.getvalue()
    

def procesar_infoequipo(df):
    """
    Procesa el DataFrame para agregar secciones:
    - PERSONAL INACTIVO (con su encabezado)
    - Separador
    - PERSONAL ACTIVO (con su encabezado repetido)
    """
    import pandas as pd

    df_proc = df.copy()

    # Columnas de datos (excluyendo la auxiliar 'orden_estado')
    columnas_datos = [col for col in df_proc.columns if col != "orden_estado"]
    columnas_salida = ["N°"] + columnas_datos

    # Separar inactivos y activos
    inactivos = df_proc[df_proc["orden_estado"] == 0]
    activos = df_proc[df_proc["orden_estado"] == 1]

    filas = []

    def fila_seccion(titulo):
        """Crea una fila con un título en la columna N°"""
        fila = {col: "" for col in columnas_salida}
        fila["N°"] = titulo
        return fila

    def fila_encabezados():
        """Crea una fila con los nombres de las columnas (igual al primer encabezado)"""
        # 🔥 Usar los nombres exactos de las columnas
        return {col: col for col in columnas_salida}

    # ============================================================
    # SECCIÓN: PERSONAL INACTIVO
    # ============================================================
    if not inactivos.empty:
        # 🔥 ENCABEZADO PRINCIPAL (se genera automáticamente con df.to_excel)
        # Pero agregamos el título de la sección
        filas.append(fila_seccion("PERSONAL INACTIVO"))

        for numero, (_, registro) in enumerate(inactivos.iterrows(), start=1):
            fila = registro.drop(labels=["orden_estado"]).to_dict()
            fila["N°"] = numero
            filas.append(fila)

    # ============================================================
    # SEPARADOR
    # ============================================================
    if not inactivos.empty and not activos.empty:
        filas.append(fila_seccion("—"))

    # ============================================================
    # SECCIÓN: PERSONAL ACTIVO (con encabezado repetido)
    # ============================================================
    if not activos.empty:
        filas.append(fila_seccion("PERSONAL ACTIVO"))

        # 🔥 REPETIR ENCABEZADO COMPLETO (con todas las columnas)
        filas.append(fila_encabezados())

        for numero, (_, registro) in enumerate(activos.iterrows(), start=1):
            fila = registro.drop(labels=["orden_estado"]).to_dict()
            fila["N°"] = numero
            filas.append(fila)

    # ============================================================
    # CREAR DATAFRAME FINAL
    # ============================================================
    df_resultado = pd.DataFrame(filas, columns=columnas_salida)

    return df_resultado

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


def mostrar_ficha_empleado(id_empleado):
    """
    Mostrar la ficha completa de un empleado.
    """
    empleado = obtener_empleado(id_empleado)
    
    if not empleado:
        st.error("❌ Empleado no encontrado")
        return
    
    # ============================================================
    # CSS PERSONALIZADO (DISEÑO CORPORATIVO CON TARJETAS)
    # ============================================================
    st.markdown("""
    <style>
        /* ESTILO GENERAL DE TARJETAS (ERP CORPORATIVO) */
        .section-card {
            background-color: #FFFFFF;
            border-radius: 14px;
            padding: 24px 24px 20px 24px;
            border: 1px solid #EAEAEA;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            margin: 20px 0;
            transition: all 0.3s ease;
        }
        .section-card:hover {
            box-shadow: 0 8px 22px rgba(0,0,0,0.08);
            transform: translateY(-2px);
        }
        
        /* TÍTULO DE SECCIÓN CON LÍNEA ROJA */
        .section-title-card {
            font-size: 18px;
            font-weight: 600;
            color: #1D3557;
            margin-bottom: 16px;
            padding-bottom: 10px;
            border-bottom: 3px solid #E63946;
            display: inline-block;
        }
        
        /* ESTILO DE PESTAÑAS TIPO CARPETAS (MODERNAS) */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
            background-color: #F7F9FC;
            border-radius: 10px 10px 0 0;
            padding: 8px 8px 0 8px;
            border-bottom: 2px solid #EAEAEA;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: transparent;
            border-radius: 8px 8px 0 0;
            padding: 10px 20px;
            font-weight: 500;
            color: #6C757D;
            transition: all 0.2s ease;
            border: none;
            border-bottom: 3px solid transparent;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background-color: #FFFFFF;
            color: #1D3557;
            border-bottom: 3px solid #E63946;
        }
        .stTabs [aria-selected="true"] {
            background-color: #FFFFFF !important;
            color: #E63946 !important;
            border-bottom: 3px solid #E63946;
            font-weight: 600;
            box-shadow: 0 -2px 8px rgba(0,0,0,0.04);
        }
        
        /* CONTENEDOR DE PESTAÑAS CON BORDE Y SOMBRA */
        .stTabs [data-baseweb="tab-panel"] {
            background-color: #FFFFFF;
            border-radius: 0 0 12px 12px;
            padding: 20px 8px 8px 8px;
            border: 1px solid #EAEAEA;
            border-top: none;
            box-shadow: 0 4px 12px rgba(0,0,0,0.04);
        }
        
        /* MÉTRICAS CORPORATIVAS */
        .metric-card {
            background-color: #FFFFFF;
            border-radius: 12px;
            padding: 20px 15px;
            text-align: center;
            border: 1px solid #EAEAEA;
            box-shadow: 0 2px 6px rgba(0,0,0,0.03);
            transition: all 0.3s ease;
        }
        .metric-card:hover {
            box-shadow: 0 8px 20px rgba(0,0,0,0.06);
            transform: translateY(-2px);
            border-color: #E63946;
        }
        .metric-card .value {
            font-size: 28px;
            font-weight: 700;
            color: #1D3557;
            margin: 5px 0;
        }
        .metric-card .label {
            font-size: 13px;
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

        /* RESUMEN DE VACACIONES CORPORATIVO */
        .vacation-summary {
            background-color: #FFFFFF;
            border-radius: 14px;
            padding: 24px 24px 20px 24px;
            border: 1px solid #EAEAEA;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            margin-bottom: 20px;
            transition: all 0.3s ease;
        }
        .vacation-summary:hover {
            box-shadow: 0 8px 22px rgba(0,0,0,0.08);
        }
        .vacation-summary .title {
            font-size: 16px;
            font-weight: 600;
            color: #1D3557;
            margin-bottom: 15px;
            padding-bottom: 8px;
            border-bottom: 3px solid #E63946;
            display: inline-block;
        }
        .vacation-summary .next {
            font-size: 14px;
            color: #457B9D;
            margin-top: 10px;
            text-align: center;
        }
        
        /* BARRA DE PROGRESO */
        .progress-bar {
            width: 100%;
            height: 10px;
            background-color: #EEEEEE;
            border-radius: 6px;
            margin-top: 12px;
            overflow: hidden;
        }
        .progress-bar .fill {
            height: 100%;
            background: linear-gradient(90deg, #E63946, #C62828);
            border-radius: 6px;
            transition: width 0.5s ease;
        }

        /* FILTROS */
        .filters {
            margin: 16px 0;
            padding: 8px 0;
            border-bottom: 2px solid #EAEAEA;
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
    # TAB 7: Incidencias (CON RESUMEN DE VACACIONES SIEMPRE VISIBLE)
    # ============================================================
    with tabs[6]:
        # 🔥 Ejecutar MERGE automáticamente al cargar la pestaña
        with st.spinner("Actualizando cálculos de incidencias..."):
            try:
                ejecutar_merge_calculo()
            except Exception as e:
                st.error(f"Error actualizando cálculos: {e}")
        
        # Título de la sección
        st.markdown('<p class="section-title">Incidencias</p>', unsafe_allow_html=True)

        # ============================================================
        # 1. RESUMEN DE VACACIONES (SIEMPRE VISIBLE)
        # ============================================================
        saldo_data = obtener_saldo_vacaciones(id_empleado)
        
        # Formatear números
        saldo_actual = formatear_numero(saldo_data['saldo_actual'])
        dias_ganados = formatear_numero(saldo_data['dias_ganados'])
        dias_usados = formatear_numero(saldo_data['dias_usados'])
        meses_trabajados = saldo_data['meses_trabajados']
        
        # Calcular porcentaje usado
        porcentaje = (saldo_data['dias_usados'] / saldo_data['dias_ganados']) * 100 if saldo_data['dias_ganados'] > 0 else 0
        porcentaje = min(porcentaje, 100)
        
        # Color del estado según el saldo
        if saldo_data['saldo_actual'] > 0:
            estado_color = "#2E7D32"
            estado_texto = f"🟢 {saldo_data['saldo_actual']:.1f} días disponibles"
        elif saldo_data['saldo_actual'] == 0:
            estado_color = "#F57C00"
            estado_texto = "🟡 Saldo en cero"
        else:
            estado_color = "#C62828"
            estado_texto = f"🔴 Saldo negativo: {saldo_data['saldo_actual']:.1f} días"
        
        st.markdown(f"""
        <div class="vacation-summary">
            <div class="title">🏖️ Resumen de Vacaciones</div>
            <div style="display: flex; gap: 20px; flex-wrap: wrap;">
                <div style="flex: 1; min-width: 120px; text-align: center;">
                    <div style="font-size: 14px; color: #6C757D;">Saldo Disponible</div>
                    <div style="font-size: 32px; font-weight: 700; color: {estado_color};">{saldo_actual}</div>
                    <div style="font-size: 12px; color: {estado_color};">{estado_texto}</div>
                </div>
                <div style="flex: 1; min-width: 120px; text-align: center;">
                    <div style="font-size: 14px; color: #6C757D;">Días Ganados</div>
                    <div style="font-size: 32px; font-weight: 700; color: #2E7D32;">{dias_ganados}</div>
                    <div style="font-size: 12px; color: #6C757D;">en {meses_trabajados} meses</div>
                </div>
                <div style="flex: 1; min-width: 120px; text-align: center;">
                    <div style="font-size: 14px; color: #6C757D;">Días Usados</div>
                    <div style="font-size: 32px; font-weight: 700; color: #F57C00;">{dias_usados}</div>
                    <div style="font-size: 12px; color: #6C757D;">{porcentaje:.0f}% utilizado</div>
                </div>
            </div>
            <div class="progress-bar">
                <div class="fill" style="width: {porcentaje:.0f}%;"></div>
            </div>
            <div class="next">📅 Próximas vacaciones: {saldo_data['proximas_vacaciones']}</div>
        </div>
        """, unsafe_allow_html=True)

        # ============================================================
        # 2. FILTROS DE INCIDENCIAS (SOLO UNO)
        # ============================================================
        st.markdown('<div class="filters">', unsafe_allow_html=True)
        
        tipos = obtener_tipos_incidencia()
        opciones = ["Todas"] + [t['nombre'] for t in tipos]
        
        # Vacaciones siempre primero
        if "VACACIONES" in opciones:
            opciones.remove("VACACIONES")
            opciones.insert(0, "VACACIONES")
        
        filtro_seleccionado = st.radio(
            "",
            options=opciones,
            horizontal=True,
            key=f"filtro_incidencias_{id_empleado}_radio",
            label_visibility="collapsed"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        tipo_filtro = filtro_seleccionado

        # ============================================================
        # 3. HISTORIAL DE INCIDENCIAS
        # ============================================================
        st.markdown("### Historial")

        incidencias = obtener_incidencias_empleado(id_empleado, tipo_filtro)

        if incidencias:
            df_incidencias = pd.DataFrame(incidencias)

            columnas = ['Inicio', 'Fin', 'tipo', 'Calculo', 'Estado', 'Descansa']
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
            st.info(f"No hay incidencias de tipo '{tipo_filtro}' registradas")

        # ============================================================
        # 4. BOTÓN DE DESCARGA
        # ============================================================
        if st.button("📄 Descargar historial", key=f"descargar_incidencias_{id_empleado}"):
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



# ============================================================
# MÓDULO 3: REPORTE DE VACACIONES
# ============================================================

def run_reporte_vacaciones(usuario):
    """
    Módulo de Reporte de Vacaciones.
    Incluye:
    1. Centro de Control de Vacaciones (dashboard)
    2. Reporte de Vacaciones por Quincena (original)
    """
    st.markdown("## 📋 Reporte de Vacaciones")
    st.caption("Centro de control y reportes operativos de vacaciones.")
    
    # ============================================================
    # TABS: Separar los dos módulos
    # ============================================================
    tab1, tab2 = st.tabs(["📊 Centro de Control", "📅 Reporte por Quincena"])
    
    # ============================================================
    # TAB 1: CENTRO DE CONTROL DE VACACIONES
    # ============================================================
    with tab1:
        st.markdown("### 🏖️ Centro de Control de Vacaciones")
        st.caption("Panorama completo de vacaciones de todos los colaboradores.")
        
        # ============================================================
        # OBTENER DATOS DEL DASHBOARD
        # ============================================================
        with st.spinner("Cargando datos de vacaciones..."):
            dashboard = obtener_dashboard_vacaciones()
        
        if not dashboard or dashboard['resumen']['total_empleados'] == 0:
            st.warning("No hay empleados activos para mostrar")
            return
        
        # ============================================================
        # 1. RESUMEN EJECUTIVO + SEMÁFORO (FUSIONADO)
        # ============================================================
        st.markdown("#### 📊 Resumen Ejecutivo")
        
        # Obtener datos del dashboard
        resumen = dashboard['resumen']
        distribucion = {item['estado']: item['cantidad'] for item in dashboard['distribucion']}
        
        # 🔥 TARJETAS FUSIONADAS
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.markdown(f"""
            <div style="background-color: #F8F9FA; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #EAEAEA; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #1D3557;">{resumen['total_empleados']}</div>
                <div style="font-size: 13px; color: #6C757D; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">👥 Empleados</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="background-color: #E8F5E9; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #C8E6C9; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #2E7D32;">{distribucion.get('🟢 Normal', 0)}</div>
                <div style="font-size: 13px; color: #2E7D32; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">🟢 Normal</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="background-color: #FFF3E0; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #FFE0B2; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #E65100;">{distribucion.get('🟡 Bajo', 0)}</div>
                <div style="font-size: 13px; color: #E65100; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">🟡 Bajo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div style="background-color: #E3F2FD; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #BBDEFB; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #0D47A1;">{distribucion.get('🟠 Acumulado', 0)}</div>
                <div style="font-size: 13px; color: #0D47A1; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">🟠 Acumulado</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div style="background-color: #FFEBEE; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #FFCDD2; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #C62828;">{resumen['negativos']}</div>
                <div style="font-size: 13px; color: #C62828; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">⛔ Negativo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col6:
            st.markdown(f"""
            <div style="background-color: #F3E5F5; border-radius: 12px; padding: 16px 12px; text-align: center; border: 1px solid #E1BEE7; box-shadow: 0 2px 4px rgba(0,0,0,0.04);">
                <div style="font-size: 28px; font-weight: 700; color: #6A1B9A;">{resumen['sin_vacaciones_12']}</div>
                <div style="font-size: 13px; color: #6A1B9A; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">⏰ +12 meses</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # ============================================================
        # 2. ALERTAS CRÍTICAS
        # ============================================================
        st.markdown("#### 🚨 Alertas Críticas")
        
        # 2.1 Saldo negativo
        if dashboard['resumen']['negativos'] > 0:
            with st.expander(f"🔴 {dashboard['resumen']['negativos']} empleados con saldo negativo", expanded=True):
                df_negativos = pd.DataFrame(dashboard['empleados_detalle'])
                df_negativos = df_negativos[df_negativos['estado_saldo'] == 'NEGATIVO']
                st.dataframe(
                    df_negativos[['nombres', 'apellidos', 'cedula', 'saldo', 'dias_usados', 'dias_ganados']],
                    use_container_width=True,
                    hide_index=True
                )
        
        # 2.2 Sin vacaciones > 12 meses
        if dashboard['resumen']['sin_vacaciones_12'] > 0:
            with st.expander(f"⏰ {dashboard['resumen']['sin_vacaciones_12']} empleados sin vacaciones en más de 12 meses", expanded=True):
                df_sin_12 = pd.DataFrame(dashboard['sin_vacaciones_12_meses'])
                st.dataframe(
                    df_sin_12[['nombres', 'apellidos', 'cedula', 'dias_sin_vacaciones', 'ultima_vacacion', 'saldo']],
                    use_container_width=True,
                    hide_index=True
                )
        
        # 2.3 Acumulado > 15 días
        if dashboard['resumen']['acumulados'] > 0:
            with st.expander(f"🟠 {dashboard['resumen']['acumulados']} empleados con más de 15 días acumulados", expanded=False):
                df_acumulados = pd.DataFrame(dashboard['empleados_detalle'])
                df_acumulados = df_acumulados[df_acumulados['estado_saldo'] == 'ACUMULADO']
                st.dataframe(
                    df_acumulados[['nombres', 'apellidos', 'cedula', 'saldo', 'ultima_vacacion', 'proxima_vacacion']],
                    use_container_width=True,
                    hide_index=True
                )
        
        # ============================================================
        # 3. VACACIONES POR MES
        # ============================================================
        if dashboard['vacaciones_por_mes']:
            st.markdown("---")
            st.markdown("#### 📅 Vacaciones Programadas por Mes")
            df_meses = pd.DataFrame(dashboard['vacaciones_por_mes'])
            st.bar_chart(df_meses.set_index('nombre_mes')['cantidad'])
        
        # ============================================================
        # 4. PRÓXIMOS 30 DÍAS
        # ============================================================
        if dashboard['proximas_30_dias']:
            st.markdown("---")
            st.markdown("#### 📅 Próximas Vacaciones (30 días)")
            df_proximas = pd.DataFrame(dashboard['proximas_30_dias'])
            st.dataframe(
                df_proximas[['nombres', 'apellidos', 'proxima_vacacion', 'proxima_vacacion_fin', 'saldo']],
                use_container_width=True,
                hide_index=True
            )
        
        # ============================================================
        # 5. TABLA FILTRABLE DE EMPLEADOS
        # ============================================================
        st.markdown("---")
        st.markdown("#### 🔎 Detalle de Empleados")
        
        df_detalle = pd.DataFrame(dashboard['empleados_detalle'])
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            estado_filtro = st.multiselect(
                "Estado de Saldo",
                options=['NEGATIVO', 'BAJO', 'NORMAL', 'ACUMULADO'],
                default=[]
            )
        with col2:
            deptos = sorted(df_detalle['departamento_nombre'].dropna().unique().tolist())
            depto_filtro = st.multiselect("Departamento", options=deptos, default=[])
        with col3:
            busqueda = st.text_input("🔍 Buscar por nombre o cédula", placeholder="Ej: Juan Pérez")
        
        # Aplicar filtros
        df_filtrado = df_detalle.copy()
        if estado_filtro:
            df_filtrado = df_filtrado[df_filtrado['estado_saldo'].isin(estado_filtro)]
        if depto_filtro:
            df_filtrado = df_filtrado[df_filtrado['departamento_nombre'].isin(depto_filtro)]
        if busqueda:
            df_filtrado = df_filtrado[
                df_filtrado['nombres'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['apellidos'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['cedula'].str.contains(busqueda, case=False, na=False)
            ]
        
        # Mostrar tabla
        st.dataframe(
            df_filtrado[['nombres', 'apellidos', 'cedula', 'departamento_nombre', 'supervisor_nombre', 'saldo', 'estado_saldo', 'ultima_vacacion', 'proxima_vacacion']],
            use_container_width=True,
            hide_index=True
        )
        
        # ============================================================
        # 6. BOTÓN PARA DESCARGAR EXCEL
        # ============================================================
        st.markdown("---")
        if st.button("📥 Descargar Reporte Completo", use_container_width=True):
            excel_data = generar_excel_reporte_vacaciones(df_detalle)
            if excel_data:
                st.download_button(
                    label="✅ Descargar Excel",
                    data=excel_data,
                    file_name=f"control_vacaciones_{date.today().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )    
    # ============================================================
    # TAB 2: REPORTE DE VACACIONES POR QUINCENA (ORIGINAL)
    # ============================================================
    with tab2:
        st.markdown("### 📅 Reporte de Vacaciones por Quincena")
        st.caption("Genera reportes de vacaciones por período, quincena o empleado.")
        
        # ============================================================
        # ALERTA DE INCIDENCIAS PENDIENTES
        # ============================================================
        pendientes = contar_incidencias_pendientes()
        if pendientes['total_pendientes'] > 0:
            st.warning(
                f"⚠️ Hay **{pendientes['total_pendientes']}** incidencias pendientes de aprobación "
                f"que afectan a **{pendientes['empleados_afectados']}** empleados."
            )
        
        # ============================================================
        # BOTÓN PARA ACTUALIZAR CÁLCULOS
        # ============================================================
        col_refresh, col_spacer = st.columns([1, 4])
        with col_refresh:
            if st.button("🔄 Actualizar cálculos", use_container_width=True):
                with st.spinner("Calculando días hábiles..."):
                    ejecutar_merge_calculo()
                    st.success("✅ Cálculos actualizados correctamente")
                    st.rerun()
        
        # ============================================================
        # FILTROS
        # ============================================================
        st.markdown("#### 🔍 Filtros")
        
        # Filtro por fechas
        col1, col2 = st.columns(2)
        with col1:
            fecha_inicio = st.date_input("Fecha de inicio", value=date(2026, 1, 1), key="reporte_quincena_fecha_inicio")
        with col2:
            fecha_fin = st.date_input("Fecha de fin", value=date.today(), key="reporte_quincena_fecha_fin")
        
        # Filtro por quincena
        quincena_opcion = st.selectbox(
            "Quincena",
            options=["Ambas", "Quincena 1 (1-15)", "Quincena 2 (16-31)"],
            key="reporte_quincena_opcion"
        )
        
        # Filtro por empleado
        empleados_opcion = st.selectbox(
            "Empleado (opcional)",
            options=["Todos"] + [f"{e['nombre_completo']}" for e in obtener_lista_empleados()],
            key="reporte_quincena_empleado"
        )
        
        # ============================================================
        # BOTÓN PARA GENERAR REPORTE
        # ============================================================
        if st.button("📊 Generar Reporte", use_container_width=True):
            with st.spinner("Generando reporte..."):
                # Ajustar por quincena
                if quincena_opcion == "Quincena 1 (1-15)":
                    fecha_fin = date(fecha_inicio.year, fecha_inicio.month, 15)
                elif quincena_opcion == "Quincena 2 (16-31)":
                    from calendar import monthrange
                    _, last_day = monthrange(fecha_inicio.year, fecha_inicio.month)
                    fecha_inicio = date(fecha_inicio.year, fecha_inicio.month, 16)
                    fecha_fin = date(fecha_inicio.year, fecha_inicio.month, last_day)
                
                # Determinar ID del empleado
                id_empleado = None
                if empleados_opcion != "Todos":
                    for emp in obtener_lista_empleados():
                        if f"{emp['nombre_completo']}" == empleados_opcion:
                            id_empleado = emp['id_empleado']
                            break
                
                # Obtener datos del reporte
                df = obtener_reporte_vacaciones(
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    id_empleado=id_empleado,
                    quincena=quincena_opcion
                )
                
                if df.empty:
                    st.warning("No hay vacaciones registradas para los filtros seleccionados")
                else:
                    st.success(f"✅ {len(df)} registros encontrados")
                    st.caption(f"📅 Período: {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}")
                    
                    # Mostrar datos
                    st.markdown("#### 📋 Resultados")
                    st.dataframe(df, use_container_width=True)
                    
                    # Resumen
                    st.markdown("#### 📊 Resumen")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        total_empleados = df['NOMBRE'].nunique() if 'NOMBRE' in df.columns else 0
                        st.metric("👥 Empleados", total_empleados)
                    with c2:
                        total_dias_habiles = df['DIA HABIL'].sum() if 'DIA HABIL' in df.columns else 0
                        st.metric("📅 Días hábiles totales", total_dias_habiles)
                    with c3:
                        total_dias_no_habiles = df['DIA NO HABIL'].sum() if 'DIA NO HABIL' in df.columns else 0
                        st.metric("📅 Días no hábiles", total_dias_no_habiles)
                    
                    # Botón para descargar Excel
                    st.markdown("#### 📥 Descargar Reporte")
                    excel_data = generar_excel_reporte_vacaciones(df)
                    if excel_data:
                        nombre_archivo = f"reporte_vacaciones_{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin.strftime('%Y%m%d')}_{quincena_opcion.replace(' ', '_')}.xlsx"
                        st.download_button(
                            label="📥 Descargar Excel",
                            data=excel_data,
                            file_name=nombre_archivo,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
# ============================================================
# FUNCIÓN ORQUESTADORA (MENÚ PRINCIPAL DE NEXO PEOPLE)
# ============================================================

def nexo_people(usuario):
    """
    Función orquestadora que maneja el menú principal de Nexo People.
    """
    opcion = st.sidebar.selectbox(
        "Nexo People",
        ["In & Out", "Ficha de empleados", "Reporte de vacaciones"],
        key="nexo_people_menu",
    )

    if opcion == "In & Out":
        run_in_out(usuario)
    elif opcion == "Ficha de empleados":
        run_ficha(usuario)
    else:
        run_reporte_vacaciones(usuario)


def obtener_dashboard_vacaciones():
    """
    Obtener indicadores agregados para el dashboard de vacaciones.
    SIN subconsultas correlacionadas.
    """
    query = """
    WITH 
    -- 1. Empleados activos con sus datos
    empleados_activos AS (
      SELECT 
        e.id_empleado,
        e.nombres,
        e.apellidos,
        e.cedula,
        e.fecha_ingreso_empresa,
        e.id_empresa,
        e.id_departamento,
        e.id_supervisor,
        emp.nombre AS empresa_nombre,
        dep.nombre AS departamento_nombre,
        CONCAT(sup.nombres, ' ', sup.apellidos) AS supervisor_nombre,
        est.nombre AS estado_empleado
      FROM `nexo_people.empleados` e
      LEFT JOIN `nexo_people.empresas` emp ON e.id_empresa = emp.id_empresa
      LEFT JOIN `nexo_people.catalogo_departamentos_empresa` dep ON e.id_departamento = dep.id_departamento
      LEFT JOIN `nexo_people.empleados` sup ON e.id_supervisor = sup.id_empleado
      LEFT JOIN `nexo_people.catalogo_estados_empleado` est ON e.id_estado_empleado = est.id_estado_empleado
      WHERE est.nombre = 'Activo'
    ),
    
    -- 2. Políticas de vacaciones (días por mes)
    politicas AS (
      SELECT 
        id_empresa,
        ROUND(dias_por_anio / 12, 2) AS dias_por_mes
      FROM `nexo_people.politicas_vacaciones`
      WHERE estado = 'ACTIVO'
    ),
    
    -- 3. Vacaciones usadas por empleado
    vacaciones_usadas AS (
      SELECT 
        id_empleado,
        COALESCE(SUM(dias_calculados), 0) AS dias_usados
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      GROUP BY id_empleado
    ),
    
    -- 4. Última vacación por empleado
    ultima_vacacion AS (
      SELECT 
        id_empleado,
        MAX(fecha_fin) AS ultima_vacacion
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      GROUP BY id_empleado
    ),
    
    -- 5. Próxima vacación por empleado
    proxima_vacacion AS (
      SELECT 
        id_empleado,
        fecha_inicio AS proxima_vacacion,
        fecha_fin AS proxima_vacacion_fin,
        ROW_NUMBER() OVER (PARTITION BY id_empleado ORDER BY fecha_inicio ASC) AS rn
      FROM `nexo_people.incidencias`
      WHERE id_tipo_incidencia = (
        SELECT id_tipo_incidencia 
        FROM `nexo_people.catalogo_tipos_incidencia` 
        WHERE nombre = 'VACACIONES'
      )
      AND estado = 'Aprobado'
      AND fecha_inicio >= CURRENT_DATE()
    )
    
    -- 6. Consulta principal
    SELECT 
      ea.id_empleado,
      ea.nombres,
      ea.apellidos,
      ea.cedula,
      ea.fecha_ingreso_empresa,
      ea.empresa_nombre,
      ea.departamento_nombre,
      ea.supervisor_nombre,
      ea.estado_empleado,
      
      -- Días ganados
      ROUND(
        DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25),
        2
      ) AS dias_ganados,
      
      -- Días usados
      COALESCE(vu.dias_usados, 0) AS dias_usados,
      
      -- Última vacación
      uv.ultima_vacacion,
      
      -- Próxima vacación (solo la primera)
      pv.proxima_vacacion,
      pv.proxima_vacacion_fin,
      
      -- Saldo
      ROUND(
        DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
        2
      ) AS saldo,
      
      -- Estado del saldo
      CASE 
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) < 0 THEN 'NEGATIVO'
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) < 5 THEN 'BAJO'
        WHEN ROUND(
          DATE_DIFF(CURRENT_DATE(), ea.fecha_ingreso_empresa, MONTH) * COALESCE(p.dias_por_mes, 1.25) - COALESCE(vu.dias_usados, 0),
          2
        ) <= 15 THEN 'NORMAL'
        ELSE 'ACUMULADO'
      END AS estado_saldo,
      
      -- Antigüedad de la última vacación
      CASE 
        WHEN uv.ultima_vacacion IS NULL THEN 'NUNCA'
        WHEN DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, MONTH) >= 12 THEN 'MAS_12_MESES'
        WHEN DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, MONTH) >= 6 THEN 'MAS_6_MESES'
        ELSE 'RECIENTE'
      END AS antiguedad_vacacion,
      
      -- Días sin vacaciones
      DATE_DIFF(CURRENT_DATE(), uv.ultima_vacacion, DAY) AS dias_sin_vacaciones
      
    FROM empleados_activos ea
    LEFT JOIN politicas p ON ea.id_empresa = p.id_empresa
    LEFT JOIN vacaciones_usadas vu ON ea.id_empleado = vu.id_empleado
    LEFT JOIN ultima_vacacion uv ON ea.id_empleado = uv.id_empleado
    LEFT JOIN proxima_vacacion pv ON ea.id_empleado = pv.id_empleado AND pv.rn = 1
    """
    
    from services.bigquery import ejecutar_query
    import pandas as pd
    from datetime import datetime
    
    df = ejecutar_query(query)
    
    if df.empty:
        return {
            "resumen": {
                "total_empleados": 0,
                "negativos": 0,
                "acumulados": 0,
                "bajos": 0,
                "normales": 0,
                "sin_vacaciones_12": 0
            },
            "distribucion": [],
            "vacaciones_por_mes": [],
            "proximas_30_dias": [],
            "sin_vacaciones_12_meses": [],
            "empleados_detalle": []
        }
    
    # 🔥 CONVERTIR FECHAS A DATETIME
    for col in ['fecha_ingreso_empresa', 'ultima_vacacion', 'proxima_vacacion', 'proxima_vacacion_fin']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 🔥 CALCULAR INDICADORES
    total_empleados = len(df)
    negativos = len(df[df['estado_saldo'] == 'NEGATIVO'])
    acumulados = len(df[df['estado_saldo'] == 'ACUMULADO'])
    bajos = len(df[df['estado_saldo'] == 'BAJO'])
    normales = len(df[df['estado_saldo'] == 'NORMAL'])
    sin_vacaciones_12 = len(df[df['antiguedad_vacacion'] == 'MAS_12_MESES'])
    
    # 🔥 VACACIONES POR MES (con manejo de NaT)
    df_proximas = df[df['proxima_vacacion'].notna()].copy()
    if not df_proximas.empty:
        df_proximas['mes'] = df_proximas['proxima_vacacion'].dt.month
        df_proximas['nombre_mes'] = df_proximas['proxima_vacacion'].dt.strftime('%B')
        vacaciones_por_mes = df_proximas.groupby(['mes', 'nombre_mes']).size().reset_index(name='cantidad')
        vacaciones_por_mes = vacaciones_por_mes.sort_values('mes').to_dict('records')
    else:
        vacaciones_por_mes = []
    
    # 🔥 PRÓXIMOS 30 DÍAS (con manejo de NaT)
    ahora = pd.Timestamp.now()
    df_proximas_30 = df[
        df['proxima_vacacion'].notna() & 
        (df['proxima_vacacion'] <= ahora + pd.Timedelta(days=30))
    ].copy()
    df_proximas_30 = df_proximas_30.sort_values('proxima_vacacion')
    proximas_30_dias = df_proximas_30.to_dict('records')
    
    # 🔥 SIN VACACIONES > 12 MESES
    df_sin_12 = df[df['antiguedad_vacacion'] == 'MAS_12_MESES'].copy()
    df_sin_12 = df_sin_12.sort_values('dias_sin_vacaciones', ascending=False)
    sin_vacaciones_12_meses = df_sin_12.to_dict('records')
    
    return {
        "resumen": {
            "total_empleados": total_empleados,
            "negativos": negativos,
            "acumulados": acumulados,
            "bajos": bajos,
            "normales": normales,
            "sin_vacaciones_12": sin_vacaciones_12
        },
        "distribucion": [
            {"estado": "🔴 Negativo", "cantidad": negativos, "color": "danger"},
            {"estado": "🟡 Bajo", "cantidad": bajos, "color": "warning"},
            {"estado": "🟢 Normal", "cantidad": normales, "color": "success"},
            {"estado": "🟠 Acumulado", "cantidad": acumulados, "color": "primary"}
        ],
        "vacaciones_por_mes": vacaciones_por_mes,
        "proximas_30_dias": proximas_30_dias,
        "sin_vacaciones_12_meses": sin_vacaciones_12_meses,
        "empleados_detalle": df.to_dict('records')
    }
