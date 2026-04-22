""")

archivo = st.file_uploader("Subir archivo", type=['csv', 'xlsx'], key="upload_agentes")

if archivo:
    try:
        if archivo.name.endswith('.csv'):
            df = pd.read_csv(archivo)
        else:
            df = pd.read_excel(archivo)
        
        # Validar columnas minimas
        if 'id_asesor' not in df.columns or 'nombre' not in df.columns:
            st.error("❌ Faltan columnas: id_asesor, nombre")
            return
        
        # Agregar id_llamadas si no existe
        if 'id_llamadas' not in df.columns:
            df['id_llamadas'] = df['id_asesor']
        if 'supervisor' not in df.columns:
            df['supervisor'] = ''
        
        # Limpiar datos
        df['id_asesor'] = df['id_asesor'].astype(str).str.strip()
        df['nombre'] = df['nombre'].astype(str).str.strip()
        df['id_llamadas'] = df['id_llamadas'].astype(str).str.strip()
        
        st.success(f"✅ {len(df)} agentes cargados")
        st.dataframe(df.head(), use_container_width=True)
        
        if st.button("💾 Guardar en BigQuery"):
            client = init_bq_client()
            df['fecha_actualizacion'] = datetime.datetime.now()
            
            job = client.load_table_from_dataframe(
                df, TABLE_ASESORES,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
            )
            job.result()
            st.success("✅ Agentes guardados correctamente")
            st.rerun()
            
    except Exception as e:
        st.error(f"Error: {e}")

def subir_informacion():
st.subheader("📂 Subir información del día")

# Verificar agentes
agentes = cargar_agentes()
if agentes is None or agentes.empty:
    st.warning("⚠️ Primero carga los agentes en 'Gestionar Agentes'")
    if st.button("Ir a Gestionar Agentes"):
        st.session_state.menu_hopsa = "Agentes"
        st.rerun()
    return

fecha_reporte = st.date_input("Fecha del reporte", datetime.date.today())
st.info(f"📋 {len(agentes)} agentes activos")

st.markdown("---")
st.markdown("### 1. Archivos del día")

col1, col2, col3 = st.columns(3)

with col1:
    ventas_file = st.file_uploader("📊 Ventas", type=['csv', 'xlsx'], key="ventas")
    if ventas_file:
        st.caption("Columnas: id_asesor, Venta, Factura")

with col2:
    llamadas_file = st.file_uploader("📞 Llamadas", type=['csv'], key="llamadas")
    if llamadas_file:
        st.caption("Columna: id_asesor (puede ser el ID alternativo)")

with col3:
    cotizaciones_file = st.file_uploader("📝 Cotizaciones", type=['csv', 'xlsx'], key="cotizaciones")
    if cotizaciones_file:
        st.caption("Columna: Vendedor (id_asesor)")

st.markdown("---")
st.markdown("### 2. Datos manuales")

# Modo rapido
modo_rapido = st.checkbox("⚡ Modo rapido - mismo valor para todos")

if modo_rapido:
    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        leads_default = st.number_input("Leads", min_value=0, value=0)
    with col_b:
        nps_default = st.number_input("NPS", min_value=0, max_value=10, value=0)
    with col_c:
        pra_default = st.number_input("PRA 90%", min_value=0, max_value=100, value=90)
    with col_d:
        asistencia_default = st.selectbox("Asistencia", [100, 0], 
                                         format_func=lambda x: "✅ Asistio" if x == 100 else "❌ No asistio")

with st.form("datos_manuales_form"):
    datos_manuales = []
    
    for _, row in agentes.iterrows():
        with st.container():
            st.markdown(f"**👤 {row['nombre']}** (ID ventas: `{row['id_asesor']}`, ID llamadas: `{row.get('id_llamadas', row['id_asesor'])}`)")
            
            if modo_rapido:
                leads = leads_default
                nps = nps_default
                pra = pra_default
                asistencia = asistencia_default
                st.caption(f"📊 Leads: {leads} | ⭐ NPS: {nps} | 🎯 PRA: {pra}% | 📅 Asistencia: {'✅' if asistencia == 100 else '❌'}")
            else:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    leads = st.number_input("Leads", min_value=0, value=0, key=f"leads_{row['id_asesor']}")
                with col2:
                    nps = st.number_input("NPS", min_value=0, max_value=10, value=0, key=f"nps_{row['id_asesor']}")
                with col3:
                    pra = st.number_input("PRA 90%", min_value=0, max_value=100, value=90, key=f"pra_{row['id_asesor']}")
                with col4:
                    asistencia = st.selectbox("Asistencia", [100, 0], 
                                             format_func=lambda x: "✅" if x == 100 else "❌",
                                             key=f"asis_{row['id_asesor']}")
            
            datos_manuales.append({
                "id_asesor": str(row['id_asesor']),
                "leads": leads,
                "nps": nps,
                "pra_90": pra,
                "asistencia": asistencia
            })
            st.markdown("---")
    
    submit = st.form_submit_button("🚀 Procesar y guardar", type="primary")

if submit:
    if not all([ventas_file, llamadas_file, cotizaciones_file]):
        st.error("❌ Faltan archivos por subir")
        return
    
    try:
        client = init_bq_client()
        df_manual = pd.DataFrame(datos_manuales)
        
        # 1. Procesar ventas
        if ventas_file.name.endswith('.csv'):
            df_ventas = pd.read_csv(ventas_file)
        else:
            df_ventas = pd.read_excel(ventas_file)
        
        df_ventas['id_asesor'] = df_ventas['id_asesor'].astype(str)
        ventas_agg = df_ventas.groupby('id_asesor').agg(
            ventas=('Venta', 'sum'),
            cierres=('Factura', 'count')
        ).reset_index()
        
        # 2. Procesar llamadas (con mapeo de IDs)
        df_llamadas = pd.read_csv(llamadas_file)
        df_llamadas['id_original'] = df_llamadas['id_asesor'].astype(str)
        
        # Crear mapeo: id_llamadas -> id_asesor
        mapeo = dict(zip(agentes['id_llamadas'].astype(str), agentes['id_asesor'].astype(str)))
        df_llamadas['id_asesor'] = df_llamadas['id_original'].map(mapeo).fillna(df_llamadas['id_original'])
        
        llamadas_agg = df_llamadas.groupby('id_asesor').size().reset_index(name='llamadas')
        
        # 3. Procesar cotizaciones
        if cotizaciones_file.name.endswith('.csv'):
            df_cotizaciones = pd.read_csv(cotizaciones_file)
        else:
            df_cotizaciones = pd.read_excel(cotizaciones_file)
        
        df_cotizaciones['Vendedor'] = df_cotizaciones['Vendedor'].astype(str)
        cotizaciones_agg = df_cotizaciones.groupby('Vendedor').size().reset_index(name='cantidad_cotizaciones')
        cotizaciones_agg = cotizaciones_agg.rename(columns={'Vendedor': 'id_asesor'})
        
        # 4. Construir reporte final
        reporte = agentes.merge(ventas_agg, on='id_asesor', how='left')
        reporte = reporte.merge(llamadas_agg, on='id_asesor', how='left')
        reporte = reporte.merge(cotizaciones_agg, on='id_asesor', how='left')
        reporte = reporte.merge(df_manual, on='id_asesor', how='left')
        
        # Rellenar nulos
        for col in ['ventas', 'cierres', 'llamadas', 'cantidad_cotizaciones', 'leads', 'nps', 'pra_90', 'asistencia']:
            if col in reporte.columns:
                reporte[col] = reporte[col].fillna(0)
        
        # Calcular metricas
        reporte['conversion'] = (reporte['cierres'] / reporte['leads'].replace(0, 1)) * 100
        reporte['conversion'] = reporte['conversion'].round(2)
        reporte['ticket_promedio'] = (reporte['ventas'] / reporte['cierres'].replace(0, 1)).round(2)
        
        # Fechas
        reporte['fecha'] = fecha_reporte
        reporte['mes'] = fecha_reporte.strftime('%B')
        reporte['dia'] = fecha_reporte.strftime('%A')
        reporte['sem_mes'] = (fecha_reporte.day - 1) // 7 + 1
        reporte['sem_año'] = fecha_reporte.isocalendar()[1]
        reporte['año'] = fecha_reporte.year
        reporte['fecha_creacion'] = datetime.datetime.now()
        
        # Guardar reporte
        job = client.load_table_from_dataframe(
            reporte, TABLE_REPORTE,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
        )
        job.result()
        
        # Guardar datos manuales
        df_manual['fecha'] = fecha_reporte
        df_manual['actualizado_por'] = st.session_state.get('usuario', 'unknown')
        df_manual['timestamp'] = datetime.datetime.now()
        
        job_manual = client.load_table_from_dataframe(
            df_manual, TABLE_MANUAL,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
        )
        job_manual.result()
        
        st.success(f"✅ Reporte del {fecha_reporte} guardado exitosamente")
        
        # Resumen
        st.subheader("📊 Resumen del día")
        resumen = reporte[['nombre', 'leads', 'cierres', 'ventas', 'conversion']].copy()
        resumen['ventas'] = resumen['ventas'].round(2)
        st.dataframe(resumen, use_container_width=True)
        
        # Totales
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Ventas", f"${reporte['ventas'].sum():,.0f}")
        col2.metric("Total Cierres", f"{reporte['cierres'].sum():,.0f}")
        col3.metric("Total Leads", f"{reporte['leads'].sum():,.0f}")
        col4.metric("Conversion Promedio", f"{reporte['conversion'].mean():.1f}%")
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)

def descargar_reportes():
st.subheader("📥 Descargar reportes historicos")

client = init_bq_client()

col1, col2 = st.columns(2)
with col1:
    fecha_inicio = st.date_input("Fecha inicio", datetime.date.today() - datetime.timedelta(days=30))
with col2:
    fecha_fin = st.date_input("Fecha fin", datetime.date.today())

tipo = st.radio("Tipo de reporte", ["Diario por agente", "Consolidado por agente"])

if st.button("🔍 Generar reporte", type="primary"):
    try:
        if tipo == "Diario por agente":
            query = f"""
            SELECT 
                fecha,
                nombre as agente,
                supervisor,
                leads,
                cierres,
                ROUND(conversion, 2) as conversion,
                ROUND(ventas, 2) as ventas,
                llamadas,
                cantidad_cotizaciones,
                asistencia
            FROM `{TABLE_REPORTE}`
            WHERE fecha BETWEEN @fecha_inicio AND @fecha_fin
            ORDER BY fecha DESC, nombre
            """
        else:
            query = f"""
            SELECT 
                nombre as agente,
                supervisor,
                SUM(leads) as total_leads,
                SUM(cierres) as total_cierres,
                ROUND(AVG(conversion), 2) as conversion_promedio,
                ROUND(SUM(ventas), 2) as total_ventas,
                SUM(llamadas) as total_llamadas,
                ROUND(AVG(asistencia), 0) as asistencia_promedio
            FROM `{TABLE_REPORTE}`
            WHERE fecha BETWEEN @fecha_inicio AND @fecha_fin
            GROUP BY nombre, supervisor
            ORDER BY total_ventas DESC
            """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
                bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin),
            ]
        )
        
        df = client.query(query, job_config=job_config).to_dataframe()
        
        if df.empty:
            st.warning("No hay datos en el rango seleccionado")
        else:
            st.success(f"✅ {len(df)} registros encontrados")
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Descargar CSV",
                csv,
                f"hopsa_{tipo.replace(' ', '_')}_{fecha_inicio}_{fecha_fin}.csv",
                "text/csv"
            )
            
    except Exception as e:
        st.error(f"Error: {e}")

def run(usuario):
st.title("🎯 HOPSA - Gestion de Ventas")
st.caption(f"Usuario: {usuario}")

# Inicializar estado de menu
if 'menu_hopsa' not in st.session_state:
    st.session_state.menu_hopsa = "Agentes"

opcion = st.sidebar.radio(
    "Menu",
    ["Agentes", "Subir Informacion", "Reportes"],
    index=["Agentes", "Subir Informacion", "Reportes"].index(st.session_state.menu_hopsa)
)

st.session_state.menu_hopsa = opcion

if opcion == "Agentes":
    actualizar_agentes()
elif opcion == "Subir Informacion":
    subir_informacion()
else:
    descargar_reportes()
