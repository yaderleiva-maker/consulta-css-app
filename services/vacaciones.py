# services/vacaciones.py (agregar)

def obtener_tipos_incidencia():
    """
    Obtener la lista de tipos de incidencia para los filtros.
    """
    query = """
    SELECT id_tipo_incidencia, nombre
    FROM `nexo_people.catalogo_tipos_incidencia`
    WHERE estado = 'ACTIVO'
    ORDER BY nombre
    """
    df = ejecutar_query(query)
    return df.to_dict('records')


def obtener_incidencias_empleado(id_empleado, tipo_filtro=None):
    """
    Obtener incidencias de un empleado con filtro opcional por tipo.
    """
    query = """
    SELECT 
      i.id_incidencia,
      i.fecha_inicio,
      i.fecha_fin,
      i.dias_calculados,
      i.estado,
      i.observacion,
      i.fecha_creacion,
      t.nombre AS tipo
    FROM `nexo_people.incidencias` i
    JOIN `nexo_people.catalogo_tipos_incidencia` t ON i.id_tipo_incidencia = t.id_tipo_incidencia
    WHERE i.id_empleado = @id_empleado
    """
    
    if tipo_filtro and tipo_filtro != 'Todas':
        query += " AND t.nombre = @tipo_filtro"
    
    query += " ORDER BY i.fecha_inicio DESC"
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    if tipo_filtro and tipo_filtro != 'Todas':
        params.append({"name": "tipo_filtro", "type": "STRING", "value": tipo_filtro})
    
    df = ejecutar_query(query, params)
    return df.to_dict('records')


def obtener_resumen_incidencias(id_empleado):
    """
    Obtener resumen de todas las incidencias de un empleado.
    """
    query = """
    SELECT 
      t.nombre AS tipo,
      COUNT(*) AS total,
      SUM(i.dias_calculados) AS total_dias,
      COUNTIF(i.estado = 'Aprobado') AS aprobadas,
      COUNTIF(i.estado = 'Pendiente') AS pendientes
    FROM `nexo_people.incidencias` i
    JOIN `nexo_people.catalogo_tipos_incidencia` t ON i.id_tipo_incidencia = t.id_tipo_incidencia
    WHERE i.id_empleado = @id_empleado
    GROUP BY t.nombre
    ORDER BY t.nombre
    """
    
    params = [{"name": "id_empleado", "type": "STRING", "value": id_empleado}]
    df = ejecutar_query(query, params)
    return df.to_dict('records')
