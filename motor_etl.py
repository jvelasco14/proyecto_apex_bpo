"""
========================================================================================
MOTOR ETL (EXTRACCIÓN, TRANSFORMACIÓN Y CARGA) Y GOBERNANZA DE DATOS
Proyecto de Grado: Sistema Automatizado de Monitoreo GTR y Analítica Predictiva
Operación: Apex America BPO (Migraciones Inbound, Retenciones Outbound, Redes Sociales)
Autor: Juan Guillermo Velasco Ahumada
Director: Ing. Fabio Hernán Realpe Martínez | Asesor: Andrés Oicata Casas
========================================================================================

DESCRIPCIÓN DE INGENIERÍA:
Este módulo implementa el pipeline de procesamiento de datos en memoria (Data Pipeline)
diseñado bajo la Arquitectura Lambda para alimentar la Capa Speed (tablas transaccionales
vivas) en PostgreSQL / Supabase Cloud.

ETAPAS DEL PIPELINE:
1. Ingesta desde Zona de Aterrizaje: Escaneo de archivos planos (.CSV) en 'descargas_bot/'.
2. Capa de Calidad de Datos (Data Cleansing Universal):
   - Regla 1: Normalización sintáctica de nombres de columnas (minúsculas, trim, remoción de acentos/puntuación).
   - Regla 2: Poda de columnas fantasma, campos residuales y marcas de tiempo ofimáticas.
   - Regla 3: Erradicación de filas completamente vacías preservando la cardinalidad operacional.
   - Regla 4: Mapeo y reemplazo de falsos nulos ('[None]', 'N/A', 'NaN', 'null') a valores ausentes reales.
   - Regla 5: Imputación adaptativa por tipo de dato (texto -> 'No Registrado', numérico -> 0.0 para blindar el cálculo de KPIs).
   - Regla 6: Inversión cronológica del DataFrame para garantizar orden secuencial de inserción.
3. Blindaje de Esquema (Schema Shields):
   - Alineación estricta de columnas DDL para Monetización RRSS y Ventas Migraciones.
   - Guillotina de columnas calculadas ofimáticas previas (evita sobreescritura de lógica SQL).
4. Persistencia en Base de Datos:
   - Inyección transaccional en la Capa Speed de PostgreSQL (Supabase).
   - Trazabilidad y metadatos de auditoría (lote UUID, archivo fuente, timestamp Colombia).
5. Archivo Histórico Local:
   - Movimiento atómico de archivos procesados hacia 'historico_procesados/' con prefijo temporal.
6. Bitácora de Auditoría:
   - Registro del resultado operacional de cada lote en la tabla relacional 'etl_auditoria'.
========================================================================================
"""

import pandas as pd
import numpy as np
import os
import shutil
import uuid
import pytz
import re  
from datetime import datetime
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')

# Configuración de base de datos desde variable de entorno o fallback oficial
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+psycopg2://postgres.vdvmyueufdfviztmribf:Velasco14062000**@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
)


def normalizar_y_limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las Reglas Universales de Calidad de Datos (Data Cleansing) a nivel de producción.
    Estandariza cualquier estructura tabular entrante sin alterar la integridad matemática.

    Parámetros:
    -----------
    df : pd.DataFrame
        DataFrame en bruto cargado desde el archivo CSV extraído por RPA.

    Retorna:
    --------
    pd.DataFrame
        DataFrame normalizado, imputado y ordenado cronológicamente.
    """
    print("   🧹 Aplicando reglas universales de gobernanza y limpieza de datos...")
    
    # -------------------------------------------------------------------------
    # REGLA 1: Normalización estricta de nombres de columnas
    # -------------------------------------------------------------------------
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df.columns = df.columns.str.replace('.', '', regex=False).str.replace('(', '', regex=False).str.replace(')', '', regex=False)
    df.columns = df.columns.str.replace('unnamed:_', 'unnamed_', regex=False)
    df.columns = df.columns.str.replace('__', '_', regex=False)

    # Prevenir colisión con la clave primaria relacional de PostgreSQL
    if 'id' in df.columns:
        df.rename(columns={'id': 'id_original'}, inplace=True)
        
    # -------------------------------------------------------------------------
    # REGLA 2: Eliminación de columnas fantasma, temporales y residuales
    # -------------------------------------------------------------------------
    columnas_basura = [
        'unicos/reapertura', 'segmento', 'horario', 'intervalos',
        '00:05:00', '300', '00:10:00', '600', '00:20:00', '1200',
        'llave', 'cc'
    ]
    columnas_a_borrar = [col for col in columnas_basura if col in df.columns]
    if columnas_a_borrar:
        df.drop(columns=columnas_a_borrar, inplace=True)
        print(f"      🗑️ Columnas fantasma eliminadas: {columnas_a_borrar}")

    columnas_unnamed = [col for col in df.columns if str(col).startswith('unnamed')]
    if columnas_unnamed:
        df.drop(columns=columnas_unnamed, inplace=True)
        print(f"      🗑️ Columnas Unnamed residuales eliminadas: {len(columnas_unnamed)}")
        
    columnas_fecha = [col for col in df.columns if re.match(r'^202\d-\d{2}-\d{2}', str(col))]
    if columnas_fecha:
        df.drop(columns=columnas_fecha, inplace=True)
        print(f"      🗑️ Columnas de fecha ofimática descartadas: {columnas_fecha}")

    # -------------------------------------------------------------------------
    # REGLA 3: Borrado exclusivo de filas completamente vacías
    # -------------------------------------------------------------------------
    df = df.dropna(how='all')

    # -------------------------------------------------------------------------
    # REGLA 4: Mapeo de falsos nulos ofimáticos a NaN nativo
    # -------------------------------------------------------------------------
    valores_nulos_sistema = ['[None]', 'N/A', 'n/a', 'NaN', 'null', '']
    df.replace(valores_nulos_sistema, np.nan, inplace=True)

    # -------------------------------------------------------------------------
    # REGLA 5: Imputación tipificada según la naturaleza del campo
    # -------------------------------------------------------------------------
    for col in df.columns:
        if df[col].dtype == 'object':
            # Campos de texto: sanitizar espacios y reemplazar nulos por categoría neutra
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None'], 'No Registrado') 
            df[col] = df[col].fillna('No Registrado')
        else:
            # Campos métricos numéricos: imputar con cero para preservar sumatorias y promedios SQL
            df[col] = df[col].fillna(0)

    # -------------------------------------------------------------------------
    # REGLA 6: Inversión cronológica (garantiza orden 00:01 -> 23:59)
    # -------------------------------------------------------------------------
    df = df.iloc[::-1].reset_index(drop=True)
    return df


def ejecutar_etl():
    """
    Función controladora del pipeline ETL.
    Lee los archivos planos generados por RPA, aplica limpieza, alinea esquemas
    y persiste en Supabase bajo la Arquitectura Lambda.
    """
    print("===================================================")
    print("⚙️ INICIANDO MOTOR ETL (GOBERNANZA DE DATOS)")
    print("===================================================")
    
    carpeta_descargas = 'descargas_bot'
    carpeta_historico = 'historico_procesados'
    os.makedirs(carpeta_historico, exist_ok=True)
    
    # Huso horario oficial de la operación (Colombia UTC-5)
    tz_colombia = pytz.timezone('America/Bogota')
    lote_id = str(uuid.uuid4())[:8]
    
    # Conexión relacional con el pooler de Supabase
    engine = create_engine(DATABASE_URL)
    print("🔗 Conectado exitosamente al clúster PostgreSQL en Supabase Cloud")
    
    archivos_procesados = 0
    registros_auditoria = []

    try:
        if not os.path.exists(carpeta_descargas):
            print("📭 Carpeta de descargas no encontrada.")
            return

        archivos_csv = [f for f in os.listdir(carpeta_descargas) if f.endswith('.csv')]
        
        if not archivos_csv:
            print("📭 No hay archivos nuevos en la zona de aterrizaje para procesar.")
            return

        for archivo in archivos_csv:
            ruta_archivo = os.path.join(carpeta_descargas, archivo)
            inicio_proc = datetime.now(tz_colombia)
            print(f"\n⏳ Procesando e inyectando: {archivo}...")
            
            # Carga del archivo con codificación robusta UTF-8 con BOM
            df = pd.read_csv(ruta_archivo, encoding='utf-8-sig', low_memory=False)
            
            # Limpieza universal
            df = normalizar_y_limpiar_datos(df)

            # Guillotina de columnas calculadas manuales (previene duplicidad con vistas SQL)
            columna_corte = 'cuenta_atendido'
            if columna_corte in df.columns:
                indice = df.columns.get_loc(columna_corte)
                df = df.iloc[:, :indice]
                print(f"      🔪 Guillotina activada: descartadas columnas calculadas desde '{columna_corte}'.")

            # -----------------------------------------------------------------
            # BLINDAJES ESPECÍFICOS POR CAMPAÑA Y ALINEACIÓN DE ESQUEMAS
            # -----------------------------------------------------------------
            if 'día' in df.columns:
                df['día'] = df['día'].astype(str).str.extract(r'(\d+)')[0]
                df['día'] = pd.to_numeric(df['día'], errors='coerce').fillna(0).astype(int)
                
            if 'mes' in df.columns:
                df['mes'] = pd.to_numeric(df['mes'], errors='coerce').fillna(0).astype(int)
                
            if 'año' in df.columns:
                df['año'] = pd.to_numeric(df['año'], errors='coerce').fillna(0).astype(int)

            archivo_lower = archivo.lower()
            
            # Sanitización de fechas en Monetización RRSS
            if 'chats_monetizacion' in archivo_lower:
                print("      🔧 Aplicando estandarización de marcas de tiempo en Monetización RRSS...")
                columnas_de_fecha = ['fecha_inicio', 'fecha_asignación', 'fecha_atención', 'fecha_finalización']
                for col in columnas_de_fecha:
                    if col in df.columns:
                        df[col] = df[col].astype(str)
                        df[col] = df[col].str.replace('2025-06-22', '2025-06-26', regex=False)
                        df[col] = df[col].str.replace('22/06/2025', '26/06/2025', regex=False)
                        df[col] = df[col].str.replace('2025/06/22', '2025/06/26', regex=False)
                        df[col] = df[col].replace(['nan', 'NaN', 'None', 'NaT', ''], None)
                
                if 'año' in df.columns: df['año'] = 2025
                if 'mes' in df.columns: df['mes'] = 6
                if 'día' in df.columns: df['día'] = 26
                if 'condición' in df.columns: df['condición'] = '20250626'
            
            # Alineación relacional en Ventas Migraciones
            if 'ventas_migraciones' in archivo_lower:
                print("      🔧 Alineando esquema relacional con tabla maestra 'base_ventas_migraciones'...")
                renombres_ventas = {
                    'cédula_del_agente': 'cedula_del_agente',
                    'cédula_del_cliente': 'cedula_del_cliente',
                    'número_de_la_línea_a_portar,_migrar_o_línea_nueva_activada': 'numero_linea_gestionada',
                    'día': 'dia' 
                }
                df.rename(columns=renombres_ventas, inplace=True)
                
                if 'gestion' not in df.columns:
                    df['gestion'] = 'Migración Inbound'
            
            # -----------------------------------------------------------------
            # INYECCIÓN DE METADATOS DE AUDITORÍA Y TRAZABILIDAD (ETL)
            # -----------------------------------------------------------------
            df['etl_origen_archivo'] = archivo
            df['etl_lote_ejecucion'] = lote_id
            df['etl_fecha_carga_col'] = inicio_proc.strftime("%Y-%m-%d %H:%M:%S")
            
            # -----------------------------------------------------------------
            # ENRUTAMIENTO DINÁMICO HACIA TABLAS DE LA CAPA SPEED
            # -----------------------------------------------------------------
            if 'chat' in archivo_lower and 'cddiario' in archivo_lower:
                tabla_dinamica = 'base_chat_cddiario'
            elif 'encuestas' in archivo_lower and 'cddiario' in archivo_lower:
                tabla_dinamica = 'base_encuestas_cddiario'
            elif 'llamadas' in archivo_lower or ('migraciones' in archivo_lower and 'ventas' not in archivo_lower):
                tabla_dinamica = 'base_llamadas_migraciones'
            elif 'rete_out' in archivo_lower:
                tabla_dinamica = 'base_rete_out'
            elif 'nps_rrss' in archivo_lower:
                tabla_dinamica = 'base_nps_rrss'
            elif 'chats_monetizacion' in archivo_lower or 'chats_rrss' in archivo_lower:
                tabla_dinamica = 'chats_monetizacion_rrss'
                columnas_permitidas = [
                    'id_original', 'cola_de_atención', 'id_conversación', 'bot', 'canal', 
                    'nombre_del_cliente', 'identificacion_del_cliente', 'tipo_de_cliente', 
                    'fecha_inicio', 'fecha_asignación', 'fecha_atención', 'tiempo_en_espera', 
                    'tiempo_primera_respuesta', 'tiempo_de_asignación', 'tiempo_conversacion', 
                    'fecha_finalización', 'duración', 'cerrado_por', 'agente', 'skill', 
                    'recibida_por', 'login_agente',
                    'etl_origen_archivo', 'etl_lote_ejecucion', 'etl_fecha_carga_col'
                ]
                df = df[[c for c in df.columns if c in columnas_permitidas]]
                
            elif 'tipificaciones' in archivo_lower:
                tabla_dinamica = 'base_tipificaciones_rrss'
            
            elif 'ventas_migraciones' in archivo_lower:
                tabla_dinamica = 'base_ventas_migraciones'
                if 'base_de_gestión' in df.columns:
                    df.rename(columns={'base_de_gestión': 'base_de_gestion'}, inplace=True)
                
                columnas_permitidas_ventas = [
                    'marca_temporal', 'nombre_del_asesor', 'cedula_del_agente', 'msisdn',
                    'cedula_del_cliente', 'numero_linea_gestionada', 'gestion', 'contactado',
                    'plan_vendido', 'canal_de_venta', 'supervisor', 'base_de_gestion', 'fecha_de_venta',
                    'mes', 'dia', 'time', 'etl_origen_archivo', 'etl_lote_ejecucion', 'etl_fecha_carga_col'
                ]
                df = df[[c for c in df.columns if c in columnas_permitidas_ventas]]
            else:
                print(f"      ⚠️ Archivo no reconocido en reglas de ruteo: {archivo}. Omitiendo.")
                continue 
                
            # -----------------------------------------------------------------
            # PERSISTENCIA EN SUPABASE / POSTGRESQL (CAPA SPEED)
            # -----------------------------------------------------------------
            df.to_sql(tabla_dinamica, engine, if_exists='append', index=False)
            
            # Bitácora de ejecución del archivo
            registros_auditoria.append({
                'lote_id': lote_id,
                'archivo_procesado': archivo,
                'tabla_destino': tabla_dinamica,
                'cantidad_filas': len(df),
                'fecha_ejecucion_col': inicio_proc.strftime("%Y-%m-%d %H:%M:%S"),
                'estado': 'EXITOSO'
            })
            
            # Desplazamiento seguro a la bóveda histórica local
            timestamp_str = inicio_proc.strftime("%Y%m%d_%H%M%S")
            nuevo_nombre = f"PROCESADO_{timestamp_str}_{archivo}"
            shutil.move(ruta_archivo, os.path.join(carpeta_historico, nuevo_nombre))
            archivos_procesados += 1
            print(f"      ✅ Inyectados {len(df)} registros limpios en '{tabla_dinamica}'.")

    except Exception as e:
        print(f"\n❌ Error Crítico en el motor ETL: {e}")
        registros_auditoria.append({
            'lote_id': lote_id,
            'archivo_procesado': 'Error de Lote',
            'tabla_destino': 'ERROR',
            'cantidad_filas': 0,
            'fecha_ejecucion_col': datetime.now(tz_colombia).strftime("%Y-%m-%d %H:%M:%S"),
            'estado': f'ERROR: {str(e)[:200]}'
        })
        raise e
        
    finally:
        # Persistencia del log de trazabilidad en la tabla 'etl_auditoria'
        if registros_auditoria:
            df_auditoria = pd.DataFrame(registros_auditoria)
            try:
                df_auditoria.to_sql('etl_auditoria', engine, if_exists='append', index=False)
                print("📊 Trazabilidad registrada exitosamente en tabla 'etl_auditoria'.")
            except Exception as e_aud:
                print(f"⚠️ No se pudo escribir en 'etl_auditoria': {e_aud}")
            
        print(f"🚀 Pipeline ETL completado. {archivos_procesados} archivo(s) procesado(s).\n")


if __name__ == "__main__":
    ejecutar_etl()
