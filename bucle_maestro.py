"""
========================================================================================
ORQUESTADOR CENTRAL DEL PIPELINE E2E (BUCLE MAESTRO INTRADÍA)
Proyecto de Grado: Sistema Automatizado de Monitoreo GTR y Analítica Predictiva
Operación: Apex America BPO (Campaña Inbound Migraciones, Outbound Retenciones, RRSS)
Autor: Juan Guillermo Velasco Ahumada
Director: Ing. Fabio Hernán Realpe Martínez | Asesor: Andrés Oicata Casas
========================================================================================

DESCRIPCIÓN DE ARQUITECTURA:
Este módulo actúa como el cerebro orquestador del sistema bajo Arquitectura Lambda.
Gobierna el flujo cíclico intradía continuo y desacoplado:

1. RPA Playwright (`bot_scraper.py`):
   - Extrae desatendidamente los cortes horarios desde Cari AI / Inconcert.
2. Motor ETL (`motor_etl.py`):
   - Normaliza, limpia y persiste transacciones en la Capa Speed (tablas vivas) de PostgreSQL.
3. Inteligencia Artificial Intradía (`ia_migraciones.py`, `ia_retenciones.py`, `ia_rrss.py`):
   - Detección de micro-anomalías individuales por asesor (Isolation Forest).
   - Proyección de demanda y fatiga de base (Run-Rate Forecasting).
   - Detección de riesgos de saturación frente a capacidad instalada (6.600 llam/h).
4. Cierres Escalonados por Campaña (Gobernanza Anti-Deadlocks):
   - 15:00 hrs -> Cierre de Retenciones Outbound (Migración a `hist_base_rete_out` + K-Means).
   - 16:00 hrs -> Cierre de Redes Sociales (Migración a tablas `hist_*` + K-Means con Feature Weighting).
   - 17:00 hrs -> Cierre de Migraciones Inbound (Migración a tablas `hist_*` + K-Means comercial con SSOT).
   El desacoplamiento horario previene la contención de bloqueos (deadlocks) y picos de memoria RAM.
5. Reinicio Nocturno y Gobernanza de Datos:
   - Purga de la Capa Speed (tablas vivas a cero) para el inicio de la siguiente jornada.
   - En modo producción, preserva íntegramente la memoria histórica en las tablas `hist_*`.

MODOS DE OPERACIÓN:
- MODO_PRODUCCION=true: Esperas reales de 1 hora (3600s), reposo nocturno hasta medianoche,
  sin eliminación de históricos.
- MODO_PRODUCCION=false (Modo Evaluación / Tesis): Simulación acelerada de cortes horarios
  para demostración interactiva ante jurados con fecha calibrada '2025-06-26'.
========================================================================================
"""

import os
import time
import requests
import pytz
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from sqlalchemy import create_engine, text

import bot_scraper
import motor_etl
from ia_migraciones import analizar_migraciones
from ia_retenciones import analizar_retenciones
from ia_rrss import analizar_rrss

# =============================================================================
# ⚙️ PARÁMETROS DE CONFIGURACIÓN DEL ORQUESTADOR
# =============================================================================
API_URL = os.getenv("CARI_API_URL", "http://127.0.0.1:5000/api/set_hora/")
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+psycopg2://postgres.vdvmyueufdfviztmribf:Velasco14062000**@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
)
MODO_PRODUCCION = os.getenv("MODO_PRODUCCION", "false").lower() == "true"

# Ventana operativa del contact center: Lote 06 (06:00-06:59 extraído a las 07:00) a Lote 16 (16:00-16:59 extraído a las 17:00)
LOTE_INICIAL = 6   
LOTE_FINAL = 16    

# Tiempos de espera entre cortes horarios
TIEMPO_ESPERA_ENTRE_HORAS = 3600 if MODO_PRODUCCION else 60
TIEMPO_ESPERA_REINICIO = 300  # Pausa de 5 minutos al cierre de demostración

# Fecha de calibración experimental para evaluación empírica
FECHA_PRUEBA = '2025-06-26'

# Tablas transaccionales de la Capa Speed (vivas) que se vacían al inicio de cada jornada
TABLAS_VIVAS = [
    'base_llamadas_migraciones',
    'base_ventas_migraciones',
    'base_rete_out',
    'base_nps_rrss',
    'base_tipificaciones_rrss',
    'chats_monetizacion_rrss'
]


# =============================================================================
# 📦 FUNCIONES DE CIERRE ESCALONADO POR CAMPAÑA
# =============================================================================

def cierre_retenciones(engine, tz):
    """
    Ejecuta el cierre de jornada para Retenciones Outbound a las 15:00 hrs.
    Copia los registros vivos a la tabla histórica y dispara el clustering K-Means.
    """
    print("\n📦 [15:00] CIERRE RETENCIONES: Copiando tabla viva al histórico persistente...")
    try:
        with engine.begin() as conn:
            query_rete = """
            INSERT INTO hist_base_rete_out (
                call_id, date, "time", call_type, campaign, agent, ani, dnis, 
                queue_wait_time, talk_time, after_call_work_time, hold_time, 
                disposition, wom_inbid_ekia, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            )
            SELECT 
                call_id, date, "time", call_type, campaign, agent, ani, dnis, 
                queue_wait_time, talk_time, after_call_work_time, hold_time, 
                disposition, wom_inbid_ekia, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            FROM base_rete_out;
            """
            conn.execute(text(query_rete))
            print("   ✅ Migración de datos al histórico completada.")
            
        print("🧠 Ejecutando modelo de Machine Learning (K-Means) para Retenciones...")
        analizar_retenciones(engine, 'cierre', tz)
        print("   ✅ Cierre de Retenciones completado con éxito.")
    except Exception as e: 
        print(f"   ❌ Error en cierre de Retenciones: {e}")


def cierre_rrss(engine, tz):
    """
    Ejecuta el cierre de jornada para Redes Sociales a las 16:00 hrs.
    Transfiere chats, encuestas NPS y tipificaciones a las tablas históricas y dispara K-Means.
    """
    print("\n📦 [16:00] CIERRE REDES SOCIALES: Copiando tablas vivas al histórico...")
    try:
        with engine.begin() as conn:
            # 1. Migración de Chats y Monetización
            query_chats = """
            INSERT INTO hist_chats_monetizacion_rrss (
                id_original, "cola_de_atención", "id_conversación", bot, canal, 
                nombre_del_cliente, identificacion_del_cliente, tipo_de_cliente, 
                fecha_inicio, "fecha_asignación", "fecha_atención", tiempo_en_espera, 
                tiempo_primera_respuesta, "tiempo_de_asignación", tiempo_conversacion, 
                "fecha_finalización", "duración", cerrado_por, agente, skill, 
                recibida_por, login_agente, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            )
            SELECT 
                id_original, "cola_de_atención", "id_conversación", bot, canal, 
                nombre_del_cliente, identificacion_del_cliente, tipo_de_cliente, 
                fecha_inicio, "fecha_asignación", "fecha_atención", tiempo_en_espera, 
                tiempo_primera_respuesta, "tiempo_de_asignación", tiempo_conversacion, 
                "fecha_finalización", "duración", cerrado_por, agente, skill, 
                recibida_por, login_agente, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            FROM chats_monetizacion_rrss;
            """
            conn.execute(text(query_chats))
            
            # 2. Migración de Encuestas de Satisfacción (NPS)
            query_nps = """
            INSERT INTO hist_base_nps_rrss (
                id_cliente, bot, fecha_de_registro, "id_de_conversación", "id_conversación_con_agente", 
                codigo_ekia, agente, experiencia, "resolución_bot", "recomendación", "resolución", 
                "satisfacción", datos_de_origen, canal, custom_type, "satisfacción_bot", 
                "recomendación_bot", "solución_bot", comentarios_bot, etl_origen_archivo, 
                etl_lote_ejecucion, etl_fecha_carga_col
            )
            SELECT 
                id_cliente, bot, fecha_de_registro, "id_de_conversación", "id_conversación_con_agente", 
                codigo_ekia, agente, experiencia, "resolución_bot", "recomendación", "resolución", 
                "satisfacción", datos_de_origen, canal, custom_type, "satisfacción_bot", 
                "recomendación_bot", "solución_bot", comentarios_bot, etl_origen_archivo, 
                etl_lote_ejecucion, etl_fecha_carga_col
            FROM base_nps_rrss;
            """
            conn.execute(text(query_nps))
            
            # 3. Migración de Tipificaciones Operativas
            query_tipificaciones = """
            INSERT INTO hist_base_tipificaciones_rrss (
                id_cliente, fecha_de_registro, "id_conversación", canal, id_solicitud_pendiente, 
                "ekia_código", id_agente, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            )
            SELECT 
                id_cliente, fecha_de_registro, "id_conversación", canal, id_solicitud_pendiente, 
                "ekia_código", id_agente, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            FROM base_tipificaciones_rrss;
            """
            conn.execute(text(query_tipificaciones))
            print("   ✅ Migración de datos al histórico completada.")
            
        print("🧠 Ejecutando modelo de Machine Learning (K-Means con Feature Weighting 3.0x) para Redes Sociales...")
        analizar_rrss(engine, 'cierre', tz)
        print("   ✅ Cierre de Redes Sociales completado.")
    except Exception as e: 
        print(f"   ❌ Error en cierre de RRSS: {e}")


def cierre_migraciones(engine, tz):
    """
    Ejecuta el cierre de jornada para Migraciones Inbound a las 17:00 hrs.
    Transfiere llamadas y ventas a las tablas históricas y dispara el clustering comercial.
    """
    print("\n📦 [17:00] CIERRE MIGRACIONES: Copiando tablas vivas al histórico...")
    try:
        with engine.begin() as conn:
            # 1. Migración de Llamadas de Voz Inbound
            query_llamadas = """
            INSERT INTO hist_base_llamadas_migraciones (
                call_id, date, "time", call_type, campaign, agent, ani, dnis, 
                queue_wait_time, talk_time, after_call_work_time, hold_time, 
                disposition, wom_inbid_ekia, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            )
            SELECT 
                call_id, date, "time", call_type, campaign, agent, ani, dnis, 
                queue_wait_time, talk_time, after_call_work_time, hold_time, 
                disposition, wom_inbid_ekia, etl_origen_archivo, etl_lote_ejecucion, etl_fecha_carga_col
            FROM base_llamadas_migraciones;
            """
            conn.execute(text(query_llamadas))
            
            # 2. Migración de Ventas Validadas
            query_ventas = """
            INSERT INTO hist_base_ventas_migraciones (
                marca_temporal, nombre_del_asesor, cedula_del_agente, msisdn, 
                cedula_del_cliente, numero_linea_gestionada, gestion, contactado, 
                plan_vendido, canal_de_venta, supervisor, base_de_gestion, 
                fecha_de_venta, mes, dia, etl_origen_archivo, etl_lote_ejecucion, 
                etl_fecha_carga_col, "time"
            )
            SELECT 
                marca_temporal, nombre_del_asesor, cedula_del_agente, msisdn, 
                cedula_del_cliente, numero_linea_gestionada, gestion, contactado, 
                plan_vendido, canal_de_venta, supervisor, base_de_gestion, 
                fecha_de_venta, mes, dia, etl_origen_archivo, etl_lote_ejecucion, 
                etl_fecha_carga_col, "time"
            FROM base_ventas_migraciones;
            """
            conn.execute(text(query_ventas))
            print("   ✅ Migración de datos al histórico completada.")
            
        print("🧠 Ejecutando modelo de Machine Learning (K-Means comercial con SSOT) para Migraciones...")
        analizar_migraciones(engine, 'cierre', tz)
        print("   ✅ Cierre de Migraciones completado.")
    except Exception as e: 
        print(f"   ❌ Error en cierre de Migraciones: {e}")


# =============================================================================
# 🧹 FUNCIÓN DE REINICIO NOCTURNO Y PURGA DE CAPA SPEED
# =============================================================================

def reiniciar_entorno(engine):
    """
    Ejecuta el mantenimiento de medianoche:
    - Trunca las tablas vivas (Capa Speed) para recibir el nuevo día sin registros residuales.
    - En modo evaluación/tesis, limpia selectivamente la fecha de prueba para permitir repetición del ciclo.
    - Limpia los directorios locales de trabajo temporal.
    """
    print("\n🧹 [REINICIO NOCTURNO] Aplicando limpieza de la Capa Speed (Vivas)...")
    try:
        with engine.begin() as conn:
            # 1. Truncar (vaciar a cero) todas las tablas de la Capa Speed para el nuevo día
            for tabla in TABLAS_VIVAS:
                conn.execute(text(f"TRUNCATE TABLE {tabla};"))
            print("   ✅ Capa Speed (Vivas) vaciada correctamente para la nueva jornada.")
            
            # En modo evaluación/tesis, permitir repetir la demostración limpiando la fecha calibrada
            if not MODO_PRODUCCION:
                conn.execute(text(f"DELETE FROM hist_base_llamadas_migraciones WHERE date = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM hist_base_llamadas_migraciones WHERE date = '2025-05-26';"))
                conn.execute(text(f"DELETE FROM hist_base_ventas_migraciones WHERE fecha_de_venta = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM hist_base_rete_out WHERE date = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM hist_chats_monetizacion_rrss WHERE CAST(fecha_inicio AS TEXT) LIKE '{FECHA_PRUEBA}%';"))
                conn.execute(text(f"DELETE FROM hist_base_nps_rrss WHERE CAST(fecha_de_registro AS TEXT) LIKE '{FECHA_PRUEBA}%';"))
                conn.execute(text(f"DELETE FROM hist_base_tipificaciones_rrss WHERE CAST(fecha_de_registro AS TEXT) LIKE '{FECHA_PRUEBA}%';"))
                conn.execute(text(f"DELETE FROM ml_insights_migraciones WHERE fecha_analisis = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM ml_insights_retenciones WHERE fecha_analisis = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM ml_insights_rrss WHERE fecha_analisis = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM ml_alertas_globales WHERE call_date = '{FECHA_PRUEBA}';"))
                conn.execute(text(f"DELETE FROM ml_alertas_macro WHERE call_date = '{FECHA_PRUEBA}';"))
                print(f"   ✅ Limpieza selectiva de calibración ({FECHA_PRUEBA}) completada para nueva prueba.")

    except Exception as e:
        print(f"   ❌ Error en el reinicio de la base de datos: {e}")

    # Limpieza de carpetas temporales locales
    print("\n🗑️ Limpiando carpetas locales de trabajo...")
    carpetas = ['descargas_bot', 'historico_procesados']
    for carpeta in carpetas:
        if os.path.exists(carpeta):
            for archivo in os.listdir(carpeta):
                ruta_archivo = os.path.join(carpeta, archivo)
                try:
                    if os.path.isfile(ruta_archivo): os.unlink(ruta_archivo)
                except Exception: pass
    print("   ✅ Directorios locales limpios. El sistema está listo para un nuevo día.")


# =============================================================================
# 🚀 MOTOR PRINCIPAL DE EJECUCIÓN CONTINUA
# =============================================================================

def iniciar_orquestador():
    """
    Punto de entrada principal del orquestador.
    Mantiene el ciclo de ejecución continua hora a hora durante el turno operativo.
    """
    print("===================================================")
    print(" 🚀 INICIANDO ORQUESTADOR CENTRAL (Arquitectura Lambda)")
    print(f"    Modo: {'PRODUCCIÓN CONTINUA' if MODO_PRODUCCION else 'EVALUACIÓN EXPERIMENTAL (DEMO)'}")
    print("===================================================")
    
    engine = create_engine(DATABASE_URL)
    tz_colombia = pytz.timezone('America/Bogota')
    
    while True: 
        # Ciclo intradía: desde las 07:00 (Lote 6) hasta las 17:00 (Lote 16)
        for lote in range(LOTE_INICIAL, LOTE_FINAL + 1):
            hora_fisica_ejecucion = lote + 1 
            lote_str = str(lote).zfill(2)
            hora_fisica_str = str(hora_fisica_ejecucion).zfill(2)
            
            print(f"\n" + "="*60)
            print(f" 🕒 HORA DEL ROBOT: {hora_fisica_str}:00 | Procesando corte de {lote_str}:59")
            print("="*60)
            
            # 1. En modo evaluación, avanzar el reloj de la plataforma simuladora
            if not MODO_PRODUCCION:
                try:
                    requests.get(f"{API_URL}{lote_str}", timeout=5)
                except Exception as e:
                    print(f"❌ Error conectando a Cari AI. Asegúrate de tener app_dashboard.py corriendo: {e}")
                    return 

            # 2. RPA: Extracción autónoma de los 6 reportes transaccionales
            with sync_playwright() as playwright:
                bot_scraper.run(playwright, es_fin_de_dia=(lote == LOTE_FINAL))
            
            # 3. ETL: Limpieza, imputación y carga en la Capa Speed
            print(f"\n⚙️ Despertando Motor ETL para la hora {hora_fisica_str}:00...")
            motor_etl.ejecutar_etl()
            
            # 4. INTELIGENCIA ARTIFICIAL: Detección de anomalías y predicción horaria
            print(f"\n🔍 Ejecutando Motores de IA Horaria (Micro-anomalías y Run-Rate)...")
            analizar_migraciones(engine, 'horario', tz_colombia)
            analizar_retenciones(engine, 'horario', tz_colombia)
            analizar_rrss(engine, 'horario', tz_colombia)
            
            # 5. GOBERNANZA DE CIERRES ESCALONADOS
            if hora_fisica_ejecucion == 15:
                cierre_retenciones(engine, tz_colombia)
            
            elif hora_fisica_ejecucion == 16:
                cierre_rrss(engine, tz_colombia)
                
            elif hora_fisica_ejecucion == 17:
                cierre_migraciones(engine, tz_colombia)
                print(f"\n🌙 TODAS LAS OPERACIONES FINALIZADAS (17:00 HRS).")
                
                # Gestión de reposo nocturno
                if MODO_PRODUCCION:
                    ahora = datetime.now(tz_colombia)
                    medianoche = ahora.replace(hour=23, minute=59, second=59, microsecond=0)
                    segundos_hasta_medianoche = max(0, (medianoche - ahora).total_seconds())
                    print(f"⏳ [PRODUCCIÓN] Reposo nocturno activo. Despertando a las 23:59:59 ({int(segundos_hasta_medianoche/3600)} h restantes)...")
                    time.sleep(segundos_hasta_medianoche)
                else:
                    print(f"⏳ [EVALUACIÓN] Pausa de demostración de {TIEMPO_ESPERA_REINICIO/60} minutos...")
                    time.sleep(TIEMPO_ESPERA_REINICIO)
                
                # Limpieza de fin de ciclo y preparación de la siguiente jornada
                reiniciar_entorno(engine)
                print("\n🌅 ¡Fin de ciclo! Iniciando un nuevo día operativo...")
                break
                
            # 6. Pausa normal entre cortes horarios
            if hora_fisica_ejecucion < 17:
                print(f"\n⏳ Ciclo de las {hora_fisica_str}:00 completado. Esperando {TIEMPO_ESPERA_ENTRE_HORAS}s...")
                time.sleep(TIEMPO_ESPERA_ENTRE_HORAS)


if __name__ == "__main__":
    iniciar_orquestador()
