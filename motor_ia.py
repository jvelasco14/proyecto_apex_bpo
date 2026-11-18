"""
========================================================================================
ORQUESTADOR DE SERVICIOS ANALÍTICOS Y MACHINE LEARNING
Proyecto de Grado: Sistema Automatizado de Monitoreo GTR y Analítica Predictiva
Operación: Apex America BPO (Migraciones Inbound, Retenciones Outbound, Redes Sociales)
Autor: Juan Guillermo Velasco Ahumada
Director: Ing. Fabio Hernán Realpe Martínez | Asesor: Andrés Oicata Casas
========================================================================================

DESCRIPCIÓN TÉCNICA:
Punto de invocación y despacho para los motores analíticos de Machine Learning.
Permite ejecutar de forma unificada o independiente los modelos no supervisados
(Isolation Forest y K-Means) y algoritmos deterministas de proyección (Run-Rate):

- Modo 'horario':
  Ejecuta la detección de micro-anomalías individuales (TMO atípico, ventas exprés <30s,
  chats zombies >30m) y proyecciones intradía en las 3 campañas.
- Modo 'cierre':
  Ejecuta el clustering K-Means (K=3) para la categorización y perfilamiento
  táctico de asesores al final de su jornada laboral.
========================================================================================
"""

import os
import sys
from sqlalchemy import create_engine
import pytz
import warnings

from ia_migraciones import analizar_migraciones
from ia_rrss import analizar_rrss
from ia_retenciones import analizar_retenciones   

warnings.filterwarnings('ignore')

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+psycopg2://postgres.vdvmyueufdfviztmribf:Velasco14062000**@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
)


def ejecutar_motor_ia_completo(modo: str = 'horario'):
    """
    Ejecuta secuencialmente los modelos analíticos para las tres campañas operativas.

    Parámetros:
    -----------
    modo : str, opcional
        'horario' para inferencia intradía o 'cierre' para clustering EOD.
    """
    print("===================================================")
    print(f"🧠 INICIANDO ORQUESTADOR DE IA - MODO: {modo.upper()}")
    print("===================================================")
    
    tz_colombia = pytz.timezone('America/Bogota')
    
    try:
        engine = create_engine(DATABASE_URL)
        print("🔗 Conectado exitosamente a PostgreSQL en Supabase Cloud para inferencia.")
    except Exception as e:
        print(f"❌ Error crítico de conexión a la base de datos: {e}")
        return 

    # Módulo 1: Campaña Inbound Migraciones (Voz y Ventas)
    print("\n[1/3] Ejecutando analítica de Migraciones Inbound...")
    analizar_migraciones(engine, modo, tz_colombia)

    # Módulo 2: Campaña Redes Sociales (Mensajería Digital y NPS)
    print("\n[2/3] Ejecutando analítica de Canales Digitales (RRSS)...")
    analizar_rrss(engine, modo, tz_colombia)

    # Módulo 3: Campaña Retenciones Outbound (Llamadas Salientes)
    print("\n[3/3] Ejecutando analítica de Retenciones Outbound...")
    analizar_retenciones(engine, modo, tz_colombia)

    print("\n✅ Proceso Analítico de Inteligencia Artificial Finalizado al 100%.")
    print("===================================================")


if __name__ == "__main__":
    modo_ejecucion = sys.argv[1] if len(sys.argv) > 1 else 'horario'
    ejecutar_motor_ia_completo(modo_ejecucion)
