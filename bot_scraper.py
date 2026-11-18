"""
========================================================================================
MÓDULO DE AUTOMATIZACIÓN ROBÓTICA DE PROCESOS (RPA) - PLAYWRIGHT
Proyecto de Grado: Sistema Automatizado de Monitoreo GTR y Analítica Predictiva
Operación: Apex America BPO (Campaña Inbound Migraciones, Outbound Retenciones, RRSS)
Autor: Juan Guillermo Velasco Ahumada
Director: Ing. Fabio Hernán Realpe Martínez | Asesor: Andrés Oicata Casas
========================================================================================

DESCRIPCIÓN TÉCNICA:
Este módulo implementa el agente autónomo de extracción desatendida (RPA) utilizando
la librería Playwright sobre un motor de navegador Chromium en modo headless.
Sustituye la descarga manual humana en navegador (que consumía 10 minutos por corte)
por un ciclo automatizado y tolerante a fallos que dura menos de 45 segundos:

1. Autenticación segura mediante credenciales operativas en la plataforma de Contact Center (Cari AI / Inconcert).
2. Navegación a través del menú de Reportes Operativos.
3. Generación secuencial de los 6 reportes transaccionales de la hora operativa:
   - Base Llamadas Migraciones (Inbound)
   - Base Ventas Migraciones (Inbound)
   - Base Outbound Retenciones (Llamadas salientes)
   - Base NPS Redes Sociales (Satisfacción del cliente)
   - Base Tipificaciones Redes Sociales (Motivos de contacto)
   - Chats y Monetización Redes Sociales (Mensajería digital)
4. Filtrado del corte horario en la bandeja de 'Reportes Exportados' y descarga controlada
   asíncrona hacia la zona de aterrizaje local ('descargas_bot/').
5. Mantenimiento y depuración de la cola de exportados al cierre de jornada (Fin de Día).
6. Cierre seguro de sesión y destrucción de contextos de memoria en el navegador.

CONFIGURACIÓN DE ENTORNO:
- CARI_URL: URL del portal operativo (default: http://127.0.0.1:5000/)
- CARI_USER: Identificador de acceso del bot (default: jvelasco@apex.com)
- CARI_PASSWORD: Clave de autenticación del bot (default: 12345678*)
- RPA_HEADLESS: Ejecución visible o desatendida (default: True)
========================================================================================
"""

import os
import time
from playwright.sync_api import Playwright, sync_playwright, expect

# Parámetros de conexión configurables para entorno de producción o pruebas locales
CARI_URL = os.getenv("CARI_URL", "http://127.0.0.1:5000/")
CARI_USER = os.getenv("CARI_USER", "jvelasco@apex.com")
CARI_PASSWORD = os.getenv("CARI_PASSWORD", "12345678*")
HEADLESS_MODE = os.getenv("RPA_HEADLESS", "true").lower() == "true"


def run(playwright: Playwright, es_fin_de_dia: bool = False) -> None:
    """
    Ejecuta el ciclo completo de extracción RPA en la plataforma Cari AI.

    Parámetros:
    -----------
    playwright : Playwright
        Instancia activa del contexto de automatización Playwright.
    es_fin_de_dia : bool, opcional
        Bandera booleana que indica si el corte corresponde al cierre de jornada (17:00 hrs).
        Si es True, ejecuta la purga de la bandeja histórica de exportación en el servidor.
    """
    print("===================================================")
    print("🤖 INICIANDO ROBOT EXTRACTOR (RPA CARI AI / INCONCERT)")
    print("===================================================")
    
    # -------------------------------------------------------------------------
    # 1. PREPARACIÓN DE LA ZONA DE ATERRIZAJE (STAGING AREA)
    # -------------------------------------------------------------------------
    carpeta_descargas = "descargas_bot"
    os.makedirs(carpeta_descargas, exist_ok=True)
    print(f"📁 Zona de aterrizaje local verificada en: {carpeta_descargas}/")

    # Inicialización del navegador Chromium en modo desatendido (producción)
    browser = playwright.chromium.launch(headless=HEADLESS_MODE, slow_mo=300)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    # Manejador automático de diálogos emergentes (acepta confirmaciones de borrado)
    page.on("dialog", lambda dialog: dialog.accept())

    try:
        # ---------------------------------------------------------------------
        # 2. AUTENTICACIÓN Y NAVEGACIÓN EN LA PLATAFORMA
        # ---------------------------------------------------------------------
        print(f"\n🌐 Navegando a la plataforma operativa ({CARI_URL}) e iniciando sesión...")
        page.goto(CARI_URL)
        time.sleep(1) 
        
        # Inserción de credenciales operativas del bot
        page.locator("input[name='usuario']").fill(CARI_USER)
        try: 
            page.locator("input[name='password']").fill(CARI_PASSWORD)
        except Exception: 
            pass 
            
        page.get_by_role("button", name="Ingresar").click() 
        time.sleep(1)

        def asegurar_menu_abierto():
            """Garantiza la visibilidad de la sección de reportes operativos."""
            if not page.locator("a", has_text="Base Transaccional (Chat)").is_visible():
                page.locator("a").filter(has_text="Reportes Operativos").wait_for(state="visible", timeout=15000)
                page.locator("a").filter(has_text="Reportes Operativos").click()
                time.sleep(0.5)

        # ---------------------------------------------------------------------
        # 3. CATÁLOGO DE REPORTES HORARIOS TRANSACCIONALES
        # ---------------------------------------------------------------------
        reportes_a_extraer = [
            "Base Llamadas Migraciones",
            "Base Ventas Migraciones",  
            "Base Outbound (Llamada)",
            "Base NPS RRSS",
            "Base Tipificaciones RRSS",
            "Chats y Monetización RRSS"
        ]

        # ---------------------------------------------------------------------
        # 4. SOLICITUD SECUENCIAL DE GENERACIÓN DE REPORTES (CORTE HORARIO)
        # ---------------------------------------------------------------------
        print(f"\n⏳ Solicitando lote de {len(reportes_a_extraer)} reportes operativos a Cari AI...")
        for reporte in reportes_a_extraer:
            asegurar_menu_abierto()
            
            # Clic en el enlace del reporte
            page.locator("a").filter(has_text=reporte).wait_for(state="visible", timeout=15000)
            page.locator("a").filter(has_text=reporte).click()
            time.sleep(0.5)
            
            # Selección del corte de 'Última Hora'
            page.locator("div.view-section:visible select[name='rango_fecha']").select_option("hora")
            
            # Disparo del generador en backend
            page.locator("div.view-section:visible button:has-text('Generar Reporte')").click()
            print(f"   ⚙️ Generación solicitada exitosamente: {reporte}")
            time.sleep(1)

        # ---------------------------------------------------------------------
        # 5. BANDEJA DE DESCARGAS: FILTRADO Y EXTRACCIÓN A DISCO
        # ---------------------------------------------------------------------
        print("\n📥 Accediendo a la bandeja de 'Reportes Exportados'...")
        asegurar_menu_abierto()
        page.locator("a").filter(has_text="Reportes Exportados").wait_for(state="visible", timeout=15000)
        page.locator("a").filter(has_text="Reportes Exportados").click()
        time.sleep(1)

        # Filtrar exclusivamente los archivos generados en la última hora operativa
        print("🔍 Aplicando filtro por franja 'Última Hora'...")
        page.locator("#filtro-historial").select_option("hora")
        page.locator("button:has-text('Buscar')").click()
        time.sleep(1)

        # Identificación de descargas disponibles
        filas_visibles = page.locator("tr.historial-row:visible")
        cantidad_descargas = filas_visibles.count()
        print(f"   📊 Se identificaron {cantidad_descargas} reportes listos para transferencia.")

        # Descarga controlada de cada archivo CSV hacia descargas_bot/
        for i in range(cantidad_descargas):
            with page.expect_download() as download_info:
                filas_visibles.nth(i).locator("a:has-text('Descargar CSV')").click()
            
            descarga = download_info.value
            ruta_final = os.path.join(carpeta_descargas, descarga.suggested_filename)
            descarga.save_as(ruta_final)
            print(f"   ⬇️ Descargado en staging: {descarga.suggested_filename}")

        # ---------------------------------------------------------------------
        # 6. GOBERNANZA DE FIN DE JORNADA (LIMPIEZA DE SERVIDOR)
        # ---------------------------------------------------------------------
        if es_fin_de_dia:
            print("\n🧹 Límite horario alcanzado (Fin de Día). Limpiando historial del servidor...")
            page.locator("#filtro-historial").select_option("dia")
            page.locator("button:has-text('Buscar')").click()
            time.sleep(0.5)
            page.locator("button:has-text('Borrar Historial')").click()
            time.sleep(1)
            print("   ✨ Historial del servidor Cari AI depurado exitosamente.")

        # ---------------------------------------------------------------------
        # 7. CIERRE SEGURO DE SESIÓN
        # ---------------------------------------------------------------------
        print("\n🔒 Cerrando sesión de forma segura...")
        page.locator(".dropdown-toggle").click()
        time.sleep(0.5)
        page.locator("a:has-text('Cerrar Sesión')").click()
        
        print("\n🎉 ¡Proceso RPA completado con éxito al 100%!")

    except Exception as e:
        print(f"\n❌ Ocurrió un error crítico en el RPA: {e}")
        raise e

    finally:
        # Destrucción segura del contexto y proceso del navegador
        context.close()
        browser.close()


if __name__ == "__main__":
    # Ejecución manual directa (por defecto en modo horario)
    with sync_playwright() as playwright:
        run(playwright, es_fin_de_dia=False)
