# APEX GTR Analytics — Plataforma de Gestión en Tiempo Real con IA

Repositorio oficial del Trabajo de Grado:  
**"Desarrollo de un Sistema Automatizado de Monitoreo, Análisis de KPIs y Alertas Inteligentes en Tiempo Real para la Gestión Operativa de un BPO"**

- **Autor:** Juan Guillermo Velasco Ahumada  
- **Director Académico:** Ing. Fabio Hernán Realpe Martínez (Universidad del Cauca)  
- **Asesor Empresarial:** Andrés Oicata Casas (Apex America Colombia)  
- **Programa:** Ingeniería en Automatización Industrial — FIET, Unicauca (2026)  

---

## 📌 Resumen Ejecutivo

Ecosistema integral diseñado para sustituir los flujos manuales ofimáticos en la operación BPO por una **Arquitectura Lambda desatendida y escalable**. Conecta las plataformas operativas (Cari AI / Inconcert) con un clúster PostgreSQL en **Supabase Cloud**, procesa y normaliza datos en memoria, infiere micro-anomalías y predicciones con **Machine Learning**, y sirve un **Dashboard Web en FastAPI** con latencia sub-segundo:

- **Reducción de latencia de ciclo:** De 20 minutos a 80 segundos (**--93.3%**).
- **Ahorro operativo:** 68 horas-hombre/mes por analista de Control Operacional (GTR).
- **Volumetría verificada:** $98\,975$ registros transaccionales procesados con cero pérdida de datos.

---

## 🏛️ Arquitectura del Pipeline y Módulos Maestros

| Componente | Script Maestro | Tecnología | Función Operativa |
| :--- | :--- | :--- | :--- |
| **RPA Extractor** | `bot_scraper.py` | Playwright (Headless) | Extracción desatendida de 6 reportes horarios en contact center. |
| **Motor ETL** | `motor_etl.py` | Pandas & SQLAlchemy | Data cleansing universal, imputación de nulos y carga a Capa Speed. |
| **Orquestador E2E** | `bucle_maestro.py` | Python Daemon | Coordinación horaria, cierres escalonados (15h, 16h, 17h) y reinicio EOD. |
| **Modelos de IA** | `ia_migraciones.py`<br>`ia_retenciones.py`<br>`ia_rrss.py` | Scikit-Learn | *Isolation Forest* (micro-anomalías), *Run-Rate* (proyección) y *K-Means* ($K=3$). |
| **Servidor Web** | `app.py` | FastAPI & Jinja2 | Vistas SQL indexadas (`vw_*`), control RBAC y tableros interactivos. |

---

## 🔑 Credenciales de Acceso para Evaluación de Jurados

### 1. Dashboard Web en Producción (Cloud OCI)
- **URL:** [https://apexgtr-analyst.duckdns.org/login](https://apexgtr-analyst.duckdns.org/login)
- **Usuario:** `juradosunicauca@apexamerica.com`
- **Contraseña:** `Clave2026+*`
- **Rol:** `System Administrator` (Acceso total a monitoreo RT, cierres EOD, modelos IA y administración).
---

## ⚡ Ejecución Local Rápida

```bash
# 1. Clonar repositorio e instalar dependencias
git clone https://github.com/jvelasco14/proyecto_apex_bpo.git
cd proyecto_apex_bpo
pip install -r requirements.txt

# 2. Iniciar dashboard web local
uvicorn app:app --host 127.0.0.1 --port 8000 --reload
```
