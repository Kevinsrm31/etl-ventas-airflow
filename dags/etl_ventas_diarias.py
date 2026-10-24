"""ETL diario de ventas: CSV de las tiendas -> validación -> PostgreSQL.

Se dispara a mano con el parámetro `fecha` (AAAA-MM-DD). Cada corrida debe
reemplazar solo los datos de esa fecha, así que repetirla no puede duplicar filas.

Flujo: extraer -> transformar -> validar_calidad -> cargar -> actualizar_resumen.
Si se rechaza más del 20 % del archivo, validar_calidad falla y no se toca la base.
"""

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task

from ventas.reglas import leer_csv, revisar_calidad, separar_ventas

CARPETA_ENTRADA = Path(os.environ.get("VENTAS_CARPETA_ENTRADA", "/opt/airflow/data/entrada"))
CONEXION = "postgres_datapath"

log = logging.getLogger(__name__)

CREAR_TABLAS = """
CREATE TABLE IF NOT EXISTS ventas_limpias (
    id_venta        INTEGER PRIMARY KEY,
    fecha           DATE NOT NULL,
    region          TEXT NOT NULL,
    producto        TEXT NOT NULL,
    cantidad        INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario NUMERIC(10, 2) NOT NULL CHECK (precio_unitario > 0),
    total           NUMERIC(12, 2) NOT NULL,
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ventas_rechazadas (
    fecha_archivo DATE NOT NULL,
    linea         INTEGER NOT NULL,
    id_venta      TEXT,
    motivo        TEXT NOT NULL,
    fila_original TEXT NOT NULL,
    PRIMARY KEY (fecha_archivo, linea)
);
CREATE TABLE IF NOT EXISTS resumen_ventas_diario (
    fecha       DATE NOT NULL,
    region      TEXT NOT NULL,
    num_ventas  INTEGER NOT NULL,
    unidades    INTEGER NOT NULL,
    monto_total NUMERIC(14, 2) NOT NULL,
    PRIMARY KEY (fecha, region)
);
"""

INSERTAR_LIMPIA = """
INSERT INTO ventas_limpias (id_venta, fecha, region, producto, cantidad, precio_unitario, total)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

INSERTAR_RECHAZADA = """
INSERT INTO ventas_rechazadas (fecha_archivo, linea, id_venta, motivo, fila_original)
VALUES (%s, %s, %s, %s, %s)
"""

RECALCULAR_RESUMEN = """
INSERT INTO resumen_ventas_diario (fecha, region, num_ventas, unidades, monto_total)
SELECT fecha, region, COUNT(*), SUM(cantidad), SUM(total)
FROM ventas_limpias
WHERE fecha = %s
GROUP BY fecha, region
"""


@dag(
    dag_id="etl_ventas_diarias",
    description="Carga diaria de ventas desde CSV a PostgreSQL",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "fecha": Param("2026-10-05", type="string", format="date",
                       description="Fecha del archivo a procesar (AAAA-MM-DD)"),
    },
    default_args={"owner": "equipo-datos", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["laboratorio", "ventas", "etl"],
)
def etl_ventas_diarias():

    @task
    def extraer():
        # date.fromisoformat falla si la fecha no viene como AAAA-MM-DD
        fecha = date.fromisoformat(get_current_context()["params"]["fecha"]).isoformat()
        ruta = CARPETA_ENTRADA / f"ventas_{fecha}.csv"
        if not ruta.is_file():
            raise FileNotFoundError(f"No existe el archivo de ventas del {fecha}: {ruta}")

        filas = leer_csv(ruta)
        log.info("Leídas %d filas de %s", len(filas), ruta.name)
        return {"fecha": fecha, "filas": filas}

    @task
    def transformar(datos):
        limpias, rechazadas = separar_ventas(datos["filas"], datos["fecha"])
        log.info("%d filas válidas y %d rechazadas", len(limpias), len(rechazadas))
        for rechazada in rechazadas:
            log.info("Línea %d rechazada: %s", rechazada["linea"], rechazada["motivo"])
        return {
            "fecha": datos["fecha"],
            "leidas": len(datos["filas"]),
            "limpias": limpias,
            "rechazadas": rechazadas,
        }

    # Sin reintentos: si el archivo viene sucio, volver a revisarlo da el mismo resultado
    @task(retries=0)
    def validar_calidad(resultado):
        tasa = revisar_calidad(resultado["leidas"], len(resultado["rechazadas"]))
        log.info("Tasa de rechazo: %.1f%%", tasa * 100)
        return resultado

    @task
    def cargar(resultado):
        fecha = resultado["fecha"]
        limpias = resultado["limpias"]
        rechazadas = resultado["rechazadas"]

        conn = PostgresHook(postgres_conn_id=CONEXION).get_conn()
        try:
            # `with conn` hace commit al salir sin errores y rollback si algo falla:
            # o se reemplaza la fecha completa o queda como estaba
            with conn, conn.cursor() as cur:
                cur.execute(CREAR_TABLAS)
                cur.execute("DELETE FROM ventas_limpias WHERE fecha = %s", (fecha,))
                cur.execute("DELETE FROM ventas_rechazadas WHERE fecha_archivo = %s", (fecha,))

                cur.executemany(INSERTAR_LIMPIA, [
                    (v["id_venta"], v["fecha"], v["region"], v["producto"],
                     v["cantidad"], v["precio_unitario"], v["total"])
                    for v in limpias
                ])
                cur.executemany(INSERTAR_RECHAZADA, [
                    (fecha, r["linea"], r["id_venta"], r["motivo"], r["fila_original"])
                    for r in rechazadas
                ])

                cur.execute("SELECT COUNT(*) FROM ventas_limpias WHERE fecha = %s", (fecha,))
                cargadas = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM ventas_rechazadas WHERE fecha_archivo = %s", (fecha,))
                guardadas = cur.fetchone()[0]
                if cargadas != len(limpias) or guardadas != len(rechazadas):
                    raise RuntimeError(
                        f"Conteo distinto tras la carga del {fecha}: "
                        f"limpias {cargadas}/{len(limpias)}, rechazadas {guardadas}/{len(rechazadas)}"
                    )
        finally:
            conn.close()

        log.info("Cargadas %d ventas y %d rechazos del %s", cargadas, guardadas, fecha)
        return {"fecha": fecha, "cargadas": cargadas, "rechazadas": guardadas}

    @task
    def actualizar_resumen(carga):
        fecha = carga["fecha"]
        conn = PostgresHook(postgres_conn_id=CONEXION).get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM resumen_ventas_diario WHERE fecha = %s", (fecha,))
                cur.execute(RECALCULAR_RESUMEN, (fecha,))
                regiones = cur.rowcount
        finally:
            conn.close()

        log.info("Resumen del %s actualizado: %d regiones", fecha, regiones)
        return {"fecha": fecha, "regiones": regiones}

    actualizar_resumen(cargar(validar_calidad(transformar(extraer()))))


etl_ventas_diarias()
