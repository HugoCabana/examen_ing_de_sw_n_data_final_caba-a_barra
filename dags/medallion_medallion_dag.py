"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

# TODO: Definir las funciones necesarias para cada etapa del pipeline
#  (bronze, silver, gold) usando las funciones de transformación y
#  los comandos de dbt.

def _abort_if_missing_raw(execution_date: str, **_kwargs) -> None:
    """
    Si no existe el archivo raw del día, se saltea la corrida del pipeline.
    execution_date llega como "YYYY-MM-DD" (Airflow ds).
    """
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    ds_nodash = exec_date.strftime("%Y%m%d")

    raw_path = RAW_DIR / f"transactions_{ds_nodash}.csv"
    if not raw_path.exists():
        raise AirflowSkipException(f"No existe archivo raw para {ds_nodash}: {raw_path}")

def _bronze_clean(execution_date: str, **_kwargs) -> None:
    """
    Toma la fecha de ejecución como string (YYYY-MM-DD),
    la convierte a date y llama a clean_daily_transactions.
    """
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d").date()

    clean_daily_transactions(
        execution_date=exec_date,
        raw_dir=RAW_DIR,
        clean_dir=CLEAN_DIR,
    )

def silver_task(ds_nodash: str):
    """
    Ejecuta dbt run filtrando modelos silver.
    """
    result = _run_dbt_command("run", ds_nodash)

    if result.returncode != 0:
        raise AirflowException(f"dbt run (silver) falló: {result.stderr}")

    return result.stdout

def _gold_dbt_tests(ds_nodash: str, **_kwargs) -> None:
    """
    Ejecuta `dbt test`, escribe el archivo de data quality
    y falla el task si hay algún test fallido.
    """
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    result = _run_dbt_command("test", ds_nodash)

    status = "passed" if result.returncode == 0 else "failed"
    output_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"

    payload = {
        "ds_nodash": ds_nodash,
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # Si hubo tests fallidos, marcamos el task como failed,
    # pero el JSON ya quedó generado para monitoreo.
    if result.returncode != 0:
        raise AirflowException(
            f"dbt test failed for {ds_nodash}."
            f"See data quality results at: {output_path}"
        )

def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        # TODO:
        # * Agregar las tasks necesarias del pipeline para completar lo pedido por el enunciado.
        # * Usar PythonOperator con el argumento op_kwargs para pasar ds_nodash a las funciones.
        #   De modo que cada task pueda trabajar con la fecha de ejecución correspondiente.
        # Recomendaciones:
        #  * Pasar el argumento ds_nodash a las funciones definidas arriba.
        #    ds_nodash contiene la fecha de ejecución en formato YYYYMMDD sin guiones.
        #    Utilizarlo para que cada task procese los datos del dia correcto y los archivos
        #    de salida tengan nombres únicos por fecha.
        #  * Asegurarse de que los paths usados en las funciones sean relativos a BASE_DIR.
        #  * Usar las funciones definidas arriba para cada etapa del pipeline.
        
        # A continuación se definen las tasks del pipeline:
        # 1. Se verifica la existencia del archivo raw del día y se saltea la corrida si no está disponible.
        # 2. Se ejecuta la limpieza (Bronze).
        # 3. Se materializan los modelos con dbt (Silver).
        # 4. Se ejecutan los tests de dbt y se registran los resultados de calidad (Gold).

        check_raw_exists = PythonOperator(
            task_id="check_raw_exists",
            python_callable=_abort_if_missing_raw,
            op_kwargs={"execution_date": "{{ ds }}"}
        )

        bronze_clean = PythonOperator(
            task_id="bronze_clean",
            python_callable=_bronze_clean,
            op_kwargs={
                # Airflow inyecta la fecha de ejecución como "YYYY-MM-DD"
                "execution_date": "{{ ds }}",
            },
        )

        silver = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=silver_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=_gold_dbt_tests,
            op_kwargs={
                "ds_nodash": "{{ ds_nodash }}",
            },
        )

        check_raw_exists >> bronze_clean >> silver >> gold_dbt_tests

    return medallion_dag


dag = build_dag()
