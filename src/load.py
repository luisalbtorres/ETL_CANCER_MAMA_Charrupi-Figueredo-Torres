import pandas as pd
import sqlite3
from pathlib import Path
from metricas import registrar_filas


def cargar_datos(datos_transformados):
    print("Aquí se cargan los datos")

    base_dir = Path(__file__).resolve().parents[1]
    processed_dir = base_dir / "data" / "processed"
    db_dir = base_dir / "data" / "db"

    processed_dir.mkdir(parents=True, exist_ok=True)
    db_dir.mkdir(parents=True, exist_ok=True)

    # 1. Guardar CSV
    for nombre, df in datos_transformados.items():
        if isinstance(df, pd.DataFrame):
            registrar_filas("Load", nombre, len(df))
            df.to_csv(processed_dir / f"{nombre}.csv", index=False, encoding="utf-8-sig")

    # 2. Guardar Excel consolidado
    ruta_excel = processed_dir / "reporte_etl_cancer_mama.xlsx"

    with pd.ExcelWriter(ruta_excel, engine="openpyxl") as writer:
        for nombre, df in datos_transformados.items():
            if isinstance(df, pd.DataFrame):
                df.to_excel(writer, sheet_name=nombre[:31], index=False)

        workbook = writer.book
        workbook.active = 0

    # 3. Guardar SQLite
    ruta_sqlite = db_dir / "etl_cancer_mama.sqlite"
    conn = sqlite3.connect(ruta_sqlite)

    try:
        for nombre, df in datos_transformados.items():
            if isinstance(df, pd.DataFrame):
                df.to_sql(nombre, conn, if_exists="replace", index=False)
                print(f"Tabla cargada en SQLite: {nombre}")
    finally:
        conn.close()

    print("Datos cargados correctamente")