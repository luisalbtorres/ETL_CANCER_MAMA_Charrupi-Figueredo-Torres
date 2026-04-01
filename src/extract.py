import pandas as pd
from pathlib import Path
from metricas import registrar_filas


def leer_csv_flexible(ruta):
    """
    Intenta leer CSV con distintos separadores y codificaciones.
    """
    separadores = [",", ";"]
    codificaciones = ["utf-8", "latin-1", "cp1252"]

    ultimo_error = None

    for sep in separadores:
        for enc in codificaciones:
            try:
                df = pd.read_csv(ruta, sep=sep, encoding=enc)
                if df.shape[1] > 1:
                    return df
            except Exception as e:
                ultimo_error = e

    raise ValueError(f"No se pudo leer el archivo {ruta}. Error: {ultimo_error}")


def leer_archivo(ruta):
    ruta = Path(ruta)

    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")

    if ruta.suffix.lower() == ".csv":
        return leer_csv_flexible(ruta)

    if ruta.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(ruta)

    raise ValueError(f"Formato no soportado: {ruta.suffix}")


def extraer_datos():
    print("Aquí se extraen los datos")

    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = base_dir / "data" / "raw"

    # Base hospitalaria: intenta primero Hospital.csv, si no existe usa BD2025.xlsx
    hospital_csv = raw_dir / "Hospital.csv"
    hospital_xlsx = raw_dir / "BD2025.xlsx"

    if hospital_csv.exists():
        hospital_df = leer_archivo(hospital_csv)
        hospital_df["fuente_archivo"] = "Hospital.csv"
    elif hospital_xlsx.exists():
        hospital_df = leer_archivo(hospital_xlsx)
        hospital_df["fuente_archivo"] = "BD2025.xlsx"
    else:
        raise FileNotFoundError("No se encontró Hospital.csv ni BD2025.xlsx en data/raw/")

    # Bases regionales
    valle_2018 = leer_archivo(raw_dir / "cancer_mama_valle_cauca_2018.csv")
    valle_2021 = leer_archivo(raw_dir / "Cancer-mama-2021.csv")
    valle_2022 = leer_archivo(raw_dir / "Cancer-mama-2022.csv")
    valle_2023 = leer_archivo(raw_dir / "Cancer-mama-2023.csv")

    registrar_filas("Extract", "hospital", len(hospital_df))
    registrar_filas("Extract", "valle_2018", len(valle_2018))
    registrar_filas("Extract", "valle_2021", len(valle_2021))
    registrar_filas("Extract", "valle_2022", len(valle_2022))
    registrar_filas("Extract", "valle_2023", len(valle_2023))

    return {
        "hospital": hospital_df,
        "valle_2018": valle_2018,
        "valle_2021": valle_2021,
        "valle_2022": valle_2022,
        "valle_2023": valle_2023,
    }


def conectar_api():
    print("Aquí se conecta a la API")
    # Se deja para después
    return None

if __name__ == "__main__":
    datos = extraer_datos()

    print("Extracción completada.")
    for nombre, df in datos.items():
        print(f"{nombre}: {df.shape[0]} filas, {df.shape[1]} columnas")
        print(df.head(3))
        print("-" * 50)