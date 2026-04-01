import pandas as pd
import numpy as np
from metricas import registrar_filas


def normalizar_columnas(df):
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def estandarizar_texto(serie):
    return (
        serie.astype(str)
        .str.strip()
        .str.upper()
        .replace({"NAN": np.nan, "": np.nan})
    )


def convertir_fecha(serie):
    meses = {
        "ene": "jan", "feb": "feb", "mar": "mar", "abr": "apr",
        "may": "may", "jun": "jun", "jul": "jul", "ago": "aug",
        "sep": "sep", "oct": "oct", "nov": "nov", "dic": "dec"
    }

    serie = (
        serie.astype(str)
        .str.strip()
        .str.lower()
        .replace({
            "": np.nan,
            "nan": np.nan,
            "nat": np.nan
        })
    )

    for es, en in meses.items():
        serie = serie.str.replace(es, en, regex=False)

    fechas = pd.to_datetime(
        serie,
        errors="coerce",
        format="mixed",
        dayfirst=True
    )

    # Convertir la fecha centinela a nulo real
    fechas = fechas.mask(fechas == pd.Timestamp("1800-01-01"))

    return fechas

def formatear_fecha_iso(serie):
    return serie.dt.strftime("%Y-%m-%d").fillna("No aplica")


def formatear_fechas_iso_df(df):
    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("No aplica")

    return df

def crear_grupo_edad(df, columna_edad="edad"):
    df = df.copy()

    if columna_edad not in df.columns:
        df["grupo_edad"] = np.nan
        return df

    df[columna_edad] = pd.to_numeric(df[columna_edad], errors="coerce")

    bins = [0, 29, 39, 49, 59, 69, 79, 150]
    labels = ["<30", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"]

    df["grupo_edad"] = pd.cut(df[columna_edad], bins=bins, labels=labels)
    return df


def transformar_hospital(df_hospital):
    df = normalizar_columnas(df_hospital)

    cie10_mama = [
        "C500", "C501", "C502", "C503", "C504", "C505",
        "C506", "C508", "C509", "D057", "D059", "D486", "Z853"
    ]

    # estandarizar CIE10
    df["var017"] = estandarizar_texto(df["var017"])

    # filtrar cáncer de mama
    df_mama = df[df["var017"].isin(cie10_mama)].copy()

    # fechas
    df_mama["var018"] = convertir_fecha(df_mama["var018"])
    df_mama["var019"] = convertir_fecha(df_mama["var019"])

    # oportunidad diagnóstica en días
    df_mama["oportunidad_diagnostica_dias"] = (df_mama["var018"] - df_mama["var019"]).dt.days

    # solo casos válidos
    df_mama["caso_valido"] = (
        df_mama["oportunidad_diagnostica_dias"].notna() &
        (df_mama["oportunidad_diagnostica_dias"] >= 0)
    )

    df_validos = df_mama[df_mama["caso_valido"]].copy()

    # edad
    if "edad" in df_validos.columns:
        df_validos["edad"] = pd.to_numeric(df_validos["edad"], errors="coerce")
        df_validos = crear_grupo_edad(df_validos, "edad")

    # régimen
    if "var010" in df_validos.columns:
        df_validos["var010"] = estandarizar_texto(df_validos["var010"])

    # ordenar de menor a mayor
    df_validos = df_validos.sort_values("oportunidad_diagnostica_dias", ascending=True)

    # tabla calidad del dato
    tabla_calidad = pd.DataFrame({
        "indicador": [
            "Registros hospitalarios originales",
            "Registros de cáncer de mama",
            "Registros válidos para oportunidad diagnóstica",
            "Porcentaje válido sobre cáncer de mama"
        ],
        "valor": [
            len(df),
            len(df_mama),
            len(df_validos),
            round((len(df_validos) / len(df_mama)) * 100, 2) if len(df_mama) > 0 else 0
        ]
    })

    df_mama = formatear_fechas_iso_df(df_mama)
    df_validos = formatear_fechas_iso_df(df_validos)

    return {
        "hospital_mama": df_mama,
        "hospital_validos": df_validos,
        "calidad_hospital": tabla_calidad
    }


def armonizar_regional(df, anio):
    df = normalizar_columnas(df)

    rename_map = {
        "tipo_regimen": "regimen_salud",
        "tip_ss_": "regimen_salud",
        "edad_": "edad",
        "fecha_con": "fecha_consulta",
        "fec_con_": "fecha_consulta",
        "fecha_res_bi": "fecha_resultado_biopsia",
        "fec_res_bi": "fecha_resultado_biopsia",
        "oportunidad_diagnostico_dias": "oportunidad_diagnostica_dias",
        "tipo_cancer_mama": "tipo_tumor",
        "regimen_seguridad_social": "regimen_salud"
    }

    for viejo, nuevo in rename_map.items():
        if viejo in df.columns:
            df = df.rename(columns={viejo: nuevo})

    df["anio"] = anio
    return df


def filtrar_valle(df):
    if "departamento" not in df.columns:
        return df

    depto = df["departamento"].astype(str).str.upper().str.strip()
    df_filtrado = df[depto.str.contains("VALLE", na=False)].copy()

    if len(df_filtrado) > 0:
        return df_filtrado

    return df


def transformar_valle_2018(df):
    df = armonizar_regional(df, 2018)
    df = filtrar_valle(df)

    df["oportunidad_diagnostica_dias"] = pd.to_numeric(df["oportunidad_diagnostica_dias"], errors="coerce")
    df["caso_valido"] = (
        df["oportunidad_diagnostica_dias"].notna() &
        (df["oportunidad_diagnostica_dias"] >= 0)
    )

    if "edad" in df.columns:
        df["edad"] = pd.to_numeric(df["edad"], errors="coerce")
        df = crear_grupo_edad(df, "edad")

    df = formatear_fechas_iso_df(df)

    return df


def transformar_valle_reciente(df, anio):
    df = armonizar_regional(df, anio)
    df = filtrar_valle(df)

    df["fecha_consulta"] = convertir_fecha(df["fecha_consulta"])
    df["fecha_resultado_biopsia"] = convertir_fecha(df["fecha_resultado_biopsia"])

    df["oportunidad_diagnostica_dias"] = (df["fecha_resultado_biopsia"] - df["fecha_consulta"]).dt.days
    df["caso_valido"] = (
        df["oportunidad_diagnostica_dias"].notna() &
        (df["oportunidad_diagnostica_dias"] >= 0)
    )

    if "edad" in df.columns:
        df["edad"] = pd.to_numeric(df["edad"], errors="coerce")
        df = crear_grupo_edad(df, "edad")

    df = formatear_fechas_iso_df(df)

    return df


def resumir_serie_historica(df_regional):
    df_validos = df_regional[df_regional["caso_valido"]].copy()

    tabla = (
        df_validos.groupby("anio")["oportunidad_diagnostica_dias"]
        .agg(
            n_validos="count",
            media="mean",
            mediana="median",
            minimo="min",
            maximo="max"
        )
        .reset_index()
        .sort_values("anio")
    )

    tabla["media"] = tabla["media"].round(2)
    tabla["mediana"] = tabla["mediana"].round(2)

    return tabla


def proyeccion_exploratoria_2025(tabla_serie):
    recientes = tabla_serie[tabla_serie["anio"].isin([2021, 2022, 2023])].copy()

    if len(recientes) == 0:
        return pd.DataFrame({
            "anio_proyeccion": [2025],
            "metodo": ["sin_datos"],
            "mediana_proyectada": [np.nan],
            "nota": ["No fue posible proyectar"]
        })

    mediana_proyectada = round(recientes["mediana"].median(), 2)

    return pd.DataFrame({
        "anio_proyeccion": [2025],
        "metodo": ["mediana_robusta_2021_2023"],
        "mediana_proyectada": [mediana_proyectada],
        "nota": ["Escenario exploratorio, no valor oficial observado"]
    })


def calcular_brechas(df_hospital_validos, tabla_serie, tabla_proyeccion):
    mediana_hospital = round(df_hospital_validos["oportunidad_diagnostica_dias"].median(), 2)
    media_hospital = round(df_hospital_validos["oportunidad_diagnostica_dias"].mean(), 2)

    recientes = tabla_serie[tabla_serie["anio"].isin([2021, 2022, 2023])].copy()
    mediana_regional_reciente = round(recientes["mediana"].median(), 2) if len(recientes) > 0 else np.nan

    mediana_proyectada = tabla_proyeccion.loc[0, "mediana_proyectada"]

    brechas = pd.DataFrame({
        "comparacion": [
            "Hospital 2025 vs mediana regional reciente 2021-2023",
            "Hospital 2025 vs proyección exploratoria regional 2025",
            "Media hospital 2025"
        ],
        "valor_hospital": [
            mediana_hospital,
            mediana_hospital,
            media_hospital
        ],
        "valor_referencia": [
            mediana_regional_reciente,
            mediana_proyectada,
            np.nan
        ],
        "brecha_dias": [
            round(mediana_hospital - mediana_regional_reciente, 2) if pd.notna(mediana_regional_reciente) else np.nan,
            round(mediana_hospital - mediana_proyectada, 2) if pd.notna(mediana_proyectada) else np.nan,
            np.nan
        ]
    })

    return brechas


def resumir_factor(df, columna, nombre_factor):
    if columna not in df.columns:
        return pd.DataFrame({
            "factor": [nombre_factor],
            "categoria": ["NO DISPONIBLE"],
            "n_validos": [0],
            "media": [np.nan],
            "mediana": [np.nan],
            "minimo": [np.nan],
            "maximo": [np.nan],
            "observacion": ["La variable no existe en la base"]
        })

    df_factor = df[df[columna].notna()].copy()

    if len(df_factor) == 0:
        return pd.DataFrame({
            "factor": [nombre_factor],
            "categoria": ["SIN DATO UTIL"],
            "n_validos": [0],
            "media": [np.nan],
            "mediana": [np.nan],
            "minimo": [np.nan],
            "maximo": [np.nan],
            "observacion": ["La variable existe pero no tiene datos útiles en los casos válidos"]
        })

    tabla = (
        df_factor.groupby(columna)["oportunidad_diagnostica_dias"]
        .agg(
            n_validos="count",
            media="mean",
            mediana="median",
            minimo="min",
            maximo="max"
        )
        .reset_index()
        .rename(columns={columna: "categoria"})
        .sort_values("mediana", ascending=False)
    )

    tabla["factor"] = nombre_factor
    tabla["observacion"] = ""
    tabla["media"] = tabla["media"].round(2)
    tabla["mediana"] = tabla["mediana"].round(2)

    return tabla[["factor", "categoria", "n_validos", "media", "mediana", "minimo", "maximo", "observacion"]]

def reemplazar_fecha_no_aplica(df):
    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].replace({
                "1800-01-01": "No aplica",
                "1800-1-1": "No aplica"
            })

    return df

def transformar_datos(datos_crudos):
    print("Aquí se transforman los datos")

    # HOSPITAL
    hospital = transformar_hospital(datos_crudos["hospital"])

    # REGIONALES
    valle_2018 = transformar_valle_2018(datos_crudos["valle_2018"])
    valle_2021 = transformar_valle_reciente(datos_crudos["valle_2021"], 2021)
    valle_2022 = transformar_valle_reciente(datos_crudos["valle_2022"], 2022)
    valle_2023 = transformar_valle_reciente(datos_crudos["valle_2023"], 2023)

    regional_todas = pd.concat([valle_2018, valle_2021, valle_2022, valle_2023], ignore_index=True, sort=False)

    # SERIE HISTÓRICA
    tabla_serie = resumir_serie_historica(regional_todas)

    # PROYECCIÓN
    tabla_proyeccion = proyeccion_exploratoria_2025(tabla_serie)

    # BRECHAS
    tabla_brechas = calcular_brechas(hospital["hospital_validos"], tabla_serie, tabla_proyeccion)

    # FACTORES ASOCIADOS
    tabla_cie10 = resumir_factor(hospital["hospital_validos"], "var017", "CIE10")
    tabla_edad = resumir_factor(hospital["hospital_validos"], "grupo_edad", "GRUPO_EDAD")
    tabla_regimen = resumir_factor(hospital["hospital_validos"], "var010", "REGIMEN_SALUD")

    # INDICADORES
    indicadores_hospital = pd.DataFrame({
        "indicador": [
            "n_validos_hospital",
            "media_hospital",
            "mediana_hospital",
            "minimo_hospital",
            "maximo_hospital"
        ],
        "valor": [
            len(hospital["hospital_validos"]),
            round(hospital["hospital_validos"]["oportunidad_diagnostica_dias"].mean(), 2) if len(hospital["hospital_validos"]) > 0 else np.nan,
            round(hospital["hospital_validos"]["oportunidad_diagnostica_dias"].median(), 2) if len(hospital["hospital_validos"]) > 0 else np.nan,
            round(hospital["hospital_validos"]["oportunidad_diagnostica_dias"].min(), 2) if len(hospital["hospital_validos"]) > 0 else np.nan,
            round(hospital["hospital_validos"]["oportunidad_diagnostica_dias"].max(), 2) if len(hospital["hospital_validos"]) > 0 else np.nan
        ]
    })

    indicadores = pd.concat(
        [hospital["calidad_hospital"], indicadores_hospital],
        ignore_index=True
    )

    hospital_mama = formatear_fechas_iso_df(hospital["hospital_mama"])
    hospital_validos = formatear_fechas_iso_df(hospital["hospital_validos"])
    regional_todas = formatear_fechas_iso_df(regional_todas)
    serie_historica = formatear_fechas_iso_df(tabla_serie)
    proyeccion_2025 = formatear_fechas_iso_df(tabla_proyeccion)
    brechas = formatear_fechas_iso_df(tabla_brechas)
    retraso_cie10 = formatear_fechas_iso_df(tabla_cie10)
    retraso_edad = formatear_fechas_iso_df(tabla_edad)
    retraso_regimen = formatear_fechas_iso_df(tabla_regimen)
    indicadores = formatear_fechas_iso_df(indicadores)

    hospital_mama = reemplazar_fecha_no_aplica(hospital_mama)
    hospital_validos = reemplazar_fecha_no_aplica(hospital_validos)
    regional_todas = reemplazar_fecha_no_aplica(regional_todas)
    serie_historica = reemplazar_fecha_no_aplica(serie_historica)
    proyeccion_2025 = reemplazar_fecha_no_aplica(proyeccion_2025)
    brechas = reemplazar_fecha_no_aplica(brechas)
    retraso_cie10 = reemplazar_fecha_no_aplica(retraso_cie10)
    retraso_edad = reemplazar_fecha_no_aplica(retraso_edad)
    retraso_regimen = reemplazar_fecha_no_aplica(retraso_regimen)
    indicadores = reemplazar_fecha_no_aplica(indicadores)

    registrar_filas("Transform", "hospital_mama", len(hospital["hospital_mama"]))
    registrar_filas("Transform", "hospital_validos", len(hospital["hospital_validos"]))
    registrar_filas("Transform", "regional_todas", len(regional_todas))
    registrar_filas("Transform", "serie_historica", len(tabla_serie))
    registrar_filas("Transform", "proyeccion_2025", len(tabla_proyeccion))
    registrar_filas("Transform", "brechas", len(tabla_brechas))
    registrar_filas("Transform", "retraso_cie10", len(tabla_cie10))
    registrar_filas("Transform", "retraso_edad", len(tabla_edad))
    registrar_filas("Transform", "retraso_regimen", len(tabla_regimen))

    return {
        "hospital_mama": hospital_mama,
        "hospital_validos": hospital_validos,
        "calidad_hospital": hospital["calidad_hospital"],
        "regional_todas": regional_todas,
        "serie_historica": serie_historica,
        "proyeccion_2025": proyeccion_2025,
        "brechas": brechas,
        "retraso_cie10": retraso_cie10,
        "retraso_edad": retraso_edad,
        "retraso_regimen": retraso_regimen,
        "indicadores": indicadores
    }