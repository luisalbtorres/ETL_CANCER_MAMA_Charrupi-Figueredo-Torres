import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Dashboard Cáncer de Mama", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "db" / "etl_cancer_mama.sqlite"


st.cache_data
def leer_tabla(nombre_tabla):
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    finally:
        conn.close()


st.title("Dashboard ETL - Cáncer de Mama")

if not DB_PATH.exists():
    st.error("No existe la base SQLite. Primero ejecuta: python src/main.py")
    st.stop()

indicadores = leer_tabla("indicadores")
serie_historica = leer_tabla("serie_historica")
brechas = leer_tabla("brechas")
hospital_validos = leer_tabla("hospital_validos")
retraso_regimen = leer_tabla("retraso_regimen")
retraso_edad = leer_tabla("retraso_edad")
retraso_cie10 = leer_tabla("retraso_cie10")

st.subheader("Indicadores generales")

col1, col2, col3 = st.columns(3)

try:
    n_validos = indicadores.loc[indicadores["indicador"] == "n_validos_hospital", "valor"].iloc[0]
    media_hospital = indicadores.loc[indicadores["indicador"] == "media_hospital", "valor"].iloc[0]
    mediana_hospital = indicadores.loc[indicadores["indicador"] == "mediana_hospital", "valor"].iloc[0]

    col1.metric("Casos válidos hospital", n_validos)
    col2.metric("Media hospital", media_hospital)
    col3.metric("Mediana hospital", mediana_hospital)
except Exception:
    st.warning("No fue posible cargar algunas métricas principales.")

st.subheader("Serie histórica regional")
if not serie_historica.empty and "anio" in serie_historica.columns and "mediana" in serie_historica.columns:
    st.line_chart(serie_historica.set_index("anio")["mediana"])
    st.dataframe(serie_historica, use_container_width=True, hide_index=True)
else:
    st.info("No hay datos suficientes para la serie histórica.")

st.subheader("Brechas")
st.dataframe(brechas, use_container_width=True, hide_index=True)

st.subheader("Retraso por régimen")
st.dataframe(retraso_regimen, use_container_width=True, hide_index=True)

st.subheader("Retraso por grupo de edad")
st.dataframe(retraso_edad, use_container_width=True, hide_index=True)

st.subheader("Retraso por CIE10")
st.dataframe(retraso_cie10, use_container_width=True, hide_index=True)

st.subheader("Casos hospitalarios válidos")
st.dataframe(hospital_validos, use_container_width=True, hide_index=True)