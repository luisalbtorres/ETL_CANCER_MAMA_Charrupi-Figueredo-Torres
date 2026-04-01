from extract import extraer_datos
from transform import transformar_datos
from load import cargar_datos
from metricas import medir_etapa, imprimir_resumen_final


def main():
    try:
        print("Iniciando pipeline ETL de cáncer de mama...")

        datos_crudos = medir_etapa("Extract", extraer_datos)
        datos_transformados = medir_etapa("Transform", transformar_datos, datos_crudos)
        medir_etapa("Load", cargar_datos, datos_transformados)

        print("Proceso ETL terminado correctamente")

    except Exception as e:
        print(f"Error en el pipeline: {e}")
        raise

    finally:
        imprimir_resumen_final()


if __name__ == "__main__":
    main()