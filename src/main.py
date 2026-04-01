from extract import extraer_datos
from transform import transformar_datos
from load import cargar_datos


def main():
    datos_crudos = extraer_datos()
    datos_transformados = transformar_datos(datos_crudos)
    cargar_datos(datos_transformados)
    print("Pipeline ETL completado.")


if __name__ == "__main__":
    main()