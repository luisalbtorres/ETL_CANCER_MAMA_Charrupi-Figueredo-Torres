import os
import time
import tracemalloc
import psutil

_metricas = {}
_MB = 1024 * 1024
_proceso = psutil.Process(os.getpid())


def medir_etapa(nombre_etapa, funcion, *args, **kwargs):
    """
    Mide tiempo, RAM delta, RAM pico y RAM total del proceso para una etapa.
    """
    ram_inicio = _proceso.memory_info().rss

    tracemalloc.start()
    t0 = time.perf_counter()

    resultado = funcion(*args, **kwargs)

    tiempo_seg = time.perf_counter() - t0
    _, pico = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    ram_fin = _proceso.memory_info().rss

    filas_previas = _metricas.get(nombre_etapa, {}).get("filas", {})

    _metricas[nombre_etapa] = {
        "tiempo_seg": tiempo_seg,
        "ram_delta_mb": (ram_fin - ram_inicio) / _MB,
        "ram_pico_mb": pico / _MB,
        "ram_proceso_mb": ram_fin / _MB,
        "filas": filas_previas
    }

    return resultado


def registrar_filas(etapa, nombre, cantidad):
    """
    Registra cantidad de filas relevantes por etapa.
    """
    if etapa not in _metricas:
        _metricas[etapa] = {}

    if "filas" not in _metricas[etapa]:
        _metricas[etapa]["filas"] = {}

    try:
        cantidad = int(cantidad)
    except Exception:
        pass

    _metricas[etapa]["filas"][nombre] = cantidad


def obtener_metricas():
    return _metricas


def imprimir_resumen_final():
    """
    Imprime la tabla consolidada de todas las etapas al final del pipeline.
    """
    if not _metricas:
        print("\nNo hay métricas registradas.")
        return

    sep = "═" * 70
    print(f"\n{sep}")
    print("  RESUMEN FINAL DE MÉTRICAS TÉCNICAS DEL PIPELINE")
    print(sep)
    print(f"  {'Etapa':<20} {'Tiempo (s)':>10} {'RAM delta (MB)':>14} {'RAM pico (MB)':>13} {'RAM proc (MB)':>13}")
    print(f"  {'─'*20} {'─'*10} {'─'*14} {'─'*13} {'─'*13}")

    tiempo_total = 0

    for etapa, m in _metricas.items():
        tiempo_total += m.get("tiempo_seg", 0)
        print(
            f"  {etapa:<20} "
            f"{m.get('tiempo_seg', 0):>10.4f} "
            f"{m.get('ram_delta_mb', 0):>+14.2f} "
            f"{m.get('ram_pico_mb', 0):>13.2f} "
            f"{m.get('ram_proceso_mb', 0):>13.2f}"
        )

    print(f"  {'─'*20} {'─'*10} {'─'*14} {'─'*13} {'─'*13}")
    print(f"  {'TOTAL':<20} {tiempo_total:>10.4f}")
    print(sep)

    for etapa, m in _metricas.items():
        if "filas" in m and m["filas"]:
            print(f"\n  Filas [{etapa}]:")
            for nombre, n in m["filas"].items():
                try:
                    print(f"    · {nombre}: {n:,}")
                except Exception:
                    print(f"    · {nombre}: {n}")

    print(sep)