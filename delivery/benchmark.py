"""
Mide cuantos pedidos por segundo es capaz de generar el generador (sin HTTP de por medio).
Este numero es el que deben documentar en la bitacora: define el reto que se imponen
para las siguientes fases (Kafka debe poder absorber al menos esta tasa en un pico).

Correr:
    python benchmark.py 50000
"""
import sys
import time

from app import generar_pedido


def medir_throughput(cantidad: int) -> None:
    inicio = time.perf_counter()
    for _ in range(cantidad):
        generar_pedido()
    duracion = time.perf_counter() - inicio

    throughput = cantidad / duracion
    print(f"Pedidos generados: {cantidad}")
    print(f"Tiempo total: {duracion:.3f} s")
    print(f"Throughput: {throughput:.1f} pedidos/segundo")


if __name__ == "__main__":
    cantidad = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    medir_throughput(cantidad)
