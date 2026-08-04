"""
Prueba de carga realista, basada en la capacidad real de cocineros por zona.
Corre en la PC PRINCIPAL. En vez de un solo golpe instantaneo (kafka_producer.py lote),
esto simula una hora de almuerzo real: arranca estable, sube hasta saturar, y baja.

SUPUESTO A DOCUMENTAR: cada cocinero tarda en promedio 4 minutos en preparar un
pedido (TIEMPO_PREPARACION_MIN). No es un dato medido, es una decision de diseno
para poder calcular una tasa de llegada "sostenible" contra la cual comparar el pico.

Requiere:
    Que kafka_producer.py y app.py esten en la misma carpeta (Fase 2).
    Que el broker, mongod, consumer.py y api.py ya esten corriendo.

Uso:
    python prueba_realista.py
"""
import random
import time

from kafka_producer import crear_productor, topic_para
from app import generar_pedido

TIEMPO_PREPARACION_MIN = 4     # minutos por pedido por cocinero (supuesto, documentar)
VENTANA_ACTIVOS_MIN = 5        # debe coincidir con VENTANA_ACTIVOS_MINUTOS en consumer.py

CAPACIDAD_COCINEROS = {
    "juticalpa": 12,
    "catacamas": 8,
}

# Cada fase dura un tiempo real y multiplica la tasa sostenible.
# multiplicador 1.0 = justo en el limite (ratio ~1.0 en el dashboard)
FASES = [
    {"nombre": "Estable (antes del almuerzo)", "duracion_seg": 90,  "multiplicador": 0.6},
    {"nombre": "Hora pico (rush de almuerzo)", "duracion_seg": 150, "multiplicador": 1.8},
    {"nombre": "Bajando (despues del pico)",   "duracion_seg": 90,  "multiplicador": 0.4},
]


def tasa_sostenible_por_min(zona: str) -> float:
    """Pedidos/minuto que la zona puede recibir sin que el ratio pase de 1.0."""
    activos_sostenibles = CAPACIDAD_COCINEROS[zona] * (VENTANA_ACTIVOS_MIN / TIEMPO_PREPARACION_MIN)
    return activos_sostenibles / VENTANA_ACTIVOS_MIN


def correr_prueba():
    productor = crear_productor()
    tasa_base = {z: tasa_sostenible_por_min(z) for z in CAPACIDAD_COCINEROS}

    print("=== Prueba de carga realista ===")
    print(f"Supuesto: {TIEMPO_PREPARACION_MIN} min por pedido por cocinero\n")
    print("Tasas sostenibles (ratio = 1.0):")
    for z, t in tasa_base.items():
        print(f"  {z}: {t:.2f} pedidos/min  ({CAPACIDAD_COCINEROS[z]} cocineros)")
    print()

    enviados = {z: 0 for z in CAPACIDAD_COCINEROS}
    inicio_total = time.monotonic()

    for fase in FASES:
        print(f"--- {fase['nombre']} | {fase['duracion_seg']}s | objetivo ratio ~{fase['multiplicador']:.1f} ---")
        fin_fase = time.monotonic() + fase["duracion_seg"]

        while time.monotonic() < fin_fase:
            for zona in CAPACIDAD_COCINEROS:
                tasa_por_seg = (tasa_base[zona] * fase["multiplicador"]) / 60
                if random.random() < tasa_por_seg:
                    pedido = generar_pedido(zona_forzada=zona)
                    topic = topic_para(pedido)
                    productor.send(topic, value=pedido)
                    enviados[zona] += 1
            time.sleep(1)

    productor.flush()
    duracion_total = time.monotonic() - inicio_total

    print()
    print(f"Prueba terminada en {duracion_total / 60:.1f} min")
    for z, n in enviados.items():
        print(f"  {z}: {n} pedidos enviados en total")
    print("\nNota: el dashboard tarda hasta 10s en reflejar cada cambio (ciclo del consumer),")
    print("y el ratio no baja de inmediato al terminar el pico: la ventana de 5 min sigue")
    print("contando los pedidos recientes hasta que 'envejecen' y salen de la ventana.")

    productor.close()


if __name__ == "__main__":
    correr_prueba()
