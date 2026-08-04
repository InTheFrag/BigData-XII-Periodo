"""
Fase 2 - Productor de Kafka.
Corre en la PC PRINCIPAL (WSL). Reutiliza generar_pedido() de la Fase 1
y publica cada evento en el topic correspondiente segun su zona.

Requiere:
    pip install kafka-python --break-system-packages
    Que app.py (Fase 1) este en la misma carpeta.
    Que el broker de la laptop este corriendo y accesible en KAFKA_BROKER.

Uso:
    python kafka_producer.py individual
    python kafka_producer.py lote 20000
"""
import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app import generar_pedido

# Cambia esto por la IP real de tu laptop si es distinta
KAFKA_BROKER = "192.168.0.25:9092"

TOPIC_POR_ZONA = {
    "juticalpa": "pedidos-juticalpa",
    "catacamas": "pedidos-catacamas",
}
TOPIC_FALLBACK = "pedidos-malformados"


def crear_productor() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # linger_ms agrupa mensajes en lotes internos antes de enviar:
        # mejora throughput en ráfagas, a costa de un poco de latencia.
        linger_ms=20,
        batch_size=32768,
        acks=1,
    )


def topic_para(pedido: dict) -> str:
    zona = pedido.get("zona")
    return TOPIC_POR_ZONA.get(zona, TOPIC_FALLBACK)


def enviar_individual(productor: KafkaProducer) -> None:
    pedido = generar_pedido()
    topic = topic_para(pedido)
    future = productor.send(topic, value=pedido)
    try:
        future.get(timeout=10)
        print(f"Enviado a '{topic}': {pedido['id_pedido']}")
    except KafkaError as e:
        print(f"Fallo el envío: {e}")
    productor.flush()


def enviar_lote(productor: KafkaProducer, cantidad: int) -> None:
    errores = 0

    def callback_error(exc):
        nonlocal errores
        errores += 1

    inicio = time.perf_counter()
    for _ in range(cantidad):
        pedido = generar_pedido()
        topic = topic_para(pedido)
        productor.send(topic, value=pedido).add_errback(callback_error)

    productor.flush()  # espera a que todo el buffer salga por la red
    duracion = time.perf_counter() - inicio

    print(f"Lote de {cantidad} pedidos enviado en {duracion:.2f} s")
    print(f"Throughput de ingesta: {cantidad / duracion:.1f} pedidos/segundo")
    print(f"Errores de entrega reportados: {errores}")


if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "individual"
    productor = crear_productor()

    if modo == "individual":
        enviar_individual(productor)
    elif modo == "lote":
        cantidad = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        enviar_lote(productor, cantidad)
    else:
        print("Uso: python kafka_producer.py [individual | lote <cantidad>]")

    productor.close()
