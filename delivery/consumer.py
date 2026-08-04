"""
Fase 3 - Consumidor + agregador.
Corre en la PC PRINCIPAL. Lee los 3 topics de Kafka (2 zonas + malformados),
limpia y guarda en MongoDB, y cada 10 segundos recalcula el balance de saturacion.

Requiere:
    pip install kafka-python pymongo --break-system-packages
    MongoDB corriendo en localhost:27017 (correr mongo_setup.py antes, una vez)
    Broker de Kafka accesible en KAFKA_BROKER

Uso:
    python consumer.py
"""
import json
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

KAFKA_BROKER = "192.168.0.25:9092"
TOPICS = ["pedidos-juticalpa", "pedidos-catacamas", "pedidos-malformados"]

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "delivery_saturacion"

VENTANA_ACTIVOS_MINUTOS = 5   # "pedidos activos" = creados en los ultimos N minutos
INTERVALO_AGREGACION_SEG = 10


def limpiar(pedido: dict) -> tuple[bool, str]:
    """Valida un pedido. Devuelve (es_valido, motivo_si_no_lo_es)."""
    if "zona" not in pedido or pedido["zona"] not in ("juticalpa", "catacamas"):
        return False, "zona_ausente_o_invalida"
    if not isinstance(pedido.get("cantidad_items"), int):
        return False, "cantidad_items_invalida"
    try:
        pedido["monto"] = float(pedido["monto"])
    except (TypeError, ValueError):
        return False, "monto_no_numerico"
    return True, ""


def procesar_mensaje(db, pedido: dict, contador: dict) -> None:
    contador["extraidos"] += 1  # ETL: Extract - todo lo que llega de Kafka cuenta aquí
    valido, motivo = limpiar(pedido)

    if not valido:
        # ETL: Transform (rechazado) - no pasa la limpieza, se guarda como evidencia
        db.pedidos_descartados.insert_one({
            "id_pedido": pedido.get("id_pedido"),
            "motivo": motivo,
            "payload_original": pedido,
            "recibido_en": datetime.now(timezone.utc),
        })
        contador["transformados_descartados"] += 1
        return

    contador["transformados_ok"] += 1  # ETL: Transform (aceptado)

    try:
        db.pedidos_procesados.insert_one(pedido)
        contador["cargados"] += 1  # ETL: Load (nuevo)
    except DuplicateKeyError:
        contador["duplicados"] += 1  # ETL: Load (rechazado por duplicado)


def publicar_metricas_etl(db, contador: dict) -> None:
    """Expone el estado del pipeline ETL para que la API/dashboard lo muestren en vivo."""
    db.etl_metricas.update_one(
        {"_id": "global"},
        {"$set": {
            "extraidos": contador["extraidos"],
            "transformados_ok": contador["transformados_ok"],
            "transformados_descartados": contador["transformados_descartados"],
            "cargados": contador["cargados"],
            "duplicados": contador["duplicados"],
            "actualizado_en": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )


def recalcular_balance(db) -> None:
    """Recalcula pedidos activos por zona y el ratio contra la capacidad de cocineros."""
    desde = datetime.now(timezone.utc) - timedelta(minutes=VENTANA_ACTIVOS_MINUTOS)
    desde_iso = desde.isoformat()

    for cap in db.capacidad_zonas.find():
        zona = cap["_id"]
        cocineros = cap["cocineros_disponibles"]

        pedidos_activos = db.pedidos_procesados.count_documents({
            "zona": zona,
            "timestamp": {"$gte": desde_iso},
        })
        ratio = pedidos_activos / cocineros if cocineros else 0

        db.balance_saturacion.update_one(
            {"_id": zona},
            {"$set": {
                "pedidos_activos": pedidos_activos,
                "cocineros_disponibles": cocineros,
                "ratio": round(ratio, 3),
                "actualizado_en": datetime.now(timezone.utc).isoformat(),
            }},
            upsert=True,
        )

    # Totales acumulados por restaurante (no es ventana movil, es historico completo)
    pipeline = [
        {"$group": {"_id": {"zona": "$zona", "restaurante": "$restaurante"}, "total": {"$sum": 1}}}
    ]
    for doc in db.pedidos_procesados.aggregate(pipeline):
        db.totales_por_restaurante.update_one(
            {"_id": f"{doc['_id']['zona']}::{doc['_id']['restaurante']}"},
            {"$set": {
                "zona": doc["_id"]["zona"],
                "restaurante": doc["_id"]["restaurante"],
                "total_pedidos": doc["total"],
            }},
            upsert=True,
        )


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="procesador-saturacion",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,  # para poder revisar el reloj de agregacion periodicamente
    )

    contador = {
        "extraidos": 0,
        "transformados_ok": 0,
        "transformados_descartados": 0,
        "cargados": 0,
        "duplicados": 0,
    }
    ultima_agregacion = time.monotonic()

    print(f"Escuchando {TOPICS} en {KAFKA_BROKER}...")
    print(f"Recalculando balance cada {INTERVALO_AGREGACION_SEG}s. Ctrl+C para detener.")

    try:
        while True:
            for mensaje in consumer:
                procesar_mensaje(db, mensaje.value, contador)

            if time.monotonic() - ultima_agregacion >= INTERVALO_AGREGACION_SEG:
                recalcular_balance(db)
                publicar_metricas_etl(db, contador)
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] ETL → "
                    f"extraidos={contador['extraidos']} "
                    f"transform_ok={contador['transformados_ok']} "
                    f"transform_descartados={contador['transformados_descartados']} "
                    f"cargados={contador['cargados']} "
                    f"duplicados={contador['duplicados']}"
                )
                ultima_agregacion = time.monotonic()
    except KeyboardInterrupt:
        print("\nDeteniendo consumidor...")
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
