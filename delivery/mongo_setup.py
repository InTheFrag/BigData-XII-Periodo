"""
Fase 3 - Setup inicial de MongoDB.
Corre en la PC PRINCIPAL (donde vive Mongo). Se corre UNA sola vez
(o cada vez que quieran resetear los datos de prueba).

Requiere:
    pip install pymongo --break-system-packages
    mongod corriendo en localhost:27017

Uso:
    python mongo_setup.py
"""
from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017"  # local: este script corre en la misma PC principal
DB_NAME = "delivery_saturacion"

# Capacidad inicial de cocineros disponibles por zona.
# NO viene del stream de Kafka: es un dato de contexto/configuracion necesario
# para calcular el ratio de saturacion. Documenten en la bitacora si lo ajustan.
CAPACIDAD_INICIAL = {
    "juticalpa": 12,
    "catacamas": 8,
}


def preparar_base():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    pedidos_procesados = db["pedidos_procesados"]
    capacidad_zonas = db["capacidad_zonas"]

    # Indice unico: un segundo insert con el mismo id_pedido lanza DuplicateKeyError,
    # y el consumidor lo captura y descarta sin llevar un registro de IDs en memoria.
    pedidos_procesados.create_index([("id_pedido", ASCENDING)], unique=True)
    pedidos_procesados.create_index([("zona", ASCENDING), ("timestamp", ASCENDING)])
    pedidos_procesados.create_index([("restaurante", ASCENDING)])

    for zona, cocineros in CAPACIDAD_INICIAL.items():
        capacidad_zonas.update_one(
            {"_id": zona},
            {"$set": {"cocineros_disponibles": cocineros}},
            upsert=True,
        )

    print("Colecciones e indices listos.")
    print("Capacidad de cocineros configurada:")
    for doc in capacidad_zonas.find():
        print(f"  {doc['_id']}: {doc['cocineros_disponibles']} cocineros")

    client.close()


if __name__ == "__main__":
    preparar_base()
