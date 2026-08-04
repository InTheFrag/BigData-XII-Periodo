"""
Fase 1 - App generadora de pedidos.
Corre en la PC PRINCIPAL (Windows/WSL). Todavia no depende de Kafka:
en esta fase solo generamos y medimos: en Fase 2 conectamos a Kafka.

Instalar:
    pip install fastapi uvicorn faker --break-system-packages

Correr:
    uvicorn app:app --reload --port 8000

Probar:
    curl -X POST http://localhost:8000/pedido
    curl -X POST "http://localhost:8000/pedidos/lote?cantidad=1000"
"""
import random
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from faker import Faker

app = FastAPI(title="Generador de pedidos - Delivery Juticalpa/Catacamas")
fake = Faker("es_ES")

# ---------------------------------------------------------------------------
# Configuracion de realismo: zonas, restaurantes y sus pesos de popularidad
# ---------------------------------------------------------------------------

ZONAS = ["juticalpa", "catacamas"]
PESO_ZONAS = [0.65, 0.35]  # Juticalpa concentra mas actividad

RESTAURANTES = {
    "juticalpa": ["Pollo Campero Juticalpa", "Baleadas Dona Tere", "Pizza Olanchana", "Comedor El Fogon"],
    "catacamas": ["Asados Catacamas", "Reposteria La Espiga", "Chop Suey Oriental", "Cafe Colonial"],
}
# Un restaurante concentra ~40% de los pedidos de su ciudad, el resto se reparte
PESO_RESTAURANTES = [0.40, 0.25, 0.20, 0.15]

# Multiplicador de probabilidad de pedido segun hora del dia (simula horas pico)
PESO_POR_HORA = {h: 0.3 for h in range(24)}
PESO_POR_HORA.update({12: 1.0, 13: 1.0, 14: 0.8, 19: 1.0, 20: 1.0, 21: 0.8})

TASA_DUPLICADOS = 0.02
TASA_MALFORMADOS = 0.02

_ultimos_ids = []  # pequeno buffer para poder "duplicar" ids recientes


def _generar_pedido_base(hora_simulada: int | None = None, zona_forzada: str | None = None) -> dict:
    zona = zona_forzada if zona_forzada else random.choices(ZONAS, weights=PESO_ZONAS, k=1)[0]
    restaurante = random.choices(RESTAURANTES[zona], weights=PESO_RESTAURANTES, k=1)[0]
    hora = hora_simulada if hora_simulada is not None else datetime.now().hour

    pedido_id = str(uuid.uuid4())
    _ultimos_ids.append(pedido_id)
    if len(_ultimos_ids) > 200:
        _ultimos_ids.pop(0)

    return {
        "id_pedido": pedido_id,
        "zona": zona,
        "id_usuario": f"user_{fake.random_int(min=1000, max=9999)}",
        "restaurante": restaurante,
        "monto": round(random.uniform(80, 650), 2),  # Lempiras
        "cantidad_items": random.randint(1, 6),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "estado": "creado",
        "_hora_simulada": hora,  # informativo, no forma parte del esquema final
    }


def _aplicar_imperfecciones(pedido: dict) -> dict:
    """Con cierta probabilidad, corrompe o duplica el pedido (a proposito)."""
    r = random.random()
    if r < TASA_DUPLICADOS and _ultimos_ids:
        pedido["id_pedido"] = random.choice(_ultimos_ids)
    elif r < TASA_DUPLICADOS + TASA_MALFORMADOS:
        campo_a_danar = random.choice(["monto", "cantidad_items", "zona"])
        if campo_a_danar == "monto":
            pedido["monto"] = "no-numerico"
        elif campo_a_danar == "cantidad_items":
            pedido["cantidad_items"] = None
        else:
            pedido.pop("zona", None)
    return pedido


def generar_pedido(hora_simulada: int | None = None, zona_forzada: str | None = None) -> dict:
    pedido = _generar_pedido_base(hora_simulada, zona_forzada)
    return _aplicar_imperfecciones(pedido)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/pedido")
def enviar_pedido_individual(hora_simulada: int | None = None):
    """Genera y devuelve UN pedido (simula el envio 'uno por uno')."""
    return generar_pedido(hora_simulada)


@app.post("/pedidos/lote")
def enviar_pedidos_lote(cantidad: int = 1000, hora_simulada: int | None = None):
    """Genera 'cantidad' pedidos de un solo golpe (simula un pico, ej. inicio del almuerzo)."""
    pedidos = [generar_pedido(hora_simulada) for _ in range(cantidad)]
    return {"cantidad_generada": len(pedidos), "pedidos": pedidos}


@app.get("/")
def salud():
    return {"status": "ok", "zonas": ZONAS}
