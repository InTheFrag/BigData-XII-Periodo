"""
Fase 4 - API de lectura para el dashboard.
Corre en la PC PRINCIPAL (misma maquina que MongoDB).

Requiere:
    pip install fastapi uvicorn pymongo --break-system-packages

Correr:
    uvicorn api:app --port 8001
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "delivery_saturacion"

app = FastAPI(title="API de balance de saturación")

# El dashboard se abre como archivo local (file://) o en otro puerto,
# así que habilitamos CORS abierto - esto es un proyecto académico, no producción.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


@app.get("/balance")
def obtener_balance():
    """Balance de saturación actual de todas las zonas."""
    zonas = list(db.balance_saturacion.find({}, {"_id": 1, "pedidos_activos": 1, "cocineros_disponibles": 1, "ratio": 1, "actualizado_en": 1}))
    for z in zonas:
        z["zona"] = z.pop("_id")
    return {"zonas": zonas}


@app.get("/balance/{zona}")
def obtener_balance_zona(zona: str):
    """Balance de saturación de UNA zona específica (para cuando el docente la elija en vivo)."""
    doc = db.balance_saturacion.find_one({"_id": zona})
    if not doc:
        return {"error": f"zona '{zona}' no encontrada"}
    doc["zona"] = doc.pop("_id")
    return doc


@app.get("/restaurantes")
def obtener_totales_restaurantes():
    """Total de pedidos por restaurante, agrupado por zona."""
    docs = list(db.totales_por_restaurante.find({}, {"_id": 0}))
    return {"restaurantes": docs}


@app.get("/pedidos")
def obtener_pedidos_recientes(zona: str | None = None, limite: int = 20):
    """Pedidos individuales más recientes, opcionalmente filtrados por zona."""
    filtro = {"zona": zona} if zona else {}
    cursor = (
        db.pedidos_procesados
        .find(filtro, {"_id": 0, "_hora_simulada": 0})
        .sort("timestamp", -1)
        .limit(limite)
    )
    return {"pedidos": list(cursor)}


@app.get("/etl")
def obtener_metricas_etl():
    """Estado en vivo del pipeline ETL: Extract → Transform → Load."""
    doc = db.etl_metricas.find_one({"_id": "global"})
    if not doc:
        return {"extraidos": 0, "transformados_ok": 0, "transformados_descartados": 0, "cargados": 0, "duplicados": 0}
    doc.pop("_id", None)
    return doc


@app.get("/")
def salud():
    return {"status": "ok"}
