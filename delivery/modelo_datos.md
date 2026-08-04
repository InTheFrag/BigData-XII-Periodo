# Modelo del evento "pedido" — justificación

## Campos del evento

| Campo | Tipo | Por qué está |
|---|---|---|
| `id_pedido` | UUID string | Identificador único. Necesario para detectar duplicados en Fase 3 (Kafka puede reentregar mensajes ante reintentos del productor, y el consumidor debe poder reconocerlos). |
| `zona` | string (`juticalpa` \| `catacamas`) | Define a qué topic de Kafka se enruta el evento. Es el eje central del balance de saturación. |
| `id_usuario` | string | Simula el cliente que hace el pedido. |
| `restaurante` | string | Uno de 8 restaurantes fijos (4 por ciudad), con probabilidad de selección desigual. Permite calcular "total de pedidos por restaurante" que pide la Fase 3. |
| `monto` | float (Lempiras) | Valor económico del pedido. |
| `cantidad_items` | int | Cantidad de productos en el pedido; dimensión de tamaño distinta del monto. |
| `timestamp` | ISO 8601 | Momento de creación. Permite calcular tasas por minuto/hora y detectar picos. |
| `estado` | string, fijo `"creado"` | Deja espacio para extender el modelo más adelante si el equipo decide agregar estados (ej. `"en_cocina"`, `"listo"`). |

## Por qué el modelo no es uniforme (realismo)

1. **Las ciudades no piden por igual.** Juticalpa concentra más actividad comercial que Catacamas: **65% / 35%**, no 50/50.
2. **Los restaurantes no son igual de populares.** Dentro de cada ciudad, un restaurante concentra ~40% de los pedidos y los otros tres se reparten el resto — pesos, no selección uniforme.
3. **Hay horas pico y horas muertas.** El generador acepta una "hora simulada" que multiplica la probabilidad de pedido según franja horaria (almuerzo 12–14h, cena 19–21h con más peso).

## Imperfecciones intencionales (para la Fase 3)

El generador inyecta a propósito:
- ~2% de eventos **duplicados** (mismo `id_pedido` repetido)
- ~2% de eventos **malformados** (campo faltante o de tipo incorrecto)

Esto simula imperfecciones reales de red/cliente — la Fase 3 exige decidir qué hacer con datos sucios, y si el generador entrega datos perfectos no hay nada que limpiar ni que defender.

## Nota de dominio (cambio de "repartidores" a "cocineros")

El balance de saturación en este proyecto se calcula contra **capacidad de cocineros**, no de repartidores: el cuello de botella que se modela es la preparación en cocina, no la entrega. Esto no cambia el esquema del evento `pedido` (que no incluye reparto/entrega), pero sí afecta cómo se calcula la saturación en la Fase 3 (pedidos activos / cocineros disponibles por zona) y cómo se presenta en el dashboard de la Fase 4.
