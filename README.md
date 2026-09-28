# inventory-service-cinema

Reserva y libera stock temporal de boletos durante el flujo de compra, como bloqueo optimista de corta duración mientras se procesa el pago.

## Responsabilidad

Este servicio **no es la fuente de verdad del stock** — es una capa de reserva rápida en memoria (Redis) que evita sobre-venta mientras dura la saga de compra. Existen tres capas de stock que operan en paralelo; ver la tabla completa en [`../kafka-schemas-cinema/event_contracts_operativos.md`](../kafka-schemas-cinema/event_contracts_operativos.md#semántica-de-stock--fuente-de-verdad):

| Capa | Servicio | Propósito |
|------|----------|-----------|
| Reserva temporal | **inventory-service** (este) | Bloqueo optimista durante el flujo de compra |
| Stock de transacción | booking-service | Validación al crear la compra |
| Stock visible | catalog-service | Fuente autoritativa para el usuario final |

Se usa Redis (no PostgreSQL) porque la reserva es efímera por diseño: vive solo mientras la orden está en curso, se confirma o se compensa (`release`), y no necesita persistencia transaccional ni historial — un `SET`/`DECRBY` atómico en Redis resuelve el caso sin el costo de una tabla y sus locks.

## Stack

- FastAPI + Uvicorn
- `redis` (cliente async) — scripts Lua para las operaciones de reserva/liberación
- `aiokafka` — consumidor y productor de eventos
- Puerto: `8002` — sin ruta pública vía Traefik (servicio interno)

## Reserva atómica con Lua

`app/services/inventory.py` registra tres scripts Lua para evitar condiciones de carrera entre reservas concurrentes de la misma película:

- **Reservar** (`reserve`): decrementa el stock solo si alcanza; devuelve `-2` si la película no está sembrada, `-1` si no hay stock suficiente, `-3` si la orden ya tiene una reserva activa (deduplicación de reintentos).
- **Liberar** (`release`): revierte una reserva usando el `order_id` como clave, así una liberación duplicada no infla el stock dos veces.

## Siembra de inventario

Al arrancar (`app/main.py`), siembra Redis en background consultando `catalog-service` (`GET {CATALOG_SERVICE_URL}/api/v1/movies`), con reintento exponencial (hasta 5 intentos) y sin sobreescribir si ya hay datos (`SET NX`). Verificado en esta sesión (2026-09-18): sembró ~10 películas sin error contra el `catalog-service` local.

Si llega un `order.created` para una película que no fue sembrada (`reserve` devuelve `-2`), el consumidor intenta una **siembra on-demand** de esa película puntual antes de reintentar la reserva (`app/kafka/consumer.py::_seed_movie_on_demand`) — mismo patrón que describe `../ARCHITECTURE.md`.

## Eventos Kafka

| Dirección | Topic | Rol |
|---|---|---|
| Consume | `order.created` | Dispara la reserva; publica `inventory.reserved` o `inventory.insufficient` según el resultado |
| Consume | `inventory.release` | Compensación de saga — revierte una reserva activa |
| Publica | `inventory.reserved` | Reserva exitosa (o duplicada, re-publicada para que booking-service no se quede esperando) |
| Publica | `inventory.insufficient` | No había stock suficiente (o no se pudo sembrar on-demand) |

Contratos y payloads completos en [`../kafka-schemas-cinema/event_contracts_operativos.md`](../kafka-schemas-cinema/event_contracts_operativos.md).

## Variables de entorno

| Variable | Para qué sirve |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, `KAFKA_API_SECRET` | Credenciales de Confluent Cloud (dev y producción usan el mismo cluster, ver `../IMPLEMENTATION-GUIDE.md` Fase 3) |
| `KAFKA_GROUP_ID` | Grupo de consumidor (`inventory-service-group` por defecto) |
| `REDIS_URL` | Conexión a Redis — en local, el contenedor `redis` del `docker-compose`; en producción, Railway |
| `CATALOG_SERVICE_URL` | Endpoint de catalog-service para sembrar stock. **En `.env` local debe ser el hostname del docker-compose (`http://catalog-service:8006`), no la URL pública de Render** — ver `../IMPLEMENTATION-GUIDE.md` Fase 0, fue un bug real corregido el 2026-09-18 |

## Endpoints propios

Además de `/health`, expone dos rutas de operación directa sobre Redis (no pasan por Kafka):

- `GET /inventory/{movie_id}` — consulta el stock reservable actual
- `POST /inventory/{movie_id}/sync?available=N` — sobreescribe el stock manualmente, para cuando Redis y la BD divergen

## Correr en local

```bash
# Standalone (requiere Redis y las credenciales de Kafka en .env)
uvicorn app.main:app --reload --port 8002

# Como parte del stack completo (recomendado)
cd ../infra-cinema
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up --build inventory-service
```

Ver `../IMPLEMENTATION-GUIDE.md` para el procedimiento completo de arranque del proyecto (Kafka, bases de datos, stack local).

---

**Nota:** `requirements.txt` incluye `alembic` y `psycopg2-binary`, que este servicio no usa (no tiene base de datos SQL). Parecen arrastrados de otro `requirements.txt` del monorepo — no afectan el funcionamiento, pero son dependencias muertas.

## Flujo de trabajo: Gitflow

| Rama        | Sale de   | Entra a (vía PR)         | Método en GitHub | Para |
| ----------- | --------- | ------------------------ | ---------------- | ---- |
| `main`      | —         | —                        | —                | Lo que está en producción. Cada merge es una versión. |
| `develop`   | `main`    | —                        | —                | Integración de lo próximo a publicar. Rama por defecto. |
| `feature/*` | `develop` | `develop`                | **Squash**       | Una funcionalidad o cambio: `feature/mi-cambio`. |
| `release/*` | `develop` | `main` y luego `develop` | **Merge** a `main`; **Squash** a `develop` | Preparar una versión: `release/1.0.0`. Solo ajustes finales. |
| `hotfix/*`  | `main`    | `main` y luego `develop` | **Merge** a `main`; **Squash** a `develop` | Corrección urgente en producción. |

- **Nadie hace push directo** a `main` ni a `develop`: todo entra por pull request, con los checks de CI en verde.
- **En `develop` se usa squash:** cada feature queda como un solo commit con el título del PR.
- **En `main` se usa merge commit:** cada release o hotfix queda visible como una unidad.
- **Todavía no hay releases:** la app no está completa, así que `main` se queda como está hasta el
  primer `release/*`. Desde entonces, cada versión se etiqueta en `main` (`git tag -a v1.0.0`) con
  [versionado semántico](https://semver.org/lang/es/).

```bash
git switch develop && git pull
git switch -c feature/mi-cambio
# ...commits...
git push -u origin feature/mi-cambio   # abrir PR hacia develop → Squash and merge
```

## CI/CD

GitHub Actions (`.github/workflows/`) corre en cada PR hacia `main` o `develop`. Los rulesets exigen
estos checks; si se renombra un job, hay que actualizar `.github/rulesets/*.json`.

| Check | Qué revisa |
| ----- | ---------- |
| `Lint` | Ruff con las reglas de `ruff.toml`. |
| `Calidad y build` | Instala las dependencias, compila todo el código y carga la app con configuración falsa (sin base de datos ni Kafka). |
| `Imagen Docker` | Construye la imagen y comprueba que la app carga dentro de ella, sin red. |

Con cada push a `develop` o `main` (es decir, al fusionar un PR), y solo si pasaron los checks, se
publica en Docker Hub **la misma imagen que se probó** (no se reconstruye):

- `develop` → `<usuario>/inventory-service-cinema:develop` y `:<sha>`
- `main` → `<usuario>/inventory-service-cinema:latest` y `:<sha>`

El flujo no despliega en ningún servicio (tampoco en Render): solo publica la imagen.

### Configuración en GitHub (una vez)

- **Rulesets:** `main` y `develop` se protegen importando `.github/rulesets/main.json` y
  `.github/rulesets/develop.json` en *Settings → Rules → Rulesets → Import a ruleset*. Exigen PR, los
  checks de la tabla de arriba, y no permiten borrar la rama ni forzar pushes. `main` solo acepta
  merge commit y `develop` solo squash.
- **Settings → General:** rama por defecto `develop`; permitir merge commits y squash (no rebase);
  activar *Automatically delete head branches*.
- **Secrets** (*Settings → Secrets and variables → Actions*): `DOCKER_USERNAME` y `DOCKER_TOKEN`
  (token de acceso de Docker Hub con permiso de escritura).
