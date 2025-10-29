# Tarea-2-Sistemas-Distribuidos

Repositorio completo de tarea 2 de sistemas distribuidos.

## Características técnicas

- **Creación:** Docker Compose para levantar todos los servicios.
- **Mensajería:** Redpanda (Kafka API compatible) como broker de mensajes.
- **Procesamiento de datos:** Apache Flink (JobManager y TaskManager) ejecutando un job en Python (`app.py`).
- **Persistencia:** Servicio BDD con SQLite para almacenamiento.
- **Generación y validación:** Servicios `generator` y `llm` para generación y validación de preguntas/respuestas usando LLM y Google API.
- **Retry:** Servicios `retry-overload` y `retry-quota` para manejo de reintentos.
- **Monitorización:** Kafdrop para visualizar topics y mensajes en Kafka.
- **Interconexión:** Los servicios se comunican mediante topics Kafka definidos en las variables de entorno.

## Servicios principales

- **kafka:** Broker Redpanda, expone el puerto 9092.
- **flink-jobmanager / flink-taskmanager:** Cluster Flink para procesamiento de streams.
- **generator:** Genera preguntas y respuestas, interactúa con el servicio BDD.
- **llm:** Valida y regenera preguntas usando un modelo LLM y la API de Google.
- **bdd:** Persistencia de datos en SQLite.
- **retry-overload / retry-quota:** Manejo de reintentos por sobrecarga y cuota.
- **kafdrop:** Interfaz web para monitorear Kafka (puerto 9000).

## Requisitos previos

- Docker y Docker Compose instalados.
- Variable de entorno `GOOGLE_API_KEY` configurada para el servicio `llm`.

## Cómo levantar los servicios

1. **Clona el repositorio:**
   ```bash
   git clone https://github.com/benjaminzunigapueller/Tarea-2-Sistemas-Distribuidos.git
   cd Tarea-2-Sistemas-Distribuidos
   ```

2. **Configura la variable de entorno:**
   ```bash
   export GOOGLE_API_KEY=<tu_api_key>
   ```

3. **(Opcional) Crea los topics necesarios en Kafka si no existen:**
   ```bash
   docker compose up -d kafka
   docker exec -it tarea-2-sistemas-distribuidos-kafka-1 rpk topic create questions.answers
   docker exec -it tarea-2-sistemas-distribuidos-kafka-1 rpk topic create questions.validated
   docker exec -it tarea-2-sistemas-distribuidos-kafka-1 rpk topic create questions.llm
   ```

4. **Levanta todos los servicios:**
   ```bash
   docker compose up --build --force-recreate
   ```

5. **Accede a Kafdrop para monitorear Kafka:**
   - [http://localhost:9000](http://localhost:9000)

6. **Accede a Flink JobManager UI:**
   - [http://localhost:8081](http://localhost:8081)

## Notas

- El job de Flink se ejecuta automáticamente al iniciar el JobManager.
- El servicio BDD expone la API en el puerto 8001 (internamente mapeado a 8000).
- Los servicios se comunican usando los siguientes topics:
  - `questions.answers` (input principal)
  - `questions.validated` (output validado)
  - `questions.llm` (output para regeneración)

## Estructura del proyecto

- `docker-compose.yml`: Orquestación de servicios.
- `flink_job/`: Código y Dockerfile para Flink.
- `generator/`: Servicio de generación.
- `llm/`: Servicio de validación/regeneración.
- `bdd/`: Servicio de persistencia.
- `retry/`: Servicios de reintentos.
- `app.py`: Job principal de Flink (ubicado en `flink_job/`).

## Troubleshooting

- Si Flink falla por `UnknownTopicOrPartitionException`, asegúrate de que los topics existen en Kafka.
- Revisa los logs de cada servicio con:
  ```bash
  docker compose logs <servicio>
  ```

---
