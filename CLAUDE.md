# CLAUDE.md — Alert Streaming Showcase

## Project Purpose

A real-time alert streaming demo that processes IoT home security and financial fraud events through an AI inference pipeline. Raw activity events are published to RabbitMQ Streams, an AI processor (Spring AI + Ollama) detects alerts from batches of activities, and a web UI displays filtered alerts via Server-Sent Events.

**Demo scenarios:** IoT home security (`--spring.profiles.active=iot`) and payment fraud detection (`--spring.profiles.active=finance`).

---

## Architecture

```
[generator-supplier-source]  [http-source]
        ↓                         ↓
    activities.activity (RabbitMQ Stream)
                ↓
    [alert-ai-processor]
        - Batches activities (size OR time trigger)
        - Calls Ollama via Spring AI
        - Parses JSON response into Alert records
        - Sets account/level message headers
                ↓
    alerts.alert (RabbitMQ Stream)
                ↓
    [alert-app]  (SQL filter at broker level)
        - In-memory repository
        - REST API (SSE): /alert/alerts, /activities/activity
        - Web UI: FreeMarker templates
```

**Key architecture decisions:**

- **RabbitMQ Streams (not queues):** Enables SQL filtering at the broker, so multiple consumers can subscribe to the same stream with different filters without message duplication. Also supports offset replay (`FIRST`, `LAST`, specific offset).
- **SQL filtering:** Configured in `alert-app` via `--stream.filter.sql`. Applied in `RabbitAmqpConsumerConfig` using `.filter().sql(...)`. Example: `account = 'imani' AND level IN ('critical', 'high')`.
- **Batching before AI inference:** `InMemoryAlDetectorService` accumulates activities and calls the model when `alerts.inference.batch` is exceeded OR `alerts.inference.delayMs` has elapsed. Prevents excessive Ollama requests.
- **Profile-per-domain AI config:** Each domain (IoT, finance) has its own `@Configuration @Profile(...)` class that defines the Ollama prompt and registers the `AlertsModelInference` bean. Switching domains only requires changing the active profile.
- **Spring Cloud Function:** All message processing is expressed as `Function<Input, Output>`, `Consumer<T>`, or `Supplier<T>` beans. Spring Cloud Stream maps these to RabbitMQ bindings via `spring.cloud.function.definition` and `spring.cloud.stream.function.bindings`.

---

## Module Map

| Path | Purpose |
|------|---------|
| `components/domains` | Shared `Alert`, `Activity`, `AlertList` records |
| `applications/alert-app` | Consumer web app — REST API, SSE, FreeMarker UI |
| `applications/alert-ai-processor` | Spring AI + Ollama inference pipeline |
| `applications/generator-supplier-source` | Test data generator from CSV |
| `applications/http-source` | HTTP/MQTT ingestion bridge |
| `deployment/local/containers/rabbit.sh` | Start RabbitMQ with streams/MQTT plugins |
| `docs/demo/local/` | Step-by-step demo instructions |

---

## Coding Conventions

**Domain objects are Java records with Lombok `@Builder`:**
```java
@Builder
public record Alert(String id, String account, String level, String time, String event) {}
```
Do not use `@Data` or mutable classes for domain objects.

**Constructor injection, not field injection:** Use `@RequiredArgsConstructor` or explicit constructors. Never `@Autowired` on a field.

**Logging:** `@Slf4j` on every class that logs. Use `log.debug(...)` for trace-level diagnostic info.

**Configuration properties:** Use `@ConfigurationProperties(prefix="...")` on `@Configuration` classes (not standalone `@ConfigurationPropertiesBean`). See `AlertsInferenceProperties` for the pattern.

**Functional Spring Cloud Stream:** Register message processing as `Function`, `Consumer`, or `Supplier` beans. Wire to channels with `spring.cloud.stream.function.bindings`, not `@StreamListener`.

**Package naming:** `showcase.<domain>.*` — e.g., `showcase.alarm.*`, `showcase.streaming.domains`, `showcase.streaming.source.generator.*`.

**Tests:** JUnit 5 + Mockito + AssertJ. Use `spring-cloud-stream-test-binder` for stream integration tests. Test class names follow `<Subject>Test` convention.

---

## Build and Run

**Build all modules (skip tests):**
```bash
./mvnw clean package -DskipTests
```

**Start RabbitMQ:**
```bash
./deployment/local/containers/rabbit.sh
# Management UI: http://localhost:15672 (guest/guest)
# AMQP: 5672, Streams: 5552, MQTT: 1883
```

**Run each application (in order):**
```bash
# 1. AI processor (pick a profile)
java -jar applications/alert-ai-processor/target/alert-ai-processor-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=iot

# 2. Alert web app
java -jar applications/alert-app/target/alert-app-0.0.1-SNAPSHOT.jar \
  --server.port=8080 \
  --stream.filter.sql="account = 'josiah' AND level IN ('critical', 'high')" \
  --stream.activity.filter.value=josiah

# 3. Generator (test data)
java -jar applications/generator-supplier-source/target/generator-supplier-source-0.0.1-SNAPSHOT.jar

# 4. HTTP source (optional, for MQTT/HTTP ingestion)
java -jar applications/http-source/target/http-source-0.0.1-SNAPSHOT.jar \
  --server.port=8383 --spring.profiles.active=mqtt
```

Ollama must be running separately (`ollama serve`) with the model configured in `alert-ai-processor/src/main/resources/application.yml`.

---

## Common Tasks

### Adding a new alert domain (e.g., healthcare)

1. **Add profile config in `alert-ai-processor`** — create `ServiceHealthcareConfig.java` following the pattern of `ServiceIotHomeSecurityConfig`. Define a prompt and return an `AlertsModelInference` bean annotated `@Profile("healthcare")`.
2. **Update generator CSV** if using `generator-supplier-source`, or send events via HTTP source.
3. **Run with** `--spring.profiles.active=healthcare`.
4. **Test** by mocking `ChatClient` and verifying the `AlertsModelInference` functional interface returns correctly parsed `AlertList`.

### Changing the AI model

Edit `applications/alert-ai-processor/src/main/resources/application.yml`:
```yaml
spring.ai.ollama.chat.options:
  model: llama3:latest   # change here
  format: json
```
Increase `ai.timeouts.seconds.read` if the model is slower. Pull the model in Ollama first: `ollama pull llama3:latest`.

### Tuning batch inference

In `alert-ai-processor/application.yml`:
```yaml
alerts.inference:
  batch: 5        # trigger when N activities accumulated
  delayMs: 8000   # OR trigger after N ms
```

### Modifying SQL stream filters

Pass via command line to `alert-app`:
```bash
--stream.filter.sql="account = 'alice' AND level = 'critical'"
--stream.activity.filter.value=alice
```
The `account` and `level` values must match headers set by `alert-ai-processor`. Header names are configured in `alerts.inference.header.names` in its `application.yml`.

### Debugging message routing

1. **Enable debug logging** in the relevant application's `application.yml`:
   ```yaml
   logging.level:
     org.springframework.cloud.stream: DEBUG
     org.springframework.amqp: DEBUG
     showcase: DEBUG
   ```
2. **RabbitMQ Management UI** at `http://localhost:15672` — check stream consumers, message rates, and queue depths.
3. **Verify headers** — the SQL filter depends on `account` and `level` headers being present on published messages. If alerts aren't appearing, confirm the AI processor is setting them in `AlertAiProcessorFunction`.
4. **SQL filter syntax** — test filter expressions directly in the RabbitMQ Management UI stream filter editor before wiring into config.
5. **Publish a test message** via the HTTP source:
   ```bash
   curl -X POST "http://localhost:8383/publisher?topic=activities.activity" \
     -H "account: test" -H "Content-Type: application/json" \
     -d '{"id":"1","account":"test","icon":"fa-shield","time":"10:00AM","activity":"Test event"}'
   ```

### Adding a REST endpoint to `alert-app`

Follow the existing controller pattern in `AlertController` or `ActivityController`: inject the repository/consumer, return `SseEmitter` for streaming endpoints or `ResponseEntity` for standard REST. Register SSE endpoints with `@GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)`.

---

## Configuration Reference

Key properties across applications — all overridable via command-line args:

| Property | Application | Default | Purpose |
|----------|-------------|---------|---------|
| `stream.filter.sql` | alert-app | (account filter) | RabbitMQ SQL filter for alerts |
| `stream.activity.filter.value` | alert-app | josiah | Account value for activity filter |
| `stream.filter.offset` | alert-app | FIRST | Stream start offset |
| `alert.refresh.rateSeconds` | alert-app | 1 | SSE push interval |
| `repository.list.batch.size` | alert-app | 10 | In-memory alert capacity |
| `alerts.inference.batch` | alert-ai-processor | 2 | Batch size before inference |
| `alerts.inference.delayMs` | alert-ai-processor | 5000 | Time-based inference trigger (ms) |
| `spring.ai.ollama.chat.options.model` | alert-ai-processor | gpt-oss:20b | Ollama model name |
| `generator.activities.csv.file` | generator-supplier-source | classpath:csv/activities.csv | Source CSV for test events |
