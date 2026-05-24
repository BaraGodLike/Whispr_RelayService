# Whispr Relay Service

[English version](README.md)

Whispr Relay Service — это внутренний backend доставки для хранения pending-сообщений.

Сервис принимает opaque encrypted payload от `Realtime`, надежно сохраняет его, публикует легковесное событие о постановке сообщения в очередь, отдает payload для online delivery, возвращает pending-сообщения при reconnect/resume и удаляет их после ACK.

Основное поведение:
- `Realtime` отправляет готовый message envelope как raw protobuf `bytes payload`;
- Relay сохраняет сообщение в Postgres как pending;
- Relay пишет outbox-событие в той же транзакции;
- фоновый outbox publisher публикует `message.enqueued` в Kafka;
- `Realtime` запрашивает payload по `msg_id` для online delivery;
- `Realtime` запрашивает pending-сообщения по mailbox при resume/reconnect;
- ACK удаляет pending-сообщение, удаляет уже опубликованные outbox-строки для того же `msg_id` и очищает кэш best effort.

Relay не делает:
- аутентификацию;
- проверку realtime challenge;
- разрешение mailbox owner;
- tracking соединений;
- push-уведомления;
- расшифровку payload.

## Возможности

- Идемпотентный `EnqueueMessage` по `msg_id`.
- Детект дублей через byte-for-byte сравнение payload.
- Durable storage pending-сообщений в Postgres.
- Transactional outbox для публикации событий в Kafka.
- Shared Redis cache и локальный in-memory cache для быстрых чтений payload.
- Отдельный процесс `OutboxWorker` для фоновой публикации outbox.
- Отдельный процесс `CleanupWorker` для retention cleanup.
- Стандартный gRPC health service.
- Структурированные JSON-логи с metadata о сервисе и инстансе.

## Структура solution

- `RelayService` - gRPC host, gRPC health service, JSON formatter логов, API endpoints.
- `Application` - прикладная логика и orchestration contracts.
- `Domain` - core entities и константы.
- `Infrastructure.Storage` - Postgres persistence, outbox leasing, health check.
- `Infrastructure.Caching` - Redis и in-memory caching payload.
- `Infrastructure.Messaging` - Kafka producer и Kafka connectivity checks, которые используются `OutboxWorker`.
- `Worker` - общие компоненты фонового outbox publisher.
- `OutboxWorker` - отдельный host-процесс для публикации outbox-событий.
- `CleanupWorker` - отдельный retention worker для удаления просроченных pending и outbox-записей.
- `Migrator` - миграции БД на FluentMigrator.
- `UnitTests` - unit tests для domain и application слоев.

## Модель хранения

Postgres является source of truth.

Таблицы:
- `pending_messages`
- `outbox_events`

`pending_messages` хранит:
- `msg_id`
- `dest_mailbox`
- `payload`
- `created_at`

`outbox_events` хранит:
- `event_id`
- `event_type`
- `msg_id`
- `dest_mailbox`
- `created_at`
- `published`
- `published_at`

Кэширование:
- локальный in-memory cache: `msg_id -> payload`
- Redis cache: `msg:{msg_id} -> payload`

Цепочка чтения для `GetMessage`:
1. in-memory cache
2. Redis
3. Postgres

## gRPC API

Локальный gRPC endpoint по умолчанию в Docker Compose: `http://localhost:${RELAY_GRPC_PORT}`, по умолчанию `8080` из `.env`. Сервис читает порт прослушивания из `Grpc:Port`, который прокидывается через `Grpc__Port`.

Proto file: [RelayService/Protos/relay_service.proto](RelayService/Protos/relay_service.proto)

Сервис:

```proto
service Relay {
  rpc EnqueueMessage (EnqueueMessageRequest) returns (EnqueueMessageResponse);
  rpc GetMessage (GetMessageRequest) returns (GetMessageResponse);
  rpc GetPendingMessages (GetPendingMessagesRequest) returns (GetPendingMessagesResponse);
  rpc AckMessage (AckMessageRequest) returns (AckMessageResponse);
  rpc AckMessagesBatch (AckMessagesBatchRequest) returns (AckMessagesBatchResponse);
}
```

### EnqueueMessage

Принимает готовый opaque payload от `Realtime`.

Пример запроса:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555",
  "destMailbox": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  "payload": "base64-bytes"
}
```

Пример ответа:

```json
{
  "accepted": true
}
```

Поведение:
- вставляет запись в `pending_messages`;
- вставляет `message.enqueued` в `outbox_events`;
- прогревает Redis и memory cache best effort;
- если тот же `msg_id` уже существует с тем же payload, возвращает `OK`;
- если тот же `msg_id` существует с другим payload, возвращает `ALREADY_EXISTS`.

Типичные ошибки:
- `INVALID_ARGUMENT` - невалидный `msg_id`, невалидный `dest_mailbox`, пустой payload или payload больше `256 KB`.
- `ALREADY_EXISTS` - `msg_id` уже существует с другим payload.
- `UNAVAILABLE` - временная проблема зависимости, surfaced как database exception.
- `INTERNAL` - неожиданная внутренняя ошибка.

### GetMessage

Возвращает raw payload одного сообщения.

Пример запроса:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555"
}
```

Пример ответа:

```json
{
  "payload": "base64-bytes"
}
```

Типичные ошибки:
- `INVALID_ARGUMENT` - невалидный `msg_id`.
- `NOT_FOUND` - сообщение не найдено.
- `UNAVAILABLE` - временная проблема зависимости, surfaced как database exception.
- `INTERNAL` - неожиданная внутренняя ошибка.

### GetPendingMessages

Возвращает pending-сообщения, отсортированные по `created_at, msg_id`.

Пример запроса:

```json
{
  "mailboxIds": [
    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  ],
  "limit": 100
}
```

Пример ответа:

```json
{
  "messages": [
    {
      "msgId": "11111111-2222-3333-4444-555555555555",
      "destMailbox": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      "payload": "base64-bytes"
    }
  ],
  "hasMore": false
}
```

Правила:
- `limit = 0` означает default `500`;
- допустимый диапазон `1..500`;
- максимум `7` mailbox id в одном запросе;
- дубликаты mailbox id дедуплицируются перед SQL query;
- пагинация ACK-driven, без cursor.

Типичные ошибки:
- `INVALID_ARGUMENT` - пустой список mailbox, невалидный mailbox id, больше `7` mailbox id или невалидный `limit`.

### AckMessage

Удаляет одно pending-сообщение, удаляет уже опубликованные outbox-строки для того же `msg_id` и чистит кэш best effort.

Пример запроса:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555"
}
```

Пример ответа:

```json
{
  "success": true
}
```

Типичные ошибки:
- `INVALID_ARGUMENT` - невалидный `msg_id`.
- `INTERNAL` - неожиданная внутренняя ошибка.

Примечание:
- если сообщение уже удалено или не найдено, все равно возвращается `OK`.
- на ACK удаляются только outbox-строки с `published = true`; непубликованные outbox не трогаются, чтобы не ломать гарантию publisher-а.

### AckMessagesBatch

Удаляет несколько pending-сообщений одним вызовом, удаляет уже опубликованные outbox-строки для того же набора `msg_id` и чистит кэш best effort.

Пример запроса:

```json
{
  "msgIds": [
    "11111111-2222-3333-4444-555555555555",
    "66666666-7777-8888-9999-000000000000"
  ]
}
```

Пример ответа:

```json
{
  "ackedCount": 2
}
```

Правила:
- максимум `500` id в одном запросе;
- дубликаты id внутри одного batch дедуплицируются;
- `ackedCount` — это количество уникальных id, которые реально существовали и были удалены.

Типичные ошибки:
- `INVALID_ARGUMENT` - пустой batch, невалидный `msg_id` или больше `500` id.

## Публикация событий

Relay публикует в Kafka только легковесное событие, а не сам message payload.

Контракт события: [RelayService/Protos/relay_events.proto](RelayService/Protos/relay_events.proto)

Topic:
- `message.enqueued`

Payload события:
- `msg_id`
- `dest_mailbox`

Примечания:
- Kafka key = `dest_mailbox`;
- outbox publisher работает в отдельном процессе `outboxworker`;
- дубли событий в Kafka возможны, если падение случилось после publish и до outbox commit, поэтому downstream consumers должны быть идемпотентны.

## Retention cleanup

`CleanupWorker` — это отдельный one-shot процесс, рассчитанный на singleton scheduled execution, например через Kubernetes `CronJob`.

Логика cleanup:
- ACK — основной быстрый путь очистки для доставленных сообщений: он удаляет `pending_messages` и уже опубликованные outbox-строки для того же `msg_id`.
- `CleanupWorker` батчами удаляет старые `published` outbox-строки по `created_at`.
- `CleanupWorker` также батчами удаляет старые `pending_messages` и связанные с ними outbox-строки по `created_at`.
- retention по умолчанию — `7 days`;
- batch size по умолчанию — `1000`;
- cleanup крутится до тех пор, пока не закончатся и старые `published` outbox, и старые `pending_messages`.

Примечания по retention:
- старые published outbox удаляются по `created_at`, а не по `published_at`;
- cleanup не трогает кэши, они должны истекать по TTL;
- cleanup — это safety net и механизм контроля роста хранилища, а основная happy-path очистка идет через ACK.

## gRPC health

Сервис публикует стандартный gRPC health service `grpc.health.v1.Health`.

Текущий health-check `RelayService` определяется по:
- Postgres

Redis не входит в обязательный health-gate, потому что Relay умеет читать напрямую из Postgres.
Kafka намеренно больше не входит в health `RelayService` и целиком относится к зоне ответственности `OutboxWorker`.

## Логирование

- `RelayService` пишет структурированные single-line JSON-логи в stdout.
- Каждая .NET log entry содержит:
  - `Timestamp`
  - `Service`
  - `Instance`
  - `EventId`
  - `LogLevel`
  - `Category`
  - `Message`
  - `State`
- Service metadata берется из:
  - `ServiceMetadata:ServiceName`
  - `ServiceMetadata:InstanceId`
- Если `InstanceId` пустой, используется hostname контейнера/машины.
- Логи по design intent не должны содержать mailbox id, message id, payload bytes и другие user-identifying данные.
- Можно логировать sanitised technical metadata, например `ExceptionType` и `ExceptionMessage`.

Текущие правила уровней:
- `Default = Warning`
- `Startup = Information`
- `Application = Warning`
- `Infrastructure.Storage = Warning`
- `Infrastructure.Caching = Warning`
- `Infrastructure.Messaging = Warning`
- `Microsoft.Hosting.Lifetime = Warning`
- `Microsoft = Warning`

## Запуск через Docker Compose

Docker Compose поднимает:
- Postgres
- Redis
- RedisInsight
- Kafka
- Kafka UI
- `Migrator`
- `relayservice`
- `outboxworker`

Важно:
- `relayservice` поднимает только gRPC API и health endpoints;
- `outboxworker` запускает фонового outbox publisher отдельным контейнером;
- `CleanupWorker` не входит в стандартный Compose stack, потому что рассчитан на singleton scheduled execution в Kubernetes.
- `relayservice` слушает порт из `Grpc__Port`, который мапится из `RELAY_GRPC_PORT`.

### 1. Подготовь `.env`

Скопируй [.env.example](.env.example) в `.env` и заполни нужные значения:
- `ASPNETCORE_ENVIRONMENT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`
- `REDIS_PORT`
- `REDIS_INSIGHT_PORT`
- `KAFKA_PORT`
- `KAFKA_UI_PORT`
- `KAFKA_TOPIC`
- `OUTBOX_WORKER_KAFKA_CLIENT_ID`
- `RELAY_GRPC_PORT`

### 2. Подними стек

```powershell
docker compose up --build
```

Локальные endpoint по умолчанию после старта:
- gRPC: `http://localhost:${RELAY_GRPC_PORT}`, по умолчанию `8080`
- RedisInsight: `http://localhost:${REDIS_INSIGHT_PORT}`, по умолчанию `5540`
- Kafka UI: `http://localhost:${KAFKA_UI_PORT}`, по умолчанию `8081`

### 3. Перезапусти только `relayservice`

Если зависимости уже работают:

```powershell
docker compose up --no-deps --build relayservice
```

### 4. Перезапусти только `outboxworker`

Если зависимости уже работают:

```powershell
docker compose up --no-deps --build outboxworker
```

## Локальный запуск без Docker

### RelayService

```powershell
dotnet run --project RelayService
```

Локально gRPC порт можно переопределить так:

```powershell
$env:Grpc__Port="8080"
dotnet run --project RelayService
```

Локальные launch settings лежат в [RelayService/Properties/launchSettings.json](RelayService/Properties/launchSettings.json).

### Migrator

```powershell
dotnet run --project Migrator
```

### OutboxWorker

```powershell
dotnet run --project OutboxWorker
```

### CleanupWorker

```powershell
dotnet run --project CleanupWorker
```

## Тестирование API

gRPC API удобно тестировать через Postman или `grpcurl`.

Для Postman:
1. Создай `gRPC Request`.
2. Укажи сервер `http://localhost:${RELAY_GRPC_PORT}`.
3. Импортируй [RelayService/Protos/relay_service.proto](RelayService/Protos/relay_service.proto).
4. Выбери нужный метод `Relay`.

## Технологический стек

- .NET 10
- ASP.NET Core
- gRPC
- PostgreSQL
- Redis
- Kafka
- Dapper
- FluentMigrator
- MSTest
