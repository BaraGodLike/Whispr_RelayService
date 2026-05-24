# Whispr Relay Service

[Русская версия](README.ru.md)

Whispr Relay Service is an internal delivery backend for pending message storage.

It accepts opaque encrypted message payloads from `Realtime`, stores them durably, publishes a lightweight enqueue event, serves message payloads for online delivery, returns pending messages on reconnect, and removes messages after ACK.

Core behavior:
- `Realtime` sends a ready-made message envelope as raw protobuf `bytes payload`;
- Relay stores the message in Postgres as pending data;
- Relay writes an outbox record in the same transaction;
- a background outbox publisher publishes `message.enqueued` to Kafka;
- `Realtime` fetches payloads by `msg_id` for online delivery;
- `Realtime` fetches pending messages by mailbox on resume/reconnect;
- ACK removes the pending message, removes already published outbox rows for the same `msg_id`, and evicts cache entries.

Relay does not do:
- authentication;
- realtime challenge verification;
- mailbox ownership resolution;
- connection tracking;
- push notifications;
- payload decryption.

## Features

- Idempotent `EnqueueMessage` by `msg_id`.
- Duplicate detection with byte-for-byte payload comparison.
- Durable pending storage in Postgres.
- Transactional outbox for Kafka event publication.
- Shared Redis cache plus local in-memory cache for hot payload reads.
- Dedicated `OutboxWorker` process for background outbox publishing.
- Dedicated `CleanupWorker` process for retention cleanup.
- Standard gRPC health service.
- Structured single-line JSON logs with service and instance metadata.

## Solution structure

- `RelayService` - gRPC host, gRPC health service, logging formatter, API endpoints.
- `Application` - application logic and orchestration contracts.
- `Domain` - core entities and constants.
- `Infrastructure.Storage` - Postgres persistence, outbox leasing, health check.
- `Infrastructure.Caching` - Redis and in-memory payload caching.
- `Infrastructure.Messaging` - Kafka producer and Kafka health check.
- `Worker` - shared background worker components for outbox publishing.
- `OutboxWorker` - dedicated host process for outbox event publication.
- `CleanupWorker` - dedicated retention job for expired pending and outbox records.
- `Migrator` - FluentMigrator database migrations.
- `UnitTests` - domain and application unit tests.

## Storage model

Relay uses Postgres as the source of truth.

Tables:
- `pending_messages`
- `outbox_events`

`pending_messages` stores:
- `msg_id`
- `dest_mailbox`
- `payload`
- `created_at`

`outbox_events` stores:
- `event_id`
- `event_type`
- `msg_id`
- `dest_mailbox`
- `created_at`
- `published`
- `published_at`

Caching:
- local in-memory cache: `msg_id -> payload`
- Redis cache: `msg:{msg_id} -> payload`

Read chain for `GetMessage`:
1. in-memory cache
2. Redis
3. Postgres

## gRPC API

Default local gRPC endpoint in Docker Compose: `http://localhost:${RELAY_GRPC_PORT}` with `8080` as the default from `.env`. The service reads the listen port from `Grpc:Port`, which is wired from `Grpc__Port`.

Proto file: [RelayService/Protos/relay_service.proto](RelayService/Protos/relay_service.proto)

Service:

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

Accepts a ready-made opaque payload from `Realtime`.

Request example:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555",
  "destMailbox": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  "payload": "base64-bytes"
}
```

Response example:

```json
{
  "accepted": true
}
```

Behavior:
- inserts into `pending_messages`;
- inserts `message.enqueued` into `outbox_events`;
- warms Redis and memory cache on best effort;
- if the same `msg_id` already exists with the same payload, returns `OK`;
- if the same `msg_id` exists with a different payload, returns `ALREADY_EXISTS`.

Typical errors:
- `INVALID_ARGUMENT` - invalid `msg_id`, invalid `dest_mailbox`, empty payload, or payload larger than `256 KB`.
- `ALREADY_EXISTS` - `msg_id` exists with a different payload.
- `UNAVAILABLE` - dependency availability issue surfaced as a database exception.
- `INTERNAL` - unexpected internal failure.

### GetMessage

Returns the raw payload for one message.

Request example:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555"
}
```

Response example:

```json
{
  "payload": "base64-bytes"
}
```

Typical errors:
- `INVALID_ARGUMENT` - invalid `msg_id`.
- `NOT_FOUND` - message does not exist.
- `UNAVAILABLE` - dependency availability issue surfaced as a database exception.
- `INTERNAL` - unexpected internal failure.

### GetPendingMessages

Returns pending messages ordered by `created_at, msg_id`.

Request example:

```json
{
  "mailboxIds": [
    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  ],
  "limit": 100
}
```

Response example:

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

Rules:
- `limit = 0` means use the default `500`;
- valid range is `1..500`;
- at most `7` mailbox ids per request;
- duplicate mailbox ids are deduplicated before querying;
- pagination is ACK-driven, not cursor-based.

Typical errors:
- `INVALID_ARGUMENT` - empty mailbox list, invalid mailbox id, mailbox count above `7`, or invalid `limit`.

### AckMessage

Deletes one pending message, removes already published outbox rows for the same `msg_id`, and evicts caches on best effort.

Request example:

```json
{
  "msgId": "11111111-2222-3333-4444-555555555555"
}
```

Response example:

```json
{
  "success": true
}
```

Typical errors:
- `INVALID_ARGUMENT` - invalid `msg_id`.
- `INTERNAL` - unexpected internal failure.

Note:
- already deleted or missing messages still return `OK`.
- only `published = true` outbox rows are removed on ACK; unpublished outbox rows are preserved so the publisher guarantee is not broken.

### AckMessagesBatch

Deletes multiple pending messages in one call, removes already published outbox rows for the same `msg_id` set, and evicts caches on best effort.

Request example:

```json
{
  "msgIds": [
    "11111111-2222-3333-4444-555555555555",
    "66666666-7777-8888-9999-000000000000"
  ]
}
```

Response example:

```json
{
  "ackedCount": 2
}
```

Rules:
- maximum `500` ids per request;
- duplicate ids inside the same batch are deduplicated;
- `ackedCount` is the number of unique ids that actually existed and were deleted.

Typical errors:
- `INVALID_ARGUMENT` - empty batch, invalid `msg_id`, or more than `500` ids.

## Event publication

Relay publishes only a lightweight event to Kafka, not the message payload.

Event contract: [RelayService/Protos/relay_events.proto](RelayService/Protos/relay_events.proto)

Topic:
- `message.enqueued`

Event payload:
- `msg_id`
- `dest_mailbox`

Notes:
- Kafka message key is `dest_mailbox`;
- outbox publisher runs in the separate `outboxworker` process;
- duplicate Kafka events are possible if a crash happens after publish and before outbox commit, so downstream consumers must be idempotent.

## Retention cleanup

`CleanupWorker` is a separate one-shot process intended for singleton scheduled execution, for example through a Kubernetes `CronJob`.

Cleanup behavior:
- ACK is the primary fast-path cleanup for delivered messages and removes `pending_messages` plus already published outbox rows for the same `msg_id`.
- `CleanupWorker` removes old `published` outbox rows by `created_at` in batches.
- `CleanupWorker` also removes old `pending_messages` and their related outbox rows by `created_at` in batches.
- default retention is `7 days`;
- default cleanup batch size is `1000`;
- cleanup loops until both old `published` outbox rows and old `pending_messages` are exhausted.

Retention notes:
- old published outbox rows are cleaned by `created_at`, not by `published_at`;
- cache entries are not touched by cleanup and are expected to expire by TTL;
- cleanup exists as a safety net and storage control mechanism, while ACK remains the normal happy-path cleanup.

## gRPC health

The service exposes the standard gRPC health service `grpc.health.v1.Health`.

The current implementation marks health using:
- Postgres
- Kafka

Redis is optional for liveness because Relay can fall back to Postgres reads.

## Logging

- `RelayService` writes structured single-line JSON logs to stdout.
- Every .NET log entry includes:
  - `Timestamp`
  - `Service`
  - `Instance`
  - `EventId`
  - `LogLevel`
  - `Category`
  - `Message`
  - `State`
- Default service metadata comes from:
  - `ServiceMetadata:ServiceName`
  - `ServiceMetadata:InstanceId`
- If `InstanceId` is empty, the host/container name is used.
- Logs do not include mailbox ids, message ids, payload bytes, or user-identifying data by design intent.
- Sanitized technical metadata such as `ExceptionType` and `ExceptionMessage` may be logged.

Current log level rules:
- `Default = Warning`
- `Startup = Information`
- `Application = Warning`
- `Infrastructure.Storage = Warning`
- `Infrastructure.Caching = Warning`
- `Infrastructure.Messaging = Warning`
- `Microsoft.Hosting.Lifetime = Warning`
- `Microsoft = Warning`

## Running with Docker Compose

The Docker Compose setup includes:
- Postgres
- Redis
- RedisInsight
- Kafka
- Kafka UI
- `Migrator`
- `relayservice`
- `outboxworker`

Important:
- `relayservice` exposes only the gRPC API and health endpoints;
- `outboxworker` runs the background outbox publisher in a separate container;
- `CleanupWorker` is not part of the default Compose stack because it is intended for singleton scheduled execution in Kubernetes.
- `relayservice` listens on `Grpc__Port`, which is mapped from `RELAY_GRPC_PORT`.

### 1. Prepare `.env`

Copy [.env.example](.env.example) to `.env` and fill the required values:
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
- `RELAYSERVICE_KAFKA_CLIENT_ID`
- `OUTBOX_WORKER_KAFKA_CLIENT_ID`
- `RELAY_GRPC_PORT`

### 2. Start the stack

```powershell
docker compose up --build
```

Default local endpoints after startup:
- gRPC: `http://localhost:${RELAY_GRPC_PORT}` with `8080` as the default
- RedisInsight: `http://localhost:${REDIS_INSIGHT_PORT}` with `5540` as the default
- Kafka UI: `http://localhost:${KAFKA_UI_PORT}` with `8081` as the default

### 3. Restart only `relayservice`

If dependencies are already running:

```powershell
docker compose up --no-deps --build relayservice
```

### 4. Restart only `outboxworker`

If dependencies are already running:

```powershell
docker compose up --no-deps --build outboxworker
```

## Running locally without Docker

### RelayService

```powershell
dotnet run --project RelayService
```

You can override the gRPC port locally with:

```powershell
$env:Grpc__Port="8080"
dotnet run --project RelayService
```

Local launch settings are defined in [RelayService/Properties/launchSettings.json](RelayService/Properties/launchSettings.json).

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

## Testing the API

You can test the gRPC API with Postman or `grpcurl`.

For Postman:
1. Create a `gRPC Request`.
2. Set the server to `http://localhost:${RELAY_GRPC_PORT}`.
3. Import [RelayService/Protos/relay_service.proto](RelayService/Protos/relay_service.proto).
4. Choose a `Relay` method.

## Technology stack

- .NET 10
- ASP.NET Core
- gRPC
- PostgreSQL
- Redis
- Kafka
- Dapper
- FluentMigrator
- MSTest
