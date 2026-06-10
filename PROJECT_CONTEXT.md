# YAGPCC - Контекст проекта

## Обзор

**YAGPCC** (Yet Another Greenplum Command Center) — диагностический и мониторинговый агент для кластеров **Apache Greenplum / Cloudberry**. Собирает телеметрию запросов и сессий со всех хостов кластера (Master + Segments) через расширение `yagp-hooks-collector`, агрегирует данные и предоставляет gRPC API для real-time и исторических нужд.

- **Модуль**: `github.com/open-gpdb/yagpcc`
- **Язык**: Go 1.25
- **Лицензия**: Apache 2.0
- **Артефакт**: единый бинарник `yagpcc` + Debian-пакет
- **Роли**: `master` (на master-хосте) и `segment` (на каждом segment-хосте)

---

## Структура верхнего уровня

```
/ya/src/yagpcc/
├── api/                       # Protobuf-определения (gRPC API)
├── cmd/                       # Точки входа (бинарники)
├── internal/                  # Вся внутренняя логика
├── docs/                      # Документация (архитектура, API)
├── debian/                    # Debian-пакетирование
├── .github/workflows/         # CI: тесты + golangci-lint
├── Makefile                   # genproto, build, unittest, lint, package
├── go.mod / go.sum            # Зависимости
└── README.md                  # Обзор проекта
```

**Размер**: ~17 426 строк Go-кода (без тестов), 87 .go-файлов, 1 385 строк .proto.

---

## Команды (`cmd/`)

### Единственный бинарник: `yagpcc`

- **Точка входа**: `cmd/server/main.go` (57 строк)
- **Конфиг**: `yagpcc.yaml` (читается из CWD; флаг `-config-path <dir>`)
- **Шаблоны**: `yagpcc_master.yaml`, `yagpcc_segment.yaml`
- **SSL**: `allCAs.pem` для подключения к Greenplum
- Поведение: SIGINT → graceful shutdown; retry-loop при ошибке `app.Run()`

Сборка: `go build -o devbin/yagpcc ./cmd/server` (с PGO).

---

## API (`api/proto/`)

Всего 7 `.proto`-файлов в трёх группах. Кодогенерация — `make genproto` (protoc + protoc-gen-go + protoc-gen-go-grpc).

### Общие типы (`common/`)

| Файл | Размер | Содержимое |
|------|--------|------------|
| `yagpcc_metrics.proto` | 582 стр. | `QueryStatus` enum (SUBMIT/START/DONE/ERROR/CANCELLING/CANCELED/END), `QueryInfo`, `GPMetrics`, `SystemStat` (CPU, mem, I/O, 20+ полей), `QueryKey` (tmid/ssid/ccnt), `SegmentKey` (dbid/segindex), `PlanGenerator` (PLANNER/OPTIMIZER), `AggregatedMetrics` |
| `yagpcc_session.proto` | 202 стр. | `SessionKey`, `SessionState`, `SessionField` enum, `SessionFilter` (фильтрация по user/database/state/host) |

### Master API (`agent_master/`)

| Сервис | Файл | RPC |
|--------|------|-----|
| **GetGPInfo** | `yagpcc_get_service.proto` (423 стр.) | `GetGPSessions`, `GetGPQueries`, `GetGPQuery`, `GetGPSession`, `GetTotalSessionsStat` |
| **ActionService** | `yagpcc_action_service.proto` (59 стр.) | `MoveQueryToResourceGroup`, `TerminateQuery`, `TerminateSession`, `TerminateSessions` |

Ключевые типы: `RunningQueryType` (RQT_TOP/RQT_LAST), `StatKind` (SK_PRECISE/SK_AGGREGATED), `QueryStat`, `TotalQueryData`.

### Segment API (`agent_segment/`)

| Сервис | RPC | Использование |
|--------|-----|---------------|
| **SetQueryInfo** | `SetMetricQuery` | Приём от `yagp-hooks-collector` (через UDS) |
| **GetQueryInfo** | `GetMetricQueries` | Pull master-агентом (TCP gRPC) |
| **AgentControl** | `ResetStat`, `GetAgentInfo` | Управление агентом |

---

## Внутренняя структура (`internal/`) — 14 подпакетов

### 1. `app/` — главное приложение
- `app.go` — `AgentApp`: gRPC-сервер, регистрация сервисов, HTTP ping (health), Prometheus, pprof, signal handling, file lock (от двойного запуска)
- `pprof.go` — pprof endpoint

### 2. `baseapp/` — фреймворк приложения
Базовый класс: инструментация, HTTP-серверы, конфиг, signal handling.

### 3. `config/` — конфигурация
- `config.go` — `Config` struct (~73 поля): role, ports, UDS, master_connection, archiver, intervals, logging, Sentry, Prometheus
- `pgutil.go` — утилиты PostgreSQL connection
- Загрузка через `confita` (YAML + env)

### 4. `gp/` — взаимодействие с Greenplum
| Файл | Стр. | Назначение |
|------|-----|------------|
| `sessions.go` | 819 | `SessionsStorage` — активные сессии из `pg_stat_activity`, query status tracking, per-session metrics, RWMutex |
| `connector.go` | — | libpq-коннектор к Greenplum Master |
| `query.go` | — | Query-структуры |
| `actions.go` | — | terminate query/session, move to RSG |
| `cached_items.go` | — | кэш конфига и `pg_stat_activity` |
| `stat_activity/` | — | `lister.go` (получение `pg_stat_activity`) + `models.go` |
| `master_sentinel/` | — | проверка, что узел — master, а не standby |

### 5. `grpc/` — gRPC-сервисы (~3 870 строк)
| Файл | Стр. | Сервис |
|------|-----|--------|
| `get_master_info.go` | **2 793** ⭐ | `GetGPInfo` (master): фильтрация, сортировка, пагинация сессий/запросов |
| `actions.go` | 467 | `ActionService` (master) |
| `set_query_info.go` | — | `SetQueryInfo` (segment): приём от hooks-collector |
| `get_query_info.go` | — | `GetQueryInfo` (segment): для pull от master |
| `agent_control.go` | — | `AgentControl` |

### 6. `master/` — логика master-агента
| Файл | Стр. | Назначение |
|------|-----|------------|
| `background.go` | **624** ⭐ | `BackgroundStorage`, segment puller (периодический pull с segment-хостов), gRPC connection pooling, merge logic |
| `statwriter.go` | — | архивация статистики в JSON |
| `archiver.go` | — | историческая архивация |

### 7. `storage/` — in-memory хранилища
| Файл | Стр. | Структура |
|------|-----|-----------|
| `metrics_storage.go` | 474 | `RunningQueriesStorage` — текущие запросы с метриками, lifecycle |
| `util.go` | 375 | `QueryKey`, `SegmentKey`, helpers |
| `aggregated_storage.go` | 282 | `AggregatedStorage` — агрегаты по `(QueryID, PlanID, User, DB, RSG, time_range)` |
| `merger.go` | 240 | merge сегментных данных |
| `config_storage.go` | — | кэш `gp_segment_configuration` |

### 8. `uds/` — Unix Domain Socket
- `processor.go` — UDS-листенер для приёма от `yagp-hooks-collector`
- Формат: `[4-byte size][protobuf message]`

### 9. `metrics/` — Prometheus
- `metrics.go` — gauge/counter/histogram
- `gauge_time_histogram.go` — кастомные time-buckets для latency

### 10. `sink/clickhouse/` — опциональный sink в ClickHouse (master-only)
| Файл | Назначение |
|------|------------|
| `client.go` | `clickhouse-go/v2` клиент + TLS + Ping |
| `writer.go` | `ClickhouseWriter` orchestrator (lifecycle, Submit, FlushAggregates) |
| `tables.go` | `QueryEventWriter`, `AggregatedWriter`, `SessionSnapshotWriter` |
| `mapping.go` | конвертеры `pbm.TotalQueryData` / storage → CH-rows |
| `buffer.go` | thread-safe ring-buffer (`drop_oldest` \| `block`) |
| `migrations.go` | embedded `//go:embed migrations/*.sql` + ApplyMigrations / GetCurrentVersion |
| `schema.go` | VerifySchema, DumpSchema, DumpMigration |
| `metrics.go` | Prometheus `yagpcc_ch_*` коллекторы |
| `migrations/` | DDL миграции (`0001_init.up.sql`, `0001_init.down.sql`) |
| `integration_test.go` | testcontainers сценарий за `//go:build integration` |

Подключается из `internal/master/background.go` параллельно с `archiver.go` —
JSON-archive остаётся опциональным disaster fallback. Запуск регулируется
секцией `clickhouse:` в master-конфиге; opt-in через `enabled: true`.
Подробности — `docs/clickhouse-sink-architecture.md`.

### 11–14. `interfaces/`, `utils/`
- Сериализация (JSON/protobuf), zap-обёртка для логов, slice/time/pointer-хелперы.

---

## Архитектура и потоки данных

### Роли в кластере

```
[Segment Host N]
  Greenplum + yagp-hooks-collector  ──UDS gRPC──>  yagpcc (segment)
                                                       │
                                                       │ TCP gRPC
                                                       ▼
[Master Host]
  Greenplum + yagp-hooks-collector  ──UDS gRPC──>  yagpcc (master)
                                  ◄──libpq───────  (discovery, pg_stat_activity)
                                                       │
                                                       │ TCP gRPC
                                                       ▼
                                                [External consumers]
```

### Основной поток данных

```
[Query Execution в Greenplum]
        ↓ hooks (yagp-hooks-collector внутри backend-процесса)
[SetMetricQuery via UDS gRPC]
        ↓
[SetQueryInfoServer]
        ↓
[RunningQueriesStorage / SessionsStorage]  ← in-memory
        ↓ (master периодически pull-ит segments через GetMetricQueries)
[Merger → AggregatedStorage]
        ↓
[GetGPInfoServer / ActionServiceServer]
        ↓
[External consumers]
```

### Что **не** делает yagpcc
- Не контроллер жизненного цикла (не Kubernetes Operator)
- Не персистентное хранилище (in-memory only)
- Не расширение Greenplum (расширение — это `yagp-hooks-collector`)

---

## Ключевые зависимости (`go.mod`)

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `google.golang.org/grpc` | v1.79.1 | gRPC сервер/клиент |
| `google.golang.org/protobuf` | v1.36.11 | Protobuf runtime |
| `github.com/jackc/pgx/v4` | v4.18.3 | PostgreSQL/Greenplum драйвер |
| `github.com/jmoiron/sqlx` | v1.4.0 | SQL utilities |
| `github.com/heetch/confita` | v0.11.0 | Загрузка конфига (YAML+env) |
| `github.com/prometheus/client_golang` | v1.23.2 | Prometheus метрики |
| `go.uber.org/zap` | v1.27.1 | Логирование |
| `github.com/gofrs/flock` | v0.13.0 | Файловые блокировки |
| `github.com/onsi/ginkgo/v2` + `gomega` | — | BDD-тесты |

---

## Сборка

```bash
make genproto     # Генерация *.pb.go и *_grpc.pb.go из .proto
make build        # Бинарник devbin/yagpcc (с PGO)
make unittest     # Ginkgo --race ./...
make fmt          # gofmt
make lint         # golangci-lint
make package      # dpkg-buildpackage → .deb
```

### Целевые артефакты
1. **Go-бинарник**: `devbin/yagpcc`
2. **Debian-пакет**: `.deb` (Section: database, Architecture: any, format 3.0 native)

### CI (`.github/workflows/test.yaml`)
- Триггеры: push/PR на main
- Шаги: setup-go (1.25) → `go build` → `golangci-lint` (v2.6) → `make unittest` (`--race --github-output`)

---

## Конфигурация

### Master (`yagpcc_master.yaml`)
```yaml
role: master
listen_port: 1432
segment_pull_rate_sec: 3
segment_pull_threads: 2
master_connection:
  addrs: [host1:6432, host2:6432]
  sslmode: allow
  password: "..."
app:
  logging:
    level: debug
```

### Segment (`yagpcc_segment.yaml`)
```yaml
role: segment
listen_port: 1432
app:
  logging:
    level: debug
```

### Порты и протоколы
| Канал | Протокол | Кто слушает |
|-------|----------|-------------|
| UDS | gRPC | `yagpcc` принимает от `yagp-hooks-collector` |
| `listen_port` | TCP gRPC | segment ← master (pull); master ← external |
| `ping_port` | HTTP | health-check |
| `debug_port` | HTTP | pprof |
| Greenplum | libpq | master → GP master (discovery + `pg_stat_activity`) |

---

## Документация (`docs/`)

| Файл | Содержимое |
|------|------------|
| `architecture.md` / `architecture-ru.md` | High-level дизайн с Mermaid-диаграммой |
| `service-architecture.md` | Сервисы, роли, интерфейсы, data flows |
| `API.md` | gRPC API reference (GetGPInfo, ActionService) |
| `clickhouse-sink-architecture.md` | Архитектура опционального sink'а в ClickHouse (`internal/sink/clickhouse/`) |
| `vm-vs-ch.md` | VictoriaMetrics vs ClickHouse — разделение ролей в стенде |
| `real-time-stats-flow.md` / `historical-stats-flow.md` | заглушки (32 байта) |

---

## Ключевые файлы для изучения

### Точки входа и инициализация
- `cmd/server/main.go` — main, signal handling, retry-loop
- `internal/app/app.go` — сборка `AgentApp`, регистрация gRPC сервисов

### Master-логика
- `internal/grpc/get_master_info.go` (**2 793 стр.**) — основной API
- `internal/master/background.go` (**624 стр.**) — фоновый puller, merge

### Segment-логика
- `internal/grpc/set_query_info.go` — приём от hooks-collector
- `internal/uds/processor.go` — UDS-листенер

### Хранилища и merge
- `internal/gp/sessions.go` (819 стр.) — `SessionsStorage`
- `internal/storage/metrics_storage.go` — `RunningQueriesStorage`
- `internal/storage/aggregated_storage.go` — `AggregatedStorage`
- `internal/storage/merger.go` — merge segment-данных

### Опциональный sink (master-only)
- `internal/sink/clickhouse/writer.go` — `ClickhouseWriter` orchestrator
- `internal/sink/clickhouse/tables.go` — per-table writer'ы
- `internal/sink/clickhouse/migrations.go` + `migrations/0001_init.up.sql` — embedded DDL

### API-контракты
- `api/proto/common/yagpcc_metrics.proto` — `QueryStatus`, `GPMetrics`, `SystemStat`
- `api/proto/agent_master/yagpcc_get_service.proto` — `GetGPInfo`
- `api/proto/agent_master/yagpcc_action_service.proto` — `ActionService`

---

## Связь с GPDB

- **`yagp-hooks-collector`** — расширение в Greenplum, инжектится в backend-процессы и шлёт метрики в локальный yagpcc через UDS gRPC.
- **libpq к master** — для автодискавери segment-хостов через `gp_segment_configuration` и чтения `pg_stat_activity`.
- **Действия** (`ActionService`) — `pg_terminate_backend`, `pg_cancel_backend`, перенос в resource group.

yagpcc — внешний слой телеметрии: GP-кластер работает независимо, yagpcc только наблюдает и предоставляет управляющие действия через свой API.
