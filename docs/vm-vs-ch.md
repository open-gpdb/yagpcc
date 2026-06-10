# VictoriaMetrics vs ClickHouse — разделение ролей в стенде yagpcc

## Контекст

В observability-стеке поверх Greenplum + yagpcc одновременно используются **два хранилища**:
- **VictoriaMetrics** — для time-series метрик
- **ClickHouse** — для query history (событий и агрегатов от yagpcc)

Документ объясняет, **почему оба нужны** и **как разделены данные** между ними. Адресован тем, кто впервые видит схему и подозревает дублирование.

## Короткий ответ

**VM и CH не дублируют друг друга и не общаются между собой.** Каждый принимает свой класс данных напрямую от yagpcc. Объединение в один стор технически невозможно без потерь.

---

## Разделение ролей

| | **VictoriaMetrics** | **ClickHouse** |
|---|---|---|
| **Тип данных** | Time-series (число + labels + timestamp) | Структурированные записи (десятки колонок: текст, числа, массивы) |
| **Что про yagpcc** | Здоровье агента: UDS msg/sec, размер буфера, gRPC latency, error rate | Каждый завершённый запрос: SQL текст, план, CPU/RAM/IO, длительность, per-segment метрики |
| **Кардинальность labels** | Низкая (instance, job, segment_id) | Высокая (миллионы уникальных `query_id` / `plan_id`) |
| **Типичный запрос** | `rate(yagpcc_active_queries[5m])` | `SELECT user, sum(duration_ms) FROM query_events GROUP BY user` |
| **Ретеншен** | 2 месяца | **30 дней** (по решению `observability-stack.md` Task 2) |
| **Объём на запрос** | 1 datapoint = 16 байт | 1 строка ≈ 1-2 КБ (с plan_tree до десятков КБ) |
| **Datasource в Grafana** | Prometheus-compatible (officially) | grafana-clickhouse-datasource |
| **Где живёт алертинг** | vmalert + Alertmanager | ❌ не для алертинга |

---

## Что куда подходит — практические запросы

| Задача | VM | CH |
|---|---|---|
| "CPU агента yagpcc выше 90% последний час" | ✅ оптимально | ❌ нерелевантно |
| "Топ-10 SQL запросов по длительности за неделю" | ❌ невозможно | ✅ оптимально |
| Алерт `yagpcc_uds_send_failures > 0` | ✅ через vmalert | ❌ |
| Drill-down в plan tree конкретного запроса | ❌ | ✅ |
| Сколько rps событий принимает yagpcc | ✅ | ❌ |
| Когда последний раз user X запускал тяжёлый запрос | ❌ | ✅ |
| Скью между сегментами в конкретном запросе | ❌ | ✅ (per-segment колонки) |
| Сравнить план Q1 за прошлую неделю и сегодня | ❌ | ✅ (через `plan_id` join) |
| Health-check yagpcc сам по себе | ✅ | ❌ |
| Ретроспективный анализ инцидента "что бежало 5 апреля 14:00" | ❌ (метрики свёрнуты) | ✅ (event-log полный) |

---

## Архитектура потоков данных

```
yagpcc (master)
   │
   ├──/metrics─→ vmagent ──remote_write─→ VictoriaMetrics  ←── Grafana (PromQL)
   │   (что делает агент)                                       └→ vmalert → Alertmanager
   │
   └─[clickhouse_writer]─→ ClickHouse  ←──────────────────────── Grafana (SQL)
       (что делал GP-кластер: события + агрегаты + plan tree)
```

**Ключевое**: VM и CH питаются от yagpcc **параллельно**, не последовательно. Между ними нет ни ETL-пайплайна, ни форвардинга.

---

## Почему не один из двух

### "Только VM" — невозможно
- Per-query records порвут TSDB по cardinality. У VM есть жёсткий лимит уникальных серий (`-search.maxUniqueTimeseries`); миллионы уникальных `query_id` / `plan_id` мгновенно его превысят
- VM хранит **числа** — текст SQL, план, имена таблиц туда не положишь
- Аналитика типа "GROUP BY user, ORDER BY sum(cpu)" в PromQL громоздкая или невозможная

### "Только CH" — теоретически можно, практически плохо
- Time-series метрики (gauge `active_queries`, counter `uds_received_total`) технически в CH хранятся, но
- **vmalert не работает с CH-datasource** — нужно городить свои Go-rules или Grafana Alerting
- **Стандартные дашборды** (Node Exporter Full, VictoriaMetrics, ClickHouse Overview) написаны под Prometheus-source — придётся переписывать
- **Гранулярность time-series** в CH страдает: индексы оптимизированы под bulk inserts, не под частые точечные запросы (`avg over 1m`, `rate over 5m`)
- Экспортеры (`node_exporter`, `postgres_exporter`, `clickhouse-server` сам) умеют только Prometheus формат — нужен конвертер
- Размер CH под time-series в разы больше чем у VM (нет специализированной компрессии gorilla/dod)

---

## Что это значит для CH-sink в yagpcc

Sink в CH **не дублирует** то что уже идёт в VM:

### В VM (через `/metrics` → vmagent → remote_write) идут:
- counter'ы: `yagpcc_uds_received_total`, `yagpcc_uds_send_failures_total`, `yagpcc_grpc_calls_total`
- gauge: `yagpcc_active_queries`, `yagpcc_aggregated_storage_size_bytes`, `yagpcc_buffer_size`
- histogram: `yagpcc_grpc_pull_duration_seconds`, `yagpcc_uds_message_size_bytes`
- метрики самого sink'а: `yagpcc_ch_inserts_total`, `yagpcc_ch_buffer_size`, `yagpcc_ch_dropped_rows_total`,
  `yagpcc_ch_schema_mismatch`, `yagpcc_ch_unreachable` (см. `docs/clickhouse-sink-architecture.md`)

### В CH (через `internal/sink/clickhouse/`, реализован в v1) идёт:
- `yagpcc.query_events` — event-log запросов (включая JSON `plan_tree` и массив `segments`)
- `yagpcc.aggregated_metrics` — агрегаты по плану/пользователю/БД
- `yagpcc.session_snapshots` — периодические снэпшоты сессий
- `yagpcc._yagpcc_meta` — служебная таблица для schema versioning
- `yagpcc.plan_nodes` — нормализованный plan tree (опционально, v2)
- `yagpcc.query_segments` — per-segment отдельной таблицей (опционально, v2)

Это **разные данные**, не разные хранилища одних данных.

---

## Связанные документы

- `/ya/src/greenplum-role-ubuntu/docs/plans/observability-stack.md` — план развёртывания всего стека (Task 2 — роль `clickhouse`, Task 3 — роль `victoriametrics`)
- `/ya/src/yagpcc/docs/clickhouse-sink-architecture.md` — архитектура реализованного sink'а (`internal/sink/clickhouse/`)
- `/ya/src/yagpcc/docs/plans/clickhouse-sink.md` — план Go-реализации CH-sink (выполнен)
- `/ya/src/yagpcc/docs/plans/clickhouse-sink-questions.md` — вопросы по архитектуре sink перед стартом
