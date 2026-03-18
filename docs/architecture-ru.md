# Общая схема работы

В Greenplum клиент всегда подключается к `Master` и отправляет запросы ему.
Однако сами [запросы исполняются](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/6/greenplum-database/admin_guide-query-topics-parallel-proc.html)
преимущественно на `Segment`-ах,
где и формируется подробная статистика их выполнения.

Для сбора статистики внутри `Greenplum` используется расширение `yagp-hooks-collector`,
которое работает как на каждом из `Segment`-ов, так и на `Master`.
Этот компонент подключается к системе хуков выполнения запроса в `Greenplum`;
и собирает необходимую телеметрию о ходе исполнения запроса.

Собранная посредством хуков статистика отправляется в локально запущенный `yagpcc`
(на каждом `Segment` и `Master` хостах).
`yagpcc` "слушает" Unix Domain Socket (UDS)
и принимает телеметрию от `yagp-hooks-collector` через protobuf `SetQueryInfo/SetMetricQuery`.

Таким образом, статистика генерируется и отправляется в локальный `yagpcc`
сразу по мере поступления данных (например, по этапам выполнения запроса),
а также после завершения запроса.

Такая реализация имеет накладные расходы и влияет на задержку обработки запросов,
так как завершение запроса возможно только после передачи статистики хуком.
Для решения такого рода проблем встроены различные таймауты
и механизмы защиты от зависания (например, обработка недоступности `yagpcc`) —
в случае таких сбоев статистика теряется, но пользовательские запросы продолжают работать.

Статистика, собранная с `Segment`-ов, по своей природе является неполной,
так как каждый `Segment` отвечает только за часть запроса.

Для получения полной картины все данные должны быть агрегированы.
Этим занимается `yagpcc`, запущенный на `Master Host`.
Концептуально это выглядит следующим образом:

1. `yagpcc` на `Master Host` подключается к `Master` через стандартный протокол `libpq`
и получает список всех `Segment Host`-ов и их адреса;
2. `yagpcc` на `Master Host` периодически опрашивает `yagpcc` на `Segment Host`-ах
через GRPC-интерфейсы `GetQueryInfo/GetMetricQueries` и получает актуальную статистику о выполнении запросов;
3. `yagpcc` на `Master Host` объединяет полученные данные с `Segment`-ов
и сохраненную локальную статистику с `Master`, формирует итоговые агрегаты и хранит их в своей памяти.

Итоговая агрегированная статистика, собранная `yagpcc` на `Master Host`,
может быть предоставлена пользователю в различных формах
(см. разделы
[Статистика в реальном времени](./real-time-stats-flow.md)
и [Историческая статистика](./historical-stats-flow.md)).

На диаграмме ниже представлена общая схема работы
на примере кластера Greenplum, состоящего из одного `Master Host`
и двух `Segment Host`, на каждом из которых расположено по 2 `Segment`.
Стрелками показан поток данных статистики,
который может отличаться от реального направления вызовов.

```mermaid
flowchart TB
    subgraph SegmentHostA[Segment Host 1]
        subgraph Segment01[Segment 1]
            Greenplum01[Greenplum]
            YagpHooksCollector01[yagp-hooks-collector]

            Greenplum01 --hooks magic--> YagpHooksCollector01
        end

        subgraph Segment02[Segment 2]
            Greenplum02[Greenplum]
            YagpHooksCollector02[yagp-hooks-collector]

            Greenplum02 --hooks magic--> YagpHooksCollector02
        end

        SegmentHostAYagpcc[yagpcc]

        YagpHooksCollector01 --UDS GRPC<br>SetQueryInfo/SetMetricQuery--> SegmentHostAYagpcc
        YagpHooksCollector02 --UDS GRPC<br>SetQueryInfo/SetMetricQuery--> SegmentHostAYagpcc
    end

    subgraph SegmentHostB[Segment Host 2]
        subgraph Segment03[Segment 3]
            Greenplum03[Greenplum]
            YagpHooksCollector03[yagp-hooks-collector]

            Greenplum03 --hooks magic--> YagpHooksCollector03
        end

        subgraph Segment04[Segment 4]
            Greenplum04[Greenplum]
            YagpHooksCollector04[yagp-hooks-collector]

            Greenplum04 --hooks magic--> YagpHooksCollector04
        end

        SegmentHostBYagpcc[yagpcc]

        YagpHooksCollector03 --UDS GRPC<br>SetQueryInfo/SetMetricQuery--> SegmentHostBYagpcc
        YagpHooksCollector04 --UDS GRPC<br>SetQueryInfo/SetMetricQuery--> SegmentHostBYagpcc
    end

    subgraph MasterHost[Master Host]
        subgraph Master
            Greenplum[Greenplum]
            YagpHooksCollector[yagp-hooks-collector]

            Greenplum --hooks magic--> YagpHooksCollector
        end

        subgraph yagpcc
            direction TB

            YagpccSetQueryInfo[SetQueryInfo<br>GRPC<br>Service]
            YagpccStorage[Storage]
            YagpccSegmentPuller[Segments<br>Puller]
            YagpccGetQueryInfo[GetQueryInfo<br>GRPC<br>Service]

            YagpccSetQueryInfo --saves<br>received<br>metrics--> YagpccStorage
            YagpccSegmentPuller --saves<br>metrics<br>pulled<br>from<br>segments--> YagpccStorage
            YagpccStorage --> YagpccGetQueryInfo
        end

        YagpHooksCollector --UDS GRPC<br>SetQueryInfo/SetMetricQuery--> yagpcc
        Greenplum --libpq<br>standard<br>query<br>protocol--> yagpcc
    end

    SegmentHostAYagpcc --TCP GRPC<br>GetQueryInfo/GetMetricQueries--> yagpcc
    SegmentHostBYagpcc --TCP GRPC<br>GetQueryInfo/GetMetricQueries--> yagpcc

    StatsConsumer(Various consumers of collected statistics)

    yagpcc --> StatsConsumer
```
