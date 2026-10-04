# Research: сравнение двух учебных реализаций Raft

## Статус

- Этап: `/research`
- Дата: 2026-10-01
- Продуктовый код изменён: нет
- Локальная реализация: текущее рабочее дерево `/Users/pozhidaev/Documents/ChatGPT/raft`, ветка `main` без коммитов
- Внешняя реализация: [`MlronGaskov/raft`](https://github.com/MlronGaskov/raft/tree/16f0de1ff2f03e75f5253d3d8e0cf67a96424608), commit `16f0de1ff2f03e75f5253d3d8e0cf67a96424608` (`step 1`)

## Цель и scope

Сравнить архитектуру, поведение, корректность, эксплуатационные свойства и проверяемость двух реализаций выбора лидера Raft. Выделить различия, сильные и слабые стороны каждой реализации и риски, которые важны при выборе базы для дальнейшего развития.

В scope входят:

- выбор лидера, термы, голоса и heartbeat;
- таймеры и модель конкурентности;
- сохранение состояния;
- конфигурация и транспорт;
- API, наблюдаемость и удобство запуска;
- тесты, документация и зависимости;
- фактические ограничения обеих реализаций.

Вне scope:

- изменение любой из реализаций;
- детальный технический план объединения решений;
- commit, push, merge или deploy;
- сравнение производительности под нагрузкой;
- доказательство формальной корректности.

## Критерии готовности

1. Обе реализации привязаны к конкретному проверенному состоянию исходников.
2. Существенные выводы подтверждены ссылками на файлы и строки.
3. Отдельно описаны функциональность, алгоритмические решения, эксплуатационные свойства и тесты.
4. Плюсы и минусы сформулированы для обеих реализаций.
5. Подтверждённые факты отделены от рисков и выводов по коду.
6. Выполнены доступные локальные проверки без изменения продуктового кода.

## Краткий вывод

Решения находятся на близком алгоритмическом уровне: оба реализуют фиксированный кластер, роли follower/candidate/leader, случайный election timeout, `RequestVote`, пустой `AppendEntries` и повторные выборы. Ни одно не является полной реализацией Raft: нет приёма клиентских команд, репликации новых записей, commit/apply pipeline, state machine, snapshot и изменения membership.

Главное различие в приоритетах:

- **наша реализация** — более законченный учебный продукт и более проверяемая кодовая база: строгие границы модулей, внедряемый transport, защита таймеров от устаревших событий, строгая проверка RPC, status/health API, web UI и 17 тестов, включая выборы и failover реального трёхузлового HTTP-кластера;
- **внешняя реализация** — меньше по объёму и ближе к обязательному состоянию Raft на диске: `current_term`, `voted_for` и заготовка log сохраняются атомарной заменой файла. При этом у неё нет тестов, README, status API и защиты от нескольких реальных отказов на границе RPC.

Если цель — показать выборы и отказ лидера человеку, наша реализация существенно сильнее. Если цель — сделать более правильную основу именно для Raft, важнейшее преимущество внешней реализации — persistence term/vote — необходимо учитывать: без него наша версия не сохраняет election safety после перезапуска процесса.

## Сводная таблица

| Область | Наша реализация | `MlronGaskov/raft` | Оценка |
|---|---|---|---|
| Язык и runtime | Go, только стандартная библиотека | Python `asyncio` + `aiohttp` | Python-код компактнее; Go-версия запускается без внешних runtime-зависимостей |
| Конфигурация | CLI-флаги, полный список peers повторяется при запуске каждого узла | Один общий `config.json`, CLI содержит ID и путь к state | Внешний вариант удобнее для повторяемого ручного запуска; наша валидация заметно строже |
| Состояние Raft | В памяти | `current_term`, `voted_for`, `log` в JSON-файле | Существенный плюс внешнего решения для restart safety |
| Election timer | Deadline + generation; событие timer перепроверяется под mutex | `asyncio.Event` + `wait_for` | Наша схема сложнее, но устойчивее к устаревшему/пограничному timeout |
| Конкурентность | Mutex для state, отдельные goroutine для RPC | Один event loop, state transition без `await` | Внешняя модель проще; синхронный `fsync` блокирует event loop |
| Majority | От полного настроенного состава | От полного настроенного состава | Эквивалентно и корректно для fixed membership |
| Vote/log freshness | Поддерживается только заведомо пустой log; ненулевые log-поля отвергаются | Сравниваются last log term/index по сохранённому log | Внешний вариант ближе к Figure 2 Raft, но log нигде не пополняется штатным API |
| Идентичность RPC-отправителя | Candidate/leader должен входить в fixed membership | Membership не проверяется | Наша версия лучше защищает состояние от случайного/чужого ID |
| Wire validation | Строгий JSON, лимиты body, обязательные response fields | Входные dict конвертируются вручную; ответ проверяется только на тип `dict` | Наша версия устойчивее к повреждённым и неожиданным ответам |
| Heartbeat scheduling | Ticker может запустить новый broadcast до завершения предыдущего | Следующий цикл начинается после завершения предыдущей рассылки | Внешняя схема ограничивает overlap; наша лучше отделяет scheduler от сети, но допускает лишнюю конкуренцию |
| API и UI | Raft RPC + `/health`, `/status`, `/cluster/status`, встроенный UI | Только `/request-vote` и `/append-entries` | Явное преимущество нашей версии для демо и диагностики |
| Наблюдаемость | API/UI; минимум terminal-логов | Читаемые terminal-логи переходов ролей и голосов | Разные сильные стороны; наша версия лучше для внешнего наблюдателя, внешняя — для простого чтения terminal output |
| Тесты | 17 тестов, unit/HTTP/integration, race detector | Тестов нет | Существенное преимущество нашей версии |
| Документация | README с запуском, API и ограничениями; research/design artifacts | README отсутствует | Существенное преимущество нашей версии |

## Подтверждённые факты: наша реализация

### Алгоритм и границы

- Роли, DTO Raft RPC и status snapshot разделены в [`internal/raft/types.go`](../../../internal/raft/types.go#L11).
- Узел хранит роль, term, голос, известного лидера, deadline и generation election timer под одним mutex в [`internal/raft/node.go`](../../../internal/raft/node.go#L21).
- Higher term переводит узел в follower, очищает vote/leader и создаёт новый deadline в [`internal/raft/node.go`](../../../internal/raft/node.go#L242).
- Heartbeat того же term переводит candidate/leader в follower, но не очищает уже выданный в этом term голос в [`internal/raft/node.go`](../../../internal/raft/node.go#L257).
- Выборы используют self-vote, параллельные RPC, проверку пары `(term, generation)`, majority полного состава и отмену устаревшего election round в [`internal/raft/election.go`](../../../internal/raft/election.go#L23).
- Таймер перед началом выборов повторно проверяет роль, generation и deadline в [`internal/raft/election.go`](../../../internal/raft/election.go#L23); его единственный владелец находится в [`internal/raft/runtime.go`](../../../internal/raft/runtime.go#L7).
- Реализованы только пустые heartbeats: ненулевые log metadata, `leader_commit` или entries отклоняются в [`internal/raft/node.go`](../../../internal/raft/node.go#L214).

### Границы и эксплуатация

- Candidate/leader ID проверяется по fixed membership **до** изменения term/state в [`internal/raft/node.go`](../../../internal/raft/node.go#L175).
- HTTP decoder ограничивает request body, запрещает неизвестные поля и требует одно JSON-значение в [`internal/httpapi/server.go`](../../../internal/httpapi/server.go#L117).
- HTTP client требует обязательные поля ответа и ограничивает response body в [`internal/transport/http.go`](../../../internal/transport/http.go#L33).
- Сервер предоставляет health, local status, aggregated cluster status, Raft RPC и встроенные web assets в [`internal/httpapi/server.go`](../../../internal/httpapi/server.go#L27).
- Cluster status запрашивает соседей параллельно с индивидуальным timeout и сохраняет недоступные узлы в ответе в [`internal/httpapi/cluster.go`](../../../internal/httpapi/cluster.go#L34).
- Конфигурация проверяет уникальность ID и URL, наличие self, форму URL и соотношение таймингов в [`internal/config/config.go`](../../../internal/config/config.go#L58).
- README явно фиксирует отсутствие persistence, log replication, state machine, snapshot и membership changes в [`README.md`](../../../README.md#L5).

### Проверки

- Unit-тесты проверяют один голос в term, higher term, unknown member, повторные выборы, невозможность избрания меньшинством, стабильность follower под heartbeat и step-down по higher-term response в [`internal/raft/node_test.go`](../../../internal/raft/node_test.go#L30).
- Интеграционный тест поднимает три настоящих HTTP listener, дожидается одного лидера, останавливает его и проверяет новый term/leader и unreachable status в [`internal/integration/election_test.go`](../../../internal/integration/election_test.go#L25).
- На исследованном состоянии успешно выполнены:
  - `go test ./...`;
  - `go test -race ./...`;
  - `go vet ./...`.

## Подтверждённые факты: внешняя реализация

### Алгоритм и persistence

- Конфигурация кластера и тайминги читаются из одного JSON-файла; majority вычисляется от полного `nodes` в [`raft/config.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/config.py#L17-L53).
- `current_term`, `voted_for` и `log` загружаются при создании узла в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L18-L46).
- Перед началом выборов и перед выдачей нового голоса term/vote сохраняются на диск в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L139-L163) и [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L199-L219).
- Хранилище пишет временный файл, делает `flush` + `fsync` и заменяет основной файл через `os.replace` в [`raft/storage.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/storage.py#L15-L43).
- При голосовании проверяется freshness candidate log по term, затем index в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L76-L91).
- Vote RPC рассылаются параллельно и обрабатываются по мере завершения; majority приводит к роли leader в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L165-L197).
- Heartbeat loop ждёт завершения текущей рассылки и уменьшает следующую задержку на её длительность в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L221-L262).

### Границы и эксплуатация

- Узел сам создаёт storage, RPC client и RPC server в конструкторе, то есть инфраструктурные зависимости не внедряются извне в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L18-L46).
- HTTP server предоставляет только два POST endpoint в [`raft/transport.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/transport.py#L42-L80).
- Client скрывает network/timeout/non-200 ошибки как `None`, а успешный ответ проверяет только на то, что это dict, в [`raft/transport.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/transport.py#L14-L39).
- Входящие `candidateId` и `leaderId` не сверяются с configured membership в [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L199-L219) и [`raft/node.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/raft/node.py#L264-L291).
- CLI позволяет выбрать ID, config, state directory и явно сбросить state в [`main.py`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/main.py#L10-L29).
- Единственная внешняя зависимость — `aiohttp>=3.10,<4` в [`requirements.txt`](https://github.com/MlronGaskov/raft/blob/16f0de1ff2f03e75f5253d3d8e0cf67a96424608/requirements.txt).
- В commit отсутствуют README и тестовые файлы. `python3 -m compileall -q .` проходит, но runtime-проверка в текущем окружении без установки dependency невозможна: `aiohttp` отсутствует.

## Плюсы и минусы нашей реализации

### Плюсы

1. **Лучше защищена конкурентная state machine.** Generation/deadline не позволяют устаревшему событию timer автоматически начать выборы после принятого heartbeat.
2. **Лучше проверены значимые сценарии.** Есть unit, HTTP и настоящий трёхузловой failover test; race detector проходит.
3. **Чётче модульные границы.** Raft core зависит от узкого `PeerClient`, а HTTP transport и presentation слой вынесены отдельно.
4. **Строже protocol boundary.** Проверяются membership, schema, required fields, body size и unsupported log semantics.
5. **Намного лучше как учебное демо.** Есть health/status/cluster API, встроенный UI, инструкция запуска и явное описание ограничений.
6. **Нет внешних зависимостей.** Для сборки и запуска достаточно Go toolchain.

### Минусы

1. **Нет persistence `currentTerm` и `votedFor`.** После рестарта узел забывает term/голос, поэтому нельзя утверждать election safety через process restart; это главное протокольное отставание от внешней версии.
2. **Только пустой log.** Версия сознательно не проверяет log freshness, а отклоняет любые ненулевые log fields.
3. **Конфигурация менее удобна для ручного кластера.** Один и тот же полный peer list приходится повторять в команде каждого процесса.
4. **Возможны перекрывающиеся heartbeat broadcast.** Ticker запускает новую рассылку независимо от завершения предыдущей в [`internal/raft/runtime.go`](../../../internal/raft/runtime.go#L29), а каждая рассылка создаёт отдельный набор RPC в [`internal/raft/heartbeat.go`](../../../internal/raft/heartbeat.go#L26). Timeout ограничивает накопление, но при медленной сети это создаёт лишнюю конкуренцию.
5. **Больше кода и механизмов.** Для минимальной демонстрации mutex, generation, cancellation и несколько пакетов сложнее изучать, чем компактный asyncio-вариант.

## Плюсы и минусы внешней реализации

### Плюсы

1. **Сохраняет обязательное election state.** Term и vote переживают корректный restart процесса, что ближе к требованиям Raft.
2. **Компактная модель конкурентности.** Один asyncio event loop делает обычные state transitions последовательными без mutex.
3. **Удобная общая конфигурация.** Все узлы и тайминги описаны один раз в `config.json`.
4. **Есть проверка log freshness.** Даже при heartbeat-only scope заложена часть правильной логики будущего журнала.
5. **Нет overlap heartbeat rounds.** Следующая рассылка начинается после завершения предыдущей.
6. **Полезные terminal-логи.** Роли, term, голоса и лидер видны без отдельного клиента.

### Минусы

1. **Нет автоматических тестов.** Нельзя быстро подтвердить one-vote-per-term, split vote retry, failover, persistence или отсутствие регрессий.
2. **Нет документации и observer API.** Отсутствуют README, health/status endpoints и UI; решение труднее запустить и диагностировать без чтения исходников.
3. **Слабая валидация membership.** Любой `candidateId` или `leaderId` может изменить term/vote/role и сбрасывать election timer, если endpoint доступен.
4. **Malformed peer response может завершить фоновую task.** Client принимает любой JSON object, после чего vote/heartbeat logic без проверки обращается к `response["term"]` и `response["voteGranted"]`. `KeyError`/ошибка преобразования не перехватывается; для heartbeat это может остановить единственную `_heartbeat_task`.
5. **Election timer имеет пограничный race-риск.** После `asyncio.wait_for(..., timeout)` выборы запускаются без повторной проверки deadline/generation. Heartbeat, пришедший на границе timeout, может сбросить event, но уже истёкший loop всё равно способен начать новый term. По коду риск реален, хотя отдельного воспроизводящего теста в репозитории нет.
6. **Persistence блокирует event loop.** Синхронные `json.dump`, `flush`, `fsync` и `os.replace` выполняются внутри election/RPC handler path. На медленном диске это задержит обработку heartbeat и остальных RPC.
7. **Durability не полностью закрыта.** Файл fsync-ится, но directory после `os.replace` не fsync-ится; повреждённый JSON или неверные типы state не обрабатываются как recoverable startup error.
8. **Log support наполовину заявлен.** State хранит log и heartbeat проверяет `prevLog*`, но штатного способа добавить/реплицировать записи нет, а отказ `success=false` leader игнорирует.
9. **Сильная связность затрудняет тестирование.** `RaftNode` напрямую создаёт filesystem storage и aiohttp client/server; для unit-теста нужны monkeypatch или реальные ресурсы.
10. **RPC ошибки в основном молча теряются.** Network, timeout и non-200 превращаются в `None`, что упрощает алгоритм, но затрудняет диагностику.

## Общие ограничения обоих решений

1. Это leader-election demo, а не replicated state machine.
2. Нет приёма команд от клиента, репликации записей, commit index advancement, apply к state machine, snapshot и membership changes.
3. Fixed membership задаётся до запуска.
4. Транспорт — unauthenticated plain HTTP. Проверка заявленного node ID в нашей версии не аутентифицирует реального отправителя: злоумышленник всё равно может подставить ID известного участника.
5. Нет pre-vote, leadership transfer, read index и других расширений production-grade реализаций.
6. Нет chaos/network-partition test suite и длительных soak tests.

## Риски по приоритету

### Высокий

- **Наша версия:** потеря persisted term/vote на restart нарушает базовую safety-модель Raft за границами жизни процесса.
- **Внешняя версия:** непроверенный/повреждённый 200-ответ peer способен завершить heartbeat task; после этого узел может продолжать считать себя leader, но перестать удерживать followers heartbeat-ами.
- **Внешняя версия:** RPC от неучастника способен поднять term, получить голос или подавлять выборы heartbeat-ами.

### Средний

- **Внешняя версия:** граничная гонка event-based election timeout из-за отсутствия повторной проверки поколения/deadline.
- **Внешняя версия:** синхронный fsync внутри event loop увеличивает вероятность ложных выборов при задержках диска.
- **Наша версия:** overlap heartbeat rounds создаёт лишнюю нагрузку при времени ответа, близком к heartbeat interval.

### Низкий или эксплуатационный

- Наша CLI-конфигурация легче расходится между терминалами; внешний единый JSON легче воспроизводить.
- Внешнее решение нельзя запустить в чистом Python-окружении без установки `aiohttp`; наша версия собирается без сторонних модулей.
- Внешний state directory не привязан к cluster identity, поэтому повторное использование каталога с другой конфигурацией может принести старый term/vote/log в новый кластер.

## Итоговая оценка

### Для текущей учебной задачи

Наша реализация сильнее почти по всем критериям готовности продукта: её проще запустить, наблюдать, демонстрировать, проверять и безопасно изменять. Тесты и явные scope-ограничения дают гораздо больше доверия, чем компактность внешнего решения.

### Для движения к более полному Raft

У внешней реализации есть одно принципиальное преимущество, которое перевешивает многие косметические различия: persisted `currentTerm` и `votedFor`. Это не дополнительная функция, а часть safety-модели Raft. При дальнейшем развитии нашей реализации persistence следует рассматривать раньше log replication; иначе новый журнал будет строиться поверх состояния, которое небезопасно переживает restart.

При этом переносить внешний код целиком нецелесообразно: его timer model, response validation, membership checks и testability слабее нашей архитектуры. Полезное направление — сохранить наши модульные границы и проверки, а из внешнего решения заимствовать саму идею durable term/vote и удобную file-based cluster configuration.

## Допущения и открытые вопросы

- Сравнение сделано с внешним commit `16f0de1`; более поздние изменения репозитория могут изменить выводы.
- Отсутствие тестов означает отсутствие файлов test suite в исследованном commit, а не доказательство, что автор никогда не проверял решение вручную.
- Race-risk внешнего timer и остановка task на malformed response выведены из control flow кода; готового regression test в исходном репозитории нет.
- Если будет нужен следующий этап, сначала требуется решить, какова цель `/design`: только добавить persistence в нашу версию, перенять также file config, либо спроектировать следующий полноценный Raft milestone с log replication.
