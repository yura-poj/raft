# Технический проект: каркас Raft, выбор лидера и интерфейс состояния кластера

## Статус документа

- Этап: `/design`
- Дата: 2026-10-01
- Основание: [research.md](./research.md)
- Язык реализации: Go
- Изменение продуктового кода на этом этапе: нет
- Независимая проверка плана: выполнена один раз; критичные замечания учтены в этом документе

## 1. Цель

Создать учебный сервис, состоящий из нескольких независимо запускаемых узлов. Узлы образуют фиксированный Raft-кластер, выбирают лидера, поддерживают его полномочия heartbeat-сообщениями и выбирают нового лидера после остановки текущего. Каждый процесс предоставляет REST API и встроенную веб-страницу, на которой видны все настроенные узлы и их состояния.

Результат не является полной реализацией Raft: журнала команд, репликации, конечного автомата и сохранения состояния на диск не будет.

## 2. Выбранный технический подход

### 2.1. Один Go-процесс на узел

Каждый узел запускается одной командой `go run ./cmd/raft-node` или из собранного бинарного файла. Идентификатор узла, адрес прослушивания и полный список участников передаются флагами командной строки.

Подход выбран потому, что он:

- прямо соответствует требованию запускать узлы в разных терминалах;
- не требует Docker, базы данных, брокера или отдельного frontend-процесса;
- позволяет использовать реальные HTTP-вызовы и при ручной демонстрации, и в интеграционных тестах;
- оставляет границу для последующего добавления журнала, не включая его сейчас.

Конфигурационный файл не вводится: для трёх демонстрационных узлов CLI-флаги короче и прозрачнее, а поддержка нескольких источников конфигурации не даёт ценности в текущем scope.

### 2.2. Стандартная библиотека Go

Используются `net/http`, `encoding/json`, `sync`, `context`, `time`, `math/rand`, `embed`, `flag` и стандартные средства завершения процесса. Внешние библиотеки и frontend-фреймворки не нужны.

Это уменьшает каркас и делает алгоритм выборов видимым без инфраструктурного шума. Для текущих REST-маршрутов стандартного `http.ServeMux` достаточно.

### 2.3. Короткие критические секции под `sync.Mutex`

Состояние Raft хранится внутри `raft.Node` и защищается одним `sync.Mutex`. Под блокировкой выполняются только чтение и атомарные переходы состояния. HTTP-вызовы к соседям, ожидание ответов и JSON-кодирование выполняются без блокировки.

Выбран mutex-подход, а не полностью канальная actor-модель, потому что набор событий мал, а обработчики RPC должны синхронно возвращать результат. Для защиты от устаревших асинхронных событий каждый результат голосования и heartbeat проверяется по term и текущей роли перед применением.

### 2.4. Один управляющий цикл таймеров

Узел запускает один внутренний цикл, который отвечает за:

- election timer;
- периодическую отправку heartbeat лидером;
- остановку фоновой работы.

Единственный управляющий цикл владеет объектом `time.Timer`: только он выполняет `Stop`, при необходимости дренирует канал и вызывает `Reset`. Под lock узел хранит авторитетные `electionDeadline` и `electionGeneration`. Сброс таймера сначала увеличивает поколение и задаёт новый deadline под lock, затем уже без lock посылает неблокирующий coalescing-сигнал управляющему циклу. Потеря нескольких одинаковых wake-сигналов безопасна, потому что цикл каждый раз читает последнее авторитетное состояние.

Когда timer срабатывает, цикл повторно проверяет под lock роль, поколение и deadline. Если поколение изменилось или deadline ещё не наступил, таймер только перевзводится; выборы не начинаются. Поэтому старое или одновременно доставленное событие timer не начинает выборы после принятого heartbeat или выданного голоса.

Сами сетевые рассылки запускаются конкурентно и не блокируют управляющий цикл. Повторная попытка выборов происходит естественно по следующему случайному election timeout; отдельный универсальный retry/backoff не добавляется.

### 2.5. Полные имена Raft RPC, но только heartbeat-семантика `AppendEntries`

REST-маршруты и структуры сохраняют названия `RequestVote` и `AppendEntries` из статьи. В DTO остаются стандартные поля метаданных журнала со значением `0`, чтобы граница будущего расширения была явной.

При этом текущая реализация принимает только пустой `AppendEntries`:

- `entries` должен отсутствовать или быть пустым;
- `prev_log_index`, `prev_log_term` и `leader_commit` должны быть равны `0`;
- непустой запрос возвращает `400 Bad Request` как функциональность вне scope.

Так сервис не заявляет ложную поддержку репликации журнала и одновременно остаётся узнаваемым как каркас Raft.

### 2.6. Серверная агрегация состояния кластера

Frontend обращается только к тому узлу, с которого загружен, через `GET /cluster/status`. Этот обработчик берёт локальный snapshot напрямую, а статусы соседей запрашивает параллельно через `GET /status` с коротким timeout.

Серверная агрегация выбрана вместо прямых запросов браузера к каждому узлу, потому что:

- не нужна CORS-конфигурация;
- недоступность соседа представляется одинаково во всех браузерах;
- один медленный узел не задерживает остальные ответы;
- frontend остаётся простым статическим набором файлов.

Ответ является наблюдаемым, но не атомарным snapshot всего кластера. Это будет явно указано в README и подписи интерфейса.

## 3. Структура файлов

Во время `/release` планируется создать следующие файлы:

```text
go.mod
README.md
cmd/raft-node/main.go
internal/config/config.go
internal/config/config_test.go
internal/raft/types.go
internal/raft/node.go
internal/raft/election.go
internal/raft/heartbeat.go
internal/raft/node_test.go
internal/transport/http.go
internal/transport/http_test.go
internal/httpapi/server.go
internal/httpapi/cluster.go
internal/httpapi/server_test.go
internal/httpapi/web/index.html
internal/httpapi/web/app.js
internal/httpapi/web/styles.css
internal/integration/election_test.go
```

`docs/tasks/raft-leader-election/research.md` и этот `design.md` остаются входными артефактами и не заменяются README.

## 4. Модули и ответственность

### 4.1. `go.mod`

- Модуль: `raftdemo`.
- Версия языка: Go 1.26, соответствующая доступному локальному toolchain.
- Внешние зависимости отсутствуют.

### 4.2. `cmd/raft-node/main.go`

Точка сборки приложения:

1. Разобрать CLI-флаги через пакет `internal/config`.
2. Создать HTTP transport с заданным RPC timeout.
3. Создать `raft.Node` с фиксированным списком участников и диапазоном election timeout.
4. Создать HTTP server и зарегистрировать маршруты.
5. Сначала открыть listener и запустить HTTP serving, затем запустить Raft-цикл, чтобы узел не отправлял RPC до готовности собственного API.
6. Обработать `SIGINT` и `SIGTERM`.
7. При завершении остановить Raft-цикл, отменить исходящие запросы и корректно завершить HTTP server с ограниченным timeout.

Отдельная подсистема логирования, метрики и распределённая трассировка не добавляются. Ошибки запуска и аварийное завершение выводятся стандартными средствами точки входа.

### 4.3. `internal/config/config.go`

Структуры:

```go
type Peer struct {
    ID  string
    URL string
}

type Config struct {
    NodeID             string
    ListenAddress      string
    Peers              []Peer
    HeartbeatInterval  time.Duration
    ElectionTimeoutMin time.Duration
    ElectionTimeoutMax time.Duration
    RPCTimeout         time.Duration
}
```

`internal/config.Config` является результатом CLI-разбора. В `main` он явно преобразуется в `raft.Config`; пакет `raft` не импортирует CLI-пакет. Общий доменный `raft.Peer` создаётся при этом преобразовании, поэтому параллельных изменяемых представлений состава кластера нет.

CLI-контракт:

- `--id` — уникальный ID текущего узла;
- `--listen` — адрес HTTP listener, например `127.0.0.1:8081`;
- `--peers` — полный фиксированный состав в формате `node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082`;
- `--heartbeat` — интервал heartbeat, по умолчанию `100ms`;
- `--election-min` — нижняя граница election timeout, по умолчанию `300ms`;
- `--election-max` — верхняя граница election timeout, по умолчанию `600ms`;
- `--rpc-timeout` — timeout одного peer-запроса, по умолчанию `150ms`.

Проверки конфигурации:

- обязательные `id`, `listen` и непустой `peers`;
- уникальные непустые ID и URL участников;
- текущий узел присутствует в `peers` ровно один раз;
- URL использует только `http`, содержит host и не содержит path, query или fragment;
- `heartbeat > 0`, `rpc-timeout > 0`;
- `election-min > heartbeat`;
- `election-max > election-min`.

Проверка нечётного количества узлов не вводится: Raft корректно определяет большинство и для чётного размера, хотя это обычно не даёт преимущества по отказоустойчивости.

### 4.4. `internal/raft/types.go`

Определяет доменные типы без HTTP-зависимостей:

```go
type Role string

const (
    RoleFollower  Role = "follower"
    RoleCandidate Role = "candidate"
    RoleLeader    Role = "leader"
)

type Peer struct {
    ID  string
    URL string
}

type Config struct {
    ID                 string
    Address            string
    Peers              []Peer
    HeartbeatInterval  time.Duration
    ElectionTimeoutMin time.Duration
    ElectionTimeoutMax time.Duration
    RPCTimeout         time.Duration
}

type RequestVoteRequest struct {
    Term         uint64 `json:"term"`
    CandidateID  string `json:"candidate_id"`
    LastLogIndex uint64 `json:"last_log_index"`
    LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
    Term        uint64 `json:"term"`
    VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
    Term         uint64          `json:"term"`
    LeaderID     string          `json:"leader_id"`
    PrevLogIndex uint64          `json:"prev_log_index"`
    PrevLogTerm  uint64          `json:"prev_log_term"`
    Entries      []json.RawMessage `json:"entries,omitempty"`
    LeaderCommit uint64          `json:"leader_commit"`
}

type AppendEntriesResponse struct {
    Term    uint64 `json:"term"`
    Success bool   `json:"success"`
}
```

Также здесь располагаются read-only структуры локального snapshot и сведения о peer. Внутреннее изменяемое состояние наружу не возвращается.

### 4.5. `internal/raft/node.go`

`Node` владеет состоянием:

- неизменяемые `id`, собственный URL и полный список участников;
- `role`;
- `currentTerm`;
- `votedFor`;
- `leaderID`;
- `lastLeaderContact` для отображения статуса;
- `electionDeadline`;
- `electionGeneration`;
- cancel-функция активного election round;
- источник случайного election timeout;
- transport-интерфейс;
- каналы reset/stop и `sync.WaitGroup` для фоновых задач.

Публичные методы:

```go
func NewNode(cfg Config, client PeerClient, options ...Option) (*Node, error)
func (n *Node) Start()
func (n *Node) Stop(ctx context.Context) error
func (n *Node) Status() Status
func (n *Node) HandleRequestVote(req RequestVoteRequest) (RequestVoteResponse, error)
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error)
```

`PeerClient` объявляется рядом с потребителем в пакете `raft`:

```go
type PeerClient interface {
    RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (RequestVoteResponse, error)
    AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
```

Основные инварианты:

- `currentTerm` не уменьшается в течение жизни процесса;
- leader существует только в term, в котором он получил большинство;
- узел голосует максимум за одного candidate в term в течение жизни процесса;
- больший term в любом корректном запросе или ответе вызывает переход в follower, очистку `votedFor` и `leaderID`;
- переход candidate/leader в follower по heartbeat **того же term** сохраняет `votedFor`; голос очищается только при переходе к строго большему term;
- только участник из фиксированного списка может быть `CandidateID` или `LeaderID` входящего RPC;
- сетевые операции не выполняются под `Node.mu`;
- snapshot для API копирует данные под lock и далее не обращается к изменяемым полям.

Вместо одного двусмысленного helper используются два перехода:

- `observeHigherTermLocked(term)` — для строго большего term: обновляет term, переводит в follower, очищает `votedFor` и `leaderID`, отменяет активный election round;
- `acceptLeaderLocked(term, leaderID)` — принимает heartbeat с term не меньше текущего; для большего term сначала применяет `observeHigherTermLocked`, а для равного term меняет роль на follower, но сохраняет `votedFor`; затем устанавливает `leaderID`, обновляет время контакта и deadline.

Сигнал перевзвода timer и вызов сохранённой cancel-функции выполняются после освобождения `Node.mu`.

### 4.6. `internal/raft/election.go`

#### Начало выборов

При действительном election timeout:

1. Под lock проверить поколение таймера и что роль не leader.
2. Увеличить `currentTerm`.
3. Перейти в candidate.
4. Записать собственный ID в `votedFor`.
5. Очистить `leaderID`.
6. Зафиксировать snapshot term и списка peer.
7. Начать новый случайный election timeout для возможного повторного тура.
8. Освободить lock.
9. Считать собственный голос первым.
10. Параллельно отправить `RequestVote` всем остальным узлам.

#### Обработка ответов

- Один collector goroutine владеет набором `votesByPeer` конкретного election round; сетевые goroutine только отправляют ему результаты через канал.
- Раунд однозначно определяется парой `(term, electionGeneration)` и имеет собственный cancelable context.
- Ошибка или timeout отдельного peer не считается голосом и не останавливает остальные запросы.
- Ответ с большим term переводит узел в follower.
- Ответ учитывается только если узел всё ещё candidate и его `currentTerm` равен term начатого тура.
- Каждый peer учитывается не более одного раза.
- После достижения `floor(clusterSize/2)+1` голосов узел атомарно становится leader.
- Оставшиеся RPC отменяются через context cancellation.
- Context раунда также отменяется при step-down, запуске нового election round и `Node.Stop`.
- После перехода в leader сразу отправляется первый heartbeat, без ожидания следующего ticker.

#### Обработка `RequestVote`

1. HTTP-слой проверяет непустой `candidate_id`, а доменный handler проверяет его принадлежность фиксированному cluster membership до изменения Raft state.
2. Запрос неизвестного участника получает `400 Bad Request`; его term не применяется, поскольку такой payload не является RPC данного фиксированного кластера.
3. Запрос с меньшим term получает `vote_granted=false`.
4. При большем term узел переходит в follower, обновляет term и очищает предыдущий голос.
5. `last_log_index` и `last_log_term` должны быть `0`, так как журнал отсутствует; иначе HTTP-слой отклоняет запрос как выходящий за scope.
6. Голос выдаётся, если `votedFor` пуст или уже равен этому candidate.
7. При выдаче голоса сохраняется `votedFor` и сбрасывается election timer.
8. Ответ всегда содержит актуальный `currentTerm`.

Самопереход single-node кластера в leader происходит сразу, поскольку собственный голос уже образует большинство.

### 4.7. `internal/raft/heartbeat.go`

Leader с интервалом `heartbeat` параллельно отправляет каждому peer пустой `AppendEntries` со своим term и ID. Отдельная ошибка доставки не меняет роль лидера: Raft не требует от лидера добровольно отказываться от роли только из-за потери большинства.

Ответы обрабатываются так:

- больший term переводит leader в follower;
- равный или меньший term не меняет состояние;
- устаревший ответ от прежнего term игнорируется.

Входящий heartbeat:

1. HTTP-слой проверяет непустой `leader_id`, а доменный handler проверяет его принадлежность фиксированному cluster membership до изменения Raft state.
2. Запрос неизвестного участника получает `400 Bad Request` и не сбрасывает election timer.
3. Запрос с term меньше локального получает `success=false`.
4. Запрос с непустым журналом или ненулевыми log-полями отклоняется HTTP-слоем как неподдерживаемый.
5. Запрос с term не меньше локального переводит candidate или старого leader в follower.
6. `currentTerm` обновляется при большем term; при равном term существующий `votedFor` сохраняется.
7. `leaderID` устанавливается из запроса.
8. Обновляется `lastLeaderContact` и сбрасывается election timer.
9. Возвращается `success=true` и актуальный term.

### 4.8. `internal/transport/http.go`

HTTP transport реализует `raft.PeerClient` и отдельный метод чтения статуса:

```go
func (c *Client) RequestVote(ctx context.Context, peer raft.Peer, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
func (c *Client) AppendEntries(ctx context.Context, peer raft.Peer, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
func (c *Client) Status(ctx context.Context, peer raft.Peer) (raft.Status, error)
```

Правила:

- один общий переиспользуемый `http.Client`;
- timeout задаётся контекстом каждого вызова;
- JSON body имеет ограниченный размер на стороне server;
- любой ответ вне `2xx` считается ошибкой transport;
- body всегда закрывается;
- проверяется корректность JSON и обязательных полей ответа;
- автоматические повторные попытки отсутствуют.

Transport не меняет состояние `Node`; он только возвращает результат вызывающему коду.

Агрегатор статусов зависит не от concrete transport, а от отдельного интерфейса рядом с потребителем в `httpapi`:

```go
type StatusClient interface {
    Status(ctx context.Context, peer raft.Peer) (raft.Status, error)
}
```

### 4.9. `internal/httpapi/server.go`

Маршруты одного listener:

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/health` | Liveness текущего процесса |
| `GET` | `/status` | Локальный snapshot Raft-узла |
| `GET` | `/cluster/status` | Агрегированный snapshot всех настроенных узлов |
| `POST` | `/raft/request-vote` | Внутренний `RequestVote` RPC |
| `POST` | `/raft/append-entries` | Внутренний heartbeat `AppendEntries` RPC |
| `GET` | `/` | Встроенная страница состояния |
| `GET` | `/assets/*` | Встроенные JS/CSS-файлы |

Общие правила API:

- `Content-Type: application/json` для JSON;
- неизвестный метод для известного пути — `405 Method Not Allowed`;
- невалидный JSON, неизвестные поля или более одного JSON-значения — `400 Bad Request`;
- `term` обязателен и должен быть больше `0`; `candidate_id`/`leader_id` обязательны, непусты и должны принадлежать фиксированному cluster membership;
- для heartbeat поле `entries` может отсутствовать, быть `null` или пустым массивом; непустой массив запрещён;
- ограничение body для внутренних RPC — 64 KiB;
- нормальные отрицательные Raft-решения, например отказ в голосе из-за term, возвращаются как `200 OK` с `vote_granted=false`, а не как HTTP-ошибка;
- структурная ошибка возвращает `400 Bad Request` до вызова доменного handler; неизвестный участник и неподдерживаемые log-поля возвращаются handler как типизированная validation error, отображаются в `400 Bad Request` и также не меняют Raft state;
- внутренние endpoints не получают аутентификацию, поскольку сервис предназначен только для локальной учебной демонстрации; README запрещает выставлять его в недоверенную сеть.

### 4.10. Форматы status API

`GET /health`:

```json
{
  "status": "ok",
  "node_id": "node1"
}
```

`GET /status`:

```json
{
  "observed_at": "2026-10-01T12:00:00Z",
  "node_id": "node1",
  "address": "http://127.0.0.1:8081",
  "role": "leader",
  "term": 4,
  "leader_id": "node1",
  "voted_for": "node1",
  "last_leader_contact": null,
  "peers": [
    {"node_id": "node1", "address": "http://127.0.0.1:8081"},
    {"node_id": "node2", "address": "http://127.0.0.1:8082"},
    {"node_id": "node3", "address": "http://127.0.0.1:8083"}
  ]
}
```

Пустые `leader_id`, `voted_for` и `last_leader_contact` кодируются как `null`, а не как пустые строки.

`GET /cluster/status`:

```json
{
  "observer_id": "node1",
  "observed_at": "2026-10-01T12:00:00Z",
  "nodes": [
    {
      "node_id": "node1",
      "address": "http://127.0.0.1:8081",
      "reachable": true,
      "status": {"role": "leader", "term": 4, "leader_id": "node1"},
      "error": null
    },
    {
      "node_id": "node2",
      "address": "http://127.0.0.1:8082",
      "reachable": false,
      "status": null,
      "error": "unreachable"
    }
  ]
}
```

Агрегатор всегда возвращает все настроенные узлы в порядке конфигурации. Ошибка одного узла не превращает весь ответ в `5xx`. Для недоступного узла наружу выдаётся стабильная категория `unreachable`, без внутренних сетевых подробностей.

### 4.11. `internal/httpapi/cluster.go`

Агрегатор:

1. Получает snapshot полного списка участников.
2. Для собственного ID использует `Node.Status()` без HTTP-loopback.
3. Для остальных участников параллельно вызывает transport `Status`.
4. На каждый запрос устанавливает `rpc-timeout`.
5. Записывает результат ровно в позицию исходного peer, сохраняя детерминированный порядок.
6. После завершения всех запросов формирует один ответ.

Количество горутин ограничено числом узлов фиксированного учебного кластера; отдельный worker pool не добавляется.

### 4.12. Frontend

`index.html`, `app.js` и `styles.css` встраиваются в бинарный файл через `go:embed`.

Страница:

- показывает ID узла-наблюдателя и время последнего обновления;
- отображает каждый настроенный узел отдельной карточкой или строкой;
- показывает address, reachability, role, term и известного leader;
- визуально выделяет leader;
- различает follower, candidate, leader и unreachable;
- обновляет данные раз в секунду через `fetch('/cluster/status')`;
- не удаляет предыдущие данные при краткой ошибке запроса, а показывает предупреждение о неудачном обновлении;
- вставляет динамические значения только через `textContent`.

Кнопки управления узлами, принудительные выборы, редактирование состава и хранение истории не добавляются.

### 4.13. `README.md`

README пишется на русском и содержит:

- назначение и осознанно ограниченный scope;
- предупреждение об отсутствии persistence и безопасности после restart;
- схему REST API;
- три готовые команды для запуска `node1`, `node2`, `node3` в разных терминалах;
- адрес интерфейса каждого узла;
- сценарий ручной проверки выбора лидера и failover;
- команды сборки и тестирования;
- список того, чего реализация не поддерживает.

Пример запуска, который должен поддерживаться итоговым CLI:

```bash
go run ./cmd/raft-node --id node1 --listen 127.0.0.1:8081 --peers node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082,node3=http://127.0.0.1:8083
go run ./cmd/raft-node --id node2 --listen 127.0.0.1:8082 --peers node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082,node3=http://127.0.0.1:8083
go run ./cmd/raft-node --id node3 --listen 127.0.0.1:8083 --peers node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082,node3=http://127.0.0.1:8083
```

## 5. Жизненный цикл и взаимодействие компонентов

### 5.1. Запуск

```text
main
  -> parse + validate config
  -> create HTTP client/transport
  -> create Raft Node as follower, term 0
  -> bind HTTP listener
  -> start HTTP serving
  -> start Node timer loop
  -> wait for signal or fatal server error
```

### 5.2. Успешные выборы

```text
election timeout
  -> follower becomes candidate
  -> term++ and self-vote
  -> parallel RequestVote RPCs
  -> majority for the same term
  -> candidate becomes leader
  -> immediate empty AppendEntries
  -> periodic empty AppendEntries
```

### 5.3. Failover

```text
leader process stops
  -> followers stop receiving heartbeat
  -> first randomized election timeout expires
  -> candidate requests votes
  -> majority of full configured cluster is still available
  -> new leader is elected in a higher term
  -> cluster UI marks stopped node unreachable
```

### 5.4. Наблюдение через UI

```text
browser GET /
  -> embedded HTML/JS/CSS
  -> every second GET /cluster/status
  -> serving node reads its local snapshot
  -> serving node queries peer /status endpoints in parallel
  -> browser renders one row/card per configured node
```

## 6. Подробная последовательность реализации `/release`

1. Создать `go.mod`, каркас каталогов и русскоязычный README с заявленными границами.
2. Реализовать и протестировать разбор/валидацию CLI-конфигурации.
3. Добавить Raft-типы, роли, immutable peer list и read-only status snapshot.
4. Реализовать базовый `Node`, единые переходы term/role и безопасный lifecycle `Start/Stop`.
5. Реализовать election timer с поколениями и инъекцией источника timeout для тестов.
6. Реализовать `RequestVote`: self-vote, один голос на term, majority и игнорирование устаревших ответов.
7. Реализовать переход в leader, немедленный и периодический heartbeat, а также step-down при большем term.
8. Реализовать HTTP transport без retry и проверить timeout/JSON/error handling.
9. Реализовать HTTP handlers и строгую проверку election-only RPC payload.
10. Реализовать параллельную server-side агрегацию `/cluster/status`.
11. Добавить встроенный frontend и polling состояния.
12. Добавить интеграционный тест реального трёхузлового HTTP-кластера: начальные выборы, стабильный лидер, остановка лидера, новый лидер.
13. Дополнить README фактическими командами запуска и ручной проверкой.
14. Выполнить форматирование, тесты, race detector, vet, сборку и ручной smoke test.

Каждый следующий шаг опирается на уже проверенный нижний слой; UI добавляется после стабилизации Raft и API, чтобы визуальная проверка не маскировала ошибки алгоритма.

## 7. Стратегия тестирования

### 7.1. Unit-тесты `internal/config`

- корректный разбор трёх узлов;
- отсутствие текущего узла в списке;
- повторяющийся ID или URL;
- некорректная временная конфигурация;
- некорректный peer URL.

### 7.2. Unit-тесты `internal/raft`

- начальное состояние follower/term 0;
- вычисление majority для кластеров разного размера;
- self-vote при старте выборов;
- не более одного голоса в одном term;
- повторный голос тому же candidate идемпотентен;
- отказ запросу с меньшим term;
- сброс голоса и переход в follower при большем term;
- переход candidate/leader в follower по допустимому heartbeat;
- сохранение `votedFor` при переходе candidate в follower по heartbeat того же term и последующий отказ другому candidate в этом term;
- отклонение vote/heartbeat от ID, отсутствующего в фиксированном составе, без изменения term и election timer;
- игнорирование устаревшего vote response;
- step-down при vote response или heartbeat response с большим term;
- немедленный переход single-node cluster в leader;
- старое поколение election timer не начинает выборы после reset;
- выданный голос откладывает начало собственных выборов;
- повторяющиеся heartbeat дольше `election-min` не изменяют term follower;
- отсутствие большинства не позволяет candidate стать leader;
- split vote заканчивается новым случайным timeout, следующим term и успешным новым раундом после появления большинства;
- heartbeat не выполняет сетевой вызов под state lock;
- остановка `Node` завершает фоновые goroutine.

Fake transport возвращает программируемые ответы и позволяет проверять порядок событий без реальных задержек.

### 7.3. Тесты HTTP transport/API

- корректные JSON-контракты `RequestVote` и `AppendEntries`;
- `400` для битого JSON, лишнего JSON, отсутствующих обязательных полей, неизвестного participant ID и неподдерживаемых log-полей;
- невалидный member RPC не меняет term, vote или election deadline;
- `405` для неверного метода;
- отрицательный vote как `200` с `vote_granted=false`;
- timeout и non-2xx peer response преобразуются в transport error;
- `/status` возвращает согласованный snapshot;
- `/cluster/status` сохраняет всех peer и помечает недоступного как `unreachable`;
- `/health` и `/` доступны после старта server.

### 7.4. Интеграционный тест

Тест заранее открывает три listener на `127.0.0.1:0`, формирует полный peer list и запускает три `Node` с реальным HTTP transport. Для предсказуемости узлам задаются контролируемые источники election timeout, при этом сами production defaults остаются случайными.

Проверки:

1. За ограниченное время появляется ровно один leader.
2. Term и leader ID согласуются у доступных узлов после нескольких heartbeat.
3. В течение нескольких heartbeat лидер не меняется самопроизвольно.
4. Test harness полностью останавливает leader: Raft loop, HTTP server и listener, затем ожидает завершения его фоновых goroutine.
5. Оставшиеся два узла выбирают ровно одного нового leader с большим term.
6. `/cluster/status` одного из живых узлов показывает остановленный узел как unreachable.

Ожидания реализуются polling с общим deadline, а не фиксированными длинными `Sleep`, чтобы тест был быстрым и менее чувствительным к скорости машины.

## 8. Проверки перед завершением `/release`

Автоматические:

```bash
gofmt -w ./cmd ./internal
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/raft-node
```

Ручной smoke test:

1. Запустить три команды из README в отдельных терминалах.
2. Открыть `http://127.0.0.1:8081/`.
3. Убедиться, что видны три reachable-узла и ровно один leader.
4. Сверить UI с `curl http://127.0.0.1:8081/cluster/status`.
5. Остановить текущего лидера через `Ctrl+C`.
6. Убедиться, что он стал unreachable, а два живых узла выбрали нового leader.
7. Проверить `GET /health` и `GET /status` на живом узле.

Ожидаемый результат: сборка и все проверки завершаются успешно, race detector не сообщает гонок, а ручной сценарий показывает выборы и failover.

## 9. Совместимость и ограничения

- Поддерживается локальный Go 1.26.1 на macOS; код использует только переносимую стандартную библиотеку и должен собираться на других ОС с Go 1.26.
- Адреса peer задаются заранее и не изменяются во время работы.
- Аутентификация и TLS termination не входят в scope; peer URL принимает только `http`.
- UI рассчитан на современный браузер с `fetch`.
- Статусы разных узлов собраны в близкие, но не одинаковые моменты времени.
- При потере большинства действующий leader продолжит считать себя leader, хотя новый leader в меньшинстве не будет выбран. Это нормальное поведение данного урезанного election-механизма; lease/read-safety не реализуются.
- Из-за отсутствия persistence гарантии Raft не распространяются на перезапуски процессов.

## 10. Риски и меры в рамках плана

| Риск | Решение в плане |
|---|---|
| Срабатывание старого election timer | Один владелец `time.Timer`, авторитетные deadline/generation и повторная проверка под lock |
| Устаревший RPC response меняет новую роль | Проверка term и текущей роли перед применением |
| Повторный учёт голоса | Учёт ответа по ID peer только один раз |
| Второй голос после same-term heartbeat | `votedFor` очищается только при строго большем term |
| Неизвестный узел сбрасывает timer | Проверка membership до изменения Raft state |
| Блокировка узла медленным peer | Сетевые вызовы вне lock, context timeout, параллельная рассылка |
| Неверное большинство | `floor(len(allPeers)/2)+1`, включая self и недоступных участников в размер кластера |
| Повторяющиеся split vote | Новый случайный timeout на каждый election round |
| Утечка goroutine при остановке | Общий stop context, cancellation и `WaitGroup` |
| UI скрывает упавший узел | Полный configured peer list и явный `reachable=false` |
| Ложное впечатление полной реализации Raft | Reject непустого журнала и явные ограничения в README/UI |
| Нестабильный интеграционный тест | Контролируемые timeout sources и polling с deadline |

## 11. Явно исключённый scope

В `/release` нельзя добавлять без отдельного согласования:

- запись `currentTerm` и `votedFor` на диск;
- Raft log и команды клиента;
- `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`;
- snapshot/install-snapshot;
- membership changes;
- leader lease, linearizable reads и client redirect;
- retry/backoff-слой поверх естественных повторных выборов и heartbeat;
- база данных, очередь, Docker, Kubernetes;
- авторизация, TLS-сертификаты, метрики, tracing;
- UI-управление кластером или история состояний.

## 12. Решения, принятые на этапе design

1. Один HTTP listener обслуживает RPC, status API и UI.
2. Конфигурация передаётся CLI-флагами, без config-файла.
3. Состояние защищает один mutex; сеть выполняется вне lock.
4. Stale timer блокируется поколением и deadline, stale RPC — сверкой term/role/election round.
5. RPC DTO сохраняют стандартные log-поля, но разрешены только нулевые значения.
6. Cluster status агрегируется сервером, а не браузером.
7. Frontend встраивается в Go-бинарник и не имеет внешнего toolchain.
8. Интеграционный тест использует реальные HTTP listener и управляемые election timeout.
9. README и пользовательская документация пишутся на русском.
10. По итогам независимой проверки `votedFor` сохраняется при same-term step-down, неизвестные ID не меняют состояние, timer имеет одного владельца и authoritative deadline, а election round — одного владельца набора голосов.

## 13. Условия готовности к `/release`

План готов к реализации, если пользователь согласен со следующими решениями:

- модуль `raftdemo` и указанная файловая структура;
- CLI-конфигурация фиксированного кластера;
- REST-контракты и server-side aggregation;
- стандартные RPC-поля с явным запретом log replication;
- mutex + generation/deadline-based timer model;
- русскоязычные README и пользовательские подписи UI;
- проверки `test`, `race`, `vet`, `build` и ручной трёхузловой smoke test.

После согласования реализация выполняется только на этапе `/release`, без commit, push, merge и deploy.
