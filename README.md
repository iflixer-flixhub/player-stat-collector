# player-stat-collector

Веб-сервер принимает запросы по HTTP, складывает события в очередь и периодически сбрасывает их в БД пачками.

## Быстрые примеры

Публичный пример:

https://log.flixcdn.space/log?event=play&domain=piratka.biz&file_id=109454

Локально:

```bash
curl -i "http://localhost:34201/healthz"
curl -i "http://localhost:34201/readyz"
curl -i "http://localhost:34201/log?event=play&domain=piratka.biz&file_id=109454"
curl -i "http://localhost:34201/e/play?domain=piratka.biz&file_id=109454"
curl -i "http://localhost:34201/metrics/w8Z"
curl -i "http://localhost:34201/debug/health"
curl -s "http://localhost:34201/metrics/w8Z" | grep -E "wal|flushed|dropped|queue"
```

Внутренние URL:

- https://ingest.player-stat-collector.orb.local/healthz
- https://ingest.player-stat-collector.orb.local/readyz
- https://ingest.player-stat-collector.orb.local/log?event=play&domain=piratka.biz&file_id=109454
- https://ingest.player-stat-collector.orb.local/metrics/w8Z
- https://ingest.player-stat-collector.orb.local/debug/health
- https://ingest.player-stat-collector.orb.local/debug/domain-cache

## Гарантии WAL

Важно: WAL обеспечивает at-least-once. Если `AdvanceCommit` не успеет записать `commit.meta` после успешного INSERT, при рестарте возможны дубли. Если дубли неприемлемы — добавляйте дедуп (например, `event_uuid` + unique index, или hash + unique key). Для логов обычно это допустимо.

- Если MySQL недоступен, события продолжают писаться в WAL.
- После восстановления MySQL сервис догоняет отставание.
- Prometheus видит WAL-метрики: `ingest_wal_size_bytes`, `ingest_wal_segments`, `ingest_wal_replay_total`.

## Как работает пайплайн

На каждый валидный запрос:

- пишем запись в WAL (append);
- кладём событие в очередь в памяти;
- flusher батчит и пишет в MySQL;
- после успешного INSERT помечаем N записей committed в `commit.meta`;
- при старте сервиса replay WAL идёт от commit-позиции;
- периодически выполняется compact WAL (удаляются полностью подтверждённые сегменты).

WAL хранится сегментами (`wal/000001.log`, `wal/000002.log`, ...), каждая строка — JSON-событие. В `commit.meta` хранятся `seg` и `line` (до какой строки в сегменте данные уже подтверждены в MySQL).

## Сценарий отказа БД

- запросы продолжают приходить;
- handler пишет в WAL;
- flusher накапливает `buf` и перестаёт читать очередь (`blocked`);
- очередь заполняется, tailer перестаёт продвигать `readPos`;
- после восстановления БД flusher разгружает буфер и снимает блокировку;
- tailer дочитывает хвост WAL;
- данные доезжают в MySQL без рестарта.

## Debug health

Endpoint: `/debug/health`

- `200` — WAL и MySQL доступны;
- `503` — есть ошибка WAL или MySQL (`wal_error`/`mysql_error` в JSON).

Ключевые поля для диагностики:

- `queue_len` / `queue_cap` — заполнение очереди;
- `wal.commit` и `wal.read_pos` — отставание чтения и коммита;
- `wal.segments` и `wal.size_bytes` — рост WAL на диске;
- `domain_count` / `country_count` — актуальность кэшей.

Пример запроса:

```bash
curl -s "http://localhost:34201/debug/health" | jq
```

Пример ответа:

```json
{
	"time": "2026-03-04T10:40:00Z",
	"queue_len": 0,
	"queue_cap": 50000,
	"domain_count": 160,
	"country_count": 244,
	"wal": {
		"segments": 2,
		"size_bytes": 131072,
		"commit": {
			"seg": 12,
			"line": 310
		},
		"read_pos": {
			"seg": 12,
			"line": 310
		}
	},
	"mysql_ok": true
}
```

## Runbook: /debug/health = 503

Порядок проверки (сверху вниз):

1. Проверить тело ответа `/debug/health` и поля `wal_error` / `mysql_error`.
2. Если `mysql_ok=false`:
	- проверить доступность MySQL из контейнера (`ping`, сеть, DNS, firewall);
	- проверить лимиты соединений и ошибки авторизации;
	- после восстановления убедиться, что `queue_len` и `wal.size_bytes` начинают снижаться.
3. Если есть `wal_error`:
	- проверить права/место на диске для `WAL_DIR`;
	- проверить, что каталог WAL смонтирован и доступен на запись;
	- проверить, не повреждены ли сегменты (`*.log`) и `commit.meta`.
4. Оценить степень отставания:
	- большой разрыв `wal.read_pos` vs `wal.commit` = БД/флашер не успевают;
	- рост `wal.segments` и `wal.size_bytes` = накопление хвоста.
5. Проверить очередь:
	- `queue_len` близко к `queue_cap` = backpressure, обработка входящих скоро упрётся;
	- после восстановления БД очередь должна постепенно разгружаться.

Критерий нормализации:

- `/debug/health` возвращает `200`;
- `mysql_ok=true`, `wal_error` отсутствует;
- `queue_len` стабильно низкий;
- `wal.size_bytes` и число сегментов перестают расти и со временем уменьшаются.

## One-liners для дежурного

Быстрый статус (одной строкой):

```bash
curl -s "http://localhost:34201/debug/health" | jq -r '"status=\(.mysql_ok) queue=\(.queue_len)/\(.queue_cap) wal=\(.wal.segments)seg \(.wal.size_bytes)B commit=\(.wal.commit.seg):\(.wal.commit.line) read=\(.wal.read_pos.seg):\(.wal.read_pos.line)"'
```

Только причина 503 (если есть):

```bash
curl -s "http://localhost:34201/debug/health" | jq -r '.mysql_error // .wal_error // "ok"'
```

Проверка отставания tail/commit:

```bash
curl -s "http://localhost:34201/debug/health" | jq -r '"commit=\(.wal.commit.seg):\(.wal.commit.line) read=\(.wal.read_pos.seg):\(.wal.read_pos.line)"'
```

Наблюдение в реальном времени (каждые 2 сек):

```bash
watch -n 2 'curl -s "http://localhost:34201/debug/health" | jq "{mysql_ok, queue_len, queue_cap, wal_segments: .wal.segments, wal_size_bytes: .wal.size_bytes, commit: .wal.commit, read_pos: .wal.read_pos, mysql_error, wal_error}"'
```

Простой алерт-код для скриптов (0=ok, 1=problem):

```bash
curl -sf "http://localhost:34201/debug/health" >/dev/null && echo "OK" || echo "PROBLEM"
```