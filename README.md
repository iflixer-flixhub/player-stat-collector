# player-stat-collector

Веб-сервер принимает запросы по http. Накапливает их. И периодически сбрасывает в БД пачкой. 


https://log.flixcdn.space/log?event=play&domain=piratka.biz&file_id=109454


curl -i "http://localhost:34201/healthz"
curl -i "http://localhost:34201/readyz"
curl -i "http://localhost:34201/log?event=play&domain=piratka.biz&file_id=109454"
curl -i "http://localhost:34201/metrics"
curl -s "http://localhost:34201/metrics" | grep -E "wal|flushed|dropped|queue"

https://ingest.player-stat-collector.orb.local/healthz
https://ingest.player-stat-collector.orb.local/readyz
https://ingest.player-stat-collector.orb.local/log?event=play&domain=piratka.biz&file_id=109454
https://ingest.player-stat-collector.orb.local/metrics

https://ingest.player-stat-collector.orb.local/debug/domain-cache


Важно: WAL обеспечивает at-least-once. Если AdvanceCommit не успеет записать commit.meta после успешного INSERT, при рестарте возможны дубли. Если дубли неприемлемы — делаем дедуп (например, добавляем event_uuid и уникальный индекс, или генерим hash и уникальный key). Для логов обычно ок.


	•	MySQL упал на 10 минут? События продолжают писаться в WAL, в RAM можно даже не помещаться.
	•	После восстановления MySQL: сервис догонит по WAL (если перезапустился) и продолжит нормальный флаш.
	•	/stats даёт быстрый взгляд: очередь, размер WAL, commit-позиция.
	•	Prometheus видит WAL метрики (ingest_wal_size_bytes, ingest_wal_segments, ingest_wal_replay_total).


    •	На каждый валидный запрос:
	•	пишем запись в WAL (append),
	•	кладём в очередь в памяти.
	•	Флашер батчит в MySQL.
	•	После успешного INSERT: помечаем N записей “committed” в отдельном файле commit.meta.
	•	При старте сервиса: replay WAL начиная с committed-указателя и догружаем очередь.
	•	Периодически: компактим WAL (удаляем полностью подтверждённые сегменты).

Я делаю WAL сегментами (wal/000001.log, wal/000002.log…), каждая строка — JSON события. commit.meta хранит: seg и line (номер строки в сегменте, до которой всё записано в MySQL)


Сценарий "отвал БД"
	•	запросы продолжают приходить
	•	handler пишет WAL
	•	flusher копит buf и перестаёт читать очередь (blocked)
	•	очередь забивается → handler начинает отвечать wal only
	•	tailer тоже видит, что очередь забита и не читает WAL дальше (readPos не уезжает вперёд)
	•	включаешь БД:
	•	flusher успешно пишет buf → разблокируется
	•	очередь начинает освобождаться
	•	tailer начинает дочитывать WAL хвост и подбрасывать в очередь
	•	всё доезжает в MySQL без рестартов