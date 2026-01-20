package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"
)

/* ---------------- flusher ---------------- */

func insertBatch(ctx context.Context, db *sql.DB, batch []Event) error {
	sb := strings.Builder{}
	sb.WriteString(`INSERT INTO player_pay_log
(created_at,user_id,domain_id,geo_group_id,domain_type_id,visitor_ip,file_id,event) VALUES `)

	args := make([]any, 0, len(batch)*8)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,?,?)")
		args = append(args, e.TS, e.UserID, e.DomainID, e.GeoGroupID, e.DomainTypeID, e.VisitorIP, e.FileID, e.EventName)
	}

	ctxTO, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctxTO, sb.String(), args...)
	return err
}

func flusher(ctx context.Context, db *sql.DB, wal *WAL, ch <-chan Event, flushEvery time.Duration, batchMax int) {
	t := time.NewTicker(flushEvery)
	defer t.Stop()

	buf := make([]Event, 0, batchMax)
	blocked := false // если true — не читаем из ch, пока не запишем buf

	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		mBufLen.Set(float64(len(buf)))

		backoff := 300 * time.Millisecond
		for attempt := 1; attempt <= 10; attempt++ {
			if err := insertBatch(ctx, db, buf); err != nil {
				mFlushErr.Inc()
				log.Printf("flush failed attempt=%d err=%v", attempt, err)
				select {
				case <-time.After(backoff):
					backoff *= 2
					continue
				case <-ctx.Done():
					return false
				}
			}

			// успех: двигаем commit на len(buf)
			if err := wal.AdvanceCommit(len(buf)); err != nil {
				log.Printf("wal commit advance failed: %v", err)
			}

			mFlushed.Add(float64(len(buf)))
			buf = buf[:0]
			mBufLen.Set(0)
			return true
		}

		// ВАЖНО: НЕ ДРОПАЕМ buf!
		// Просто остаёмся blocked и будем пытаться позже.
		log.Printf("flush still failing; keep %d events in memory; WAL already has them", len(buf))
		return false
	}

	for {
		// если blocked=true, отключаем приём новых событий
		var inCh <-chan Event
		if !blocked {
			inCh = ch
		} else {
			inCh = nil
		}

		select {
		case <-ctx.Done():
			_ = flush()
			return

		case <-t.C:
			ok := flush()
			blocked = !ok

		case e := <-inCh:
			buf = append(buf, e)
			mQueueLen.Set(float64(len(ch)))
			mBufLen.Set(float64(len(buf)))
			if len(buf) >= batchMax {
				ok := flush()
				blocked = !ok
			}
		}
	}
}
