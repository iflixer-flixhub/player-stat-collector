package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"time"
)

type WALTailer struct {
	wal  *WAL
	file *os.File
	rd   *bufio.Reader

	seg  int
	line int

	// pending: событие уже прочитано из WAL, но не получилось отправить в очередь
	hasPending bool
	pending    Event
}

func NewWALTailer(w *WAL) *WALTailer {
	w.readMu.Lock()
	pos := w.readPos
	w.readMu.Unlock()

	return &WALTailer{
		wal:  w,
		seg:  pos.Seg,
		line: pos.Line,
	}
}

func (t *WALTailer) close() {
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
		t.rd = nil
	}
}

func (t *WALTailer) openIfNeeded() error {
	if t.file != nil {
		return nil
	}
	path := t.wal.segPath(t.seg)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // сегмента ещё нет
		}
		return err
	}
	t.file = f
	t.rd = bufio.NewReaderSize(f, 256*1024)

	// skip already-read lines
	for i := 0; i < t.line; i++ {
		_, err := t.rd.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			t.close()
			return err
		}
	}
	return nil
}

func (t *WALTailer) tryAdvanceToNextSeg() bool {
	nextPath := t.wal.segPath(t.seg + 1)
	if _, err := os.Stat(nextPath); err == nil {
		t.close()
		t.seg++
		t.line = 0
		t.wal.readMu.Lock()
		t.wal.readPos = CommitPos{Seg: t.seg, Line: t.line}
		t.wal.readMu.Unlock()
		return true
	}
	return false
}

// pushPending пытается отправить pending в очередь.
// Возвращает true если pending отправлен (или его не было), false если очередь всё ещё полная.
func (t *WALTailer) pushPending(out chan<- Event) bool {
	if !t.hasPending {
		return true
	}
	select {
	case out <- t.pending:
		t.hasPending = false
		return true
	default:
		return false
	}
}

// readNextRawLine читает следующую строку из текущего сегмента.
// Возвращает (lineBytes, ok, err):
// - ok=false => сейчас нечего читать (EOF без следующего сегмента / сегмента нет)
// - err!=nil => ошибка чтения
func (t *WALTailer) readNextRawLine() ([]byte, bool, error) {
	if err := t.openIfNeeded(); err != nil {
		return nil, false, err
	}
	if t.file == nil {
		return nil, false, nil
	}

	lineBytes, err := t.rd.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Если есть следующий сегмент — переключимся и продолжим
			if t.tryAdvanceToNextSeg() {
				return nil, true, nil // ok=true: "продвинулись", можно пробовать читать дальше
			}
			return nil, false, nil
		}
		t.close()
		return nil, false, err
	}
	return lineBytes, true, nil
}

// readOneAndQueue читает одну строку из WAL и пытается поставить в очередь.
// Строго без потерь: если очередь полная — кладёт в pending и вернёт false.
func (t *WALTailer) readOneAndQueue(out chan<- Event) bool {
	// сначала пробуем отправить pending
	if !t.pushPending(out) {
		return false
	}

	raw, ok, err := t.readNextRawLine()
	if err != nil {
		log.Printf("wal tailer read error: %v", err)
		return false
	}
	if !ok {
		return false // нечего читать сейчас
	}
	if raw == nil {
		// это кейс "перешли на следующий сегмент" — продолжим
		return true
	}

	trim := bytes.TrimSpace(raw)
	if len(trim) == 0 {
		// пустая строка: считаем как прочитанную
		t.line++
		t.wal.readMu.Lock()
		t.wal.readPos = CommitPos{Seg: t.seg, Line: t.line}
		t.wal.readMu.Unlock()
		return true
	}

	var ev Event
	if err := json.Unmarshal(trim, &ev); err != nil {
		// битая строка: пропускаем, но позицию двигаем
		t.line++
		t.wal.readMu.Lock()
		t.wal.readPos = CommitPos{Seg: t.seg, Line: t.line}
		t.wal.readMu.Unlock()
		return true
	}

	// попытка отправки (если не вышло — pending)
	select {
	case out <- ev:
		t.line++
		t.wal.readMu.Lock()
		t.wal.readPos = CommitPos{Seg: t.seg, Line: t.line}
		t.wal.readMu.Unlock()
		return true
	default:
		// очередь полная — не теряем событие
		t.hasPending = true
		t.pending = ev
		return false
	}
}

func (t *WALTailer) drain(out chan<- Event) {
	for {
		// если очередь заполнена — даже pending не отправится
		if len(out) >= cap(out) && !t.hasPending {
			return
		}
		if ok := t.readOneAndQueue(out); !ok {
			return
		}
	}
}

func (t *WALTailer) Run(ctx context.Context, out chan<- Event, notify <-chan struct{}) {
	defer t.close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
			t.drain(out)
		case <-time.After(1 * time.Second):
			// страховка: если notify потерялся
			t.drain(out)
		}
	}
}
