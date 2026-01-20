package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* ---------------- WAL ---------------- */

// commit.meta содержит: {"seg":1,"line":1234}
// seg - номер сегмента, line - сколько строк в этом сегменте уже подтверждено (committed)
type CommitPos struct {
	Seg  int `json:"seg"`
	Line int `json:"line"`
}

type AppendPos struct {
	Seg  int
	Line int
}

type WAL struct {
	dir         string
	segMaxBytes int64
	fsyncEvery  time.Duration

	mu sync.Mutex

	curLine int // сколько строк уже есть в текущем сегменте (1-based)

	// current segment
	curSeg    int
	curFile   *os.File
	curSize   int64
	lastFsync time.Time

	commit CommitPos

	readMu  sync.Mutex
	readPos CommitPos // НЕ сохраняем на диск, только для текущего процесса
}

func NewWAL(dir string, segMaxMB int, fsyncEvery time.Duration) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{
		dir:         dir,
		segMaxBytes: int64(segMaxMB) * 1024 * 1024,
		fsyncEvery:  fsyncEvery,
	}
	if err := w.loadCommit(); err != nil {
		return nil, err
	}
	w.readPos = w.commit
	if err := w.openOrCreateTail(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) commitPath() string {
	return filepath.Join(w.dir, "commit.meta")
}

func (w *WAL) loadCommit() error {
	b, err := os.ReadFile(w.commitPath())
	if err != nil {
		if os.IsNotExist(err) {
			w.commit = CommitPos{Seg: 1, Line: 0}
			return nil
		}
		return err
	}
	var cp CommitPos
	if err := json.Unmarshal(b, &cp); err != nil {
		return err
	}
	if cp.Seg <= 0 {
		cp.Seg = 1
	}
	if cp.Line < 0 {
		cp.Line = 0
	}
	w.commit = cp
	return nil
}

func (w *WAL) saveCommitLocked() error {
	b, _ := json.Marshal(w.commit)
	tmp := w.commitPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.commitPath())
}

func (w *WAL) segPath(seg int) string {
	return filepath.Join(w.dir, fmt.Sprintf("%06d.log", seg))
}

func (w *WAL) listSegs() ([]int, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var segs []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base := strings.TrimSuffix(name, ".log")
		n, err := strconv.Atoi(base)
		if err == nil {
			segs = append(segs, n)
		}
	}
	sort.Ints(segs)
	return segs, nil
}

func (w *WAL) openOrCreateTail() error {
	segs, err := w.listSegs()
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		w.curSeg = 1
		return w.openSeg(w.curSeg, true)
	}
	w.curSeg = segs[len(segs)-1]
	return w.openSeg(w.curSeg, false)
}

func (w *WAL) openSeg(seg int, create bool) error {
	path := w.segPath(seg)
	var f *os.File
	var err error
	log.Printf("WAL: openSeg %s %t", path, create)
	if create {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	} else {
		f, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
		if os.IsNotExist(err) {
			f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		}
	}
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	// считаем сколько строк уже в файле (для корректной позиции Append)
	lines, err := countLines(path)
	if err != nil {
		_ = f.Close()
		return err
	}

	if w.curFile != nil {
		_ = w.curFile.Close()
	}
	w.curFile = f
	w.curSize = st.Size()
	w.curSeg = seg
	w.curLine = lines
	w.lastFsync = time.Now()
	return nil
}

func (w *WAL) Append(ev Event) (AppendPos, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// если commit ушёл вперёд — пишем уже в новом сегменте
	if w.curSeg < w.commit.Seg {
		if err := w.openSeg(w.commit.Seg, true); err != nil {
			return AppendPos{}, err
		}
	}

	// rotate by size
	if w.segMaxBytes > 0 && w.curSize >= w.segMaxBytes {
		if err := w.openSeg(w.curSeg+1, true); err != nil {
			return AppendPos{}, err
		}
	}

	b, err := json.Marshal(ev)
	if err != nil {
		return AppendPos{}, err
	}
	b = append(b, '\n')

	n, err := w.curFile.Write(b)
	if err != nil {
		return AppendPos{}, err
	}
	w.curSize += int64(n)
	w.curLine++ // ВАЖНО: увеличиваем счётчик строк

	if time.Since(w.lastFsync) >= w.fsyncEvery {
		if err := w.curFile.Sync(); err != nil {
			return AppendPos{}, err
		}
		w.lastFsync = time.Now()
	}

	return AppendPos{Seg: w.curSeg, Line: w.curLine}, nil
}

func (w *WAL) MarkRead(pos AppendPos) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	if pos.Seg > w.readPos.Seg || (pos.Seg == w.readPos.Seg && pos.Line > w.readPos.Line) {
		w.readPos = CommitPos{Seg: pos.Seg, Line: pos.Line}
	}
}

// AdvanceCommit advances commit position by n events, relative to current commit pointer (seg,line).
// Поскольку commit хранится “по строкам в сегменте”, нам нужно уметь “перескакивать” на следующий сегмент.
func (w *WAL) AdvanceCommit(n int) error {
	if n <= 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	log.Printf("WAL: AdvanceCommit, n=%d, commit=%+v", n, w.commit)

	// Идём по сегментам, считая строки (лениво: читаем файл и считаем строки по необходимости).
	for n > 0 {
		path := w.segPath(w.commit.Seg)
		lines, err := countLines(path)
		if err != nil {
			if os.IsNotExist(err) {
				// сегмента ещё нет — commit указывает на “начало будущего сегмента”, это ок
				return w.saveCommitLocked()
			}
			return err
		}
		remainingInSeg := lines - w.commit.Line
		if remainingInSeg <= 0 {
			// сегмент целиком уже подтверждён — двигаемся дальше
			w.commit.Seg++
			w.commit.Line = 0
			continue
		}
		if n < remainingInSeg {
			w.commit.Line += n
			n = 0
		} else {
			// подтверждаем до конца сегмента
			w.commit.Line += remainingInSeg
			n -= remainingInSeg
			// переходим на следующий сегмент
			w.commit.Seg++
			w.commit.Line = 0
		}
	}

	return w.saveCommitLocked()
}

func (w *WAL) Compact() error {
	// держим w.mu на весь цикл, чтобы не пересечься с AdvanceCommit/Append
	w.mu.Lock()
	defer w.mu.Unlock()

	commit := w.commit
	cur := w.curSeg

	w.readMu.Lock()
	read := w.readPos
	w.readMu.Unlock()

	limit := commit.Seg
	if cur < limit {
		limit = cur
	}
	if read.Seg < limit {
		limit = read.Seg
	}

	segs, err := w.listSegs()
	if err != nil {
		return err
	}

	for _, seg := range segs {
		if seg < limit {
			_ = os.Remove(w.segPath(seg))
		}
	}
	return nil
}

func (w *WAL) Stats() (CommitPos, int, int64, error) {
	segs, err := w.listSegs()
	if err != nil {
		return CommitPos{}, 0, 0, err
	}
	var total int64
	for _, seg := range segs {
		st, err := os.Stat(w.segPath(seg))
		if err == nil {
			total += st.Size()
		}
	}
	w.mu.Lock()
	cp := w.commit
	w.mu.Unlock()
	return cp, len(segs), total, nil
}
