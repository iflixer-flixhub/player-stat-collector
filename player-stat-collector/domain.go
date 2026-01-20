package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type DomainRow struct {
	ID           int
	Domain       string
	ParentID     int
	DomainTypeID int
}

type DomainCache struct {
	db    *sql.DB
	every time.Duration
	mu    sync.RWMutex
	m     map[string]DomainRow
}

func NewDomainCache(db *sql.DB, every time.Duration) *DomainCache {
	return &DomainCache{db: db, every: every, m: make(map[string]DomainRow, 1024)}
}

func (c *DomainCache) Run(ctx context.Context) {
	c.reload(ctx)
	t := time.NewTicker(c.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.reload(ctx)
		}
	}
}

func (c *DomainCache) Snapshot() []DomainRow {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]DomainRow, 0, len(c.m))
	for domain, row := range c.m {
		out = append(out, DomainRow{
			Domain:       domain,
			ID:           row.ID,
			ParentID:     row.ParentID,
			DomainTypeID: row.DomainTypeID,
		})
	}

	// (не обязательно) можно отсортировать, чтобы diff в дебаге был стабильный
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })

	return out
}

func (c *DomainCache) Get(ctx context.Context, name string) (row DomainRow, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name = strings.ToLower(strings.TrimSpace(name))
	if row, ok := c.m[name]; ok {
		return row, nil
	}
	return row, errors.New("domain not found")
}

func (c *DomainCache) reload(ctx context.Context) {
	ctxTO, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := c.db.QueryContext(ctxTO, `SELECT id, name, id_parent, COALESCE(domain_type_id, 0) FROM domains`)
	if err != nil {
		log.Printf("domain reload failed: %v", err)
		return
	}
	defer rows.Close()

	tmp := make(map[string]DomainRow, 1024)
	for rows.Next() {
		var name string
		var id, typeId, parentId int
		if err := rows.Scan(&id, &name, &parentId, &typeId); err == nil {
			host := strings.ToLower(strings.TrimSpace(name))
			domainRow := DomainRow{
				ID:           id,
				Domain:       host,
				DomainTypeID: typeId,
				ParentID:     parentId,
			}
			tmp[host] = domainRow
		} else {
			log.Printf("domain reload row error: %v", err)
		}
	}
	c.mu.Lock()
	c.m = tmp
	c.mu.Unlock()
	log.Printf("domain reloaded: %d", len(tmp))
}
