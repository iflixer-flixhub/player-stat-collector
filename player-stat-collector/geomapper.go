package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

type Country struct {
	ID      int
	Iso2    string
	GroupID int
}

type GeoMapper struct {
	db    *sql.DB
	every time.Duration

	mu  sync.RWMutex
	iso map[IsoCode]Country
}

type IsoCode string

func NewGeoMapper(db *sql.DB, every time.Duration) *GeoMapper {
	return &GeoMapper{db: db, every: every, iso: make(map[IsoCode]Country, 512)}
}

func (g *GeoMapper) Run(ctx context.Context) {
	g.reload(ctx)
	t := time.NewTicker(g.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.reload(ctx)
		}
	}
}

func (g *GeoMapper) Snapshot() []Country {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make([]Country, 0, len(g.iso))
	for _, row := range g.iso {
		out = append(out, Country{
			ID:      row.ID,
			Iso2:    row.Iso2,
			GroupID: row.GroupID,
		})
	}
	return out
}

func (g *GeoMapper) Get(iso2 string) *Country {
	isoCode := IsoCode(strings.ToUpper(strings.TrimSpace(iso2)))
	if len(isoCode) != 2 {
		return nil
	}
	g.mu.RLock()
	v, ok := g.iso[isoCode]
	if !ok {
		return nil
	}
	g.mu.RUnlock()
	return &v
}

func (g *GeoMapper) reload(ctx context.Context) {
	ctxTO, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := g.db.QueryContext(ctxTO, `SELECT id, iso_code, COALESCE(geo_group_id, 0) AS geo_group_id FROM countries_iso`)
	if err != nil {
		log.Printf("geo reload failed: %v", err)
		return
	}
	defer rows.Close()

	tmp := make(map[IsoCode]Country, 512)
	for rows.Next() {
		var iso2 string
		var id, gid int
		if err := rows.Scan(&id, &iso2, &gid); err == nil {
			isoString := IsoCode(strings.ToUpper(strings.TrimSpace(iso2)))
			country := Country{
				ID:      id,
				Iso2:    iso2,
				GroupID: gid,
			}
			tmp[isoString] = country
		} else {
			log.Printf("geo reload row error: %v", err)
		}
	}
	g.mu.Lock()
	g.iso = tmp
	g.mu.Unlock()
	log.Printf("geo reloaded: %d", len(tmp))
}
