package main

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

/* ---------------- event ---------------- */

type Event struct {
	TS           time.Time `json:"ts"`
	UserID       int       `json:"user_id"`
	DomainID     int       `json:"domain_id"`
	GeoID        int       `json:"geo_id"`
	GeoGroupID   int       `json:"geo_group_id"`
	DomainTypeID int       `json:"domain_type_id"`
	VisitorIP    []byte    `json:"visitor_ip"` // 16 bytes
	FileID       int       `json:"file_id"`
	EventName    string    `json:"event"`
}

var allowedEvents = map[string]struct{}{
	"load": {}, "play": {}, "pay": {}, "vast_complete": {},
	"p25": {}, "p50": {}, "p75": {}, "p100": {},
	"getads": {}, "impression": {}, "p1": {}, "fallback": {}, "loaderror": {},
}

func buildEvent(r *http.Request, dc *DomainCache, geo *GeoMapper) (Event, error) {
	q := r.URL.Query()

	ev := q.Get("event")
	if _, ok := allowedEvents[ev]; !ok {
		return Event{}, errors.New("no event param")
	}

	domainName := strings.ToLower(strings.TrimSpace(q.Get("domain")))

	// если в рефе есть домен - используется он
	// refDomain := refererDomain(r)
	// if refDomain != "" {
	// 	domainName = refDomain
	// }

	// // если в ORIGIN есть домен - используется он. высший приоритет
	// origDomain := originDomain(r)
	// if origDomain != "" {
	// 	domainName = origDomain
	// }

	if domainName == "" {
		return Event{}, errors.New("no domain param")
	}

	fileID, err := strconv.Atoi(q.Get("file_id"))
	if err != nil || fileID <= 0 {
		return Event{}, errors.New("no file_id param")
	}

	drow, err := dc.Get(r.Context(), domainName)
	if err != nil {
		return Event{}, errors.New("domain not found (" + domainName + "):" + err.Error())
	}

	ip16 := clientIP16(r)
	iso2 := r.Header.Get("CF-IPCountry")
	forceCountry := q.Get("force_country")
	if forceCountry != "" {
		iso2 = forceCountry
	}

	event := Event{
		TS:           time.Now().UTC(),
		UserID:       drow.ParentID,
		DomainID:     drow.ID,
		DomainTypeID: drow.DomainTypeID,
		VisitorIP:    ip16,
		FileID:       fileID,
		EventName:    ev,
	}

	country := geo.Get(iso2)
	if country != nil {
		event.GeoID = country.ID
		event.GeoGroupID = country.GroupID
	}

	mPlayerEvent.WithLabelValues(ev).Inc()

	return event, nil
}
