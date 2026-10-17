package sessions

import (
	"context"
	"encoding/json/jsontext"
	"math"
	"net/url"
	"strconv"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

type EventQuery struct {
	After  uint64
	Floor  uint64
	Limit  int
	Filter contract.EventFilter
}

type EventPage struct {
	Items      []jsontext.Value `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
	ResumeFrom string           `json:"resume_from"`
}

func ParseEventQuery(q url.Values) (EventQuery, error) {
	query := EventQuery{Limit: 200}
	var err error
	query.After, err = contract.ParseEventCursor(q.Get("after"))
	if err != nil {
		return query, httpx.E(400, "INVALID_CURSOR", "Invalid event cursor")
	}
	if value := q.Get("limit"); value != "" {
		query.Limit, err = strconv.Atoi(value)
		if err != nil || query.Limit < 1 || query.Limit > 500 {
			return query, httpx.E(400, "INVALID_LIMIT", "limit must be between 1 and 500")
		}
	}
	query.Filter, err = contract.ParseEventFilter(q)
	if err != nil {
		return query, httpx.E(422, "INVALID_FILTER", "Invalid event filter")
	}
	return query, nil
}

// Events reads only the sink projection. The handler authorizes the session first.
func Events(ctx context.Context, session, run string, q EventQuery) (EventPage, error) {
	page := EventPage{Items: []jsontext.Value{}, ResumeFrom: strconv.FormatUint(max(q.After, q.Floor), 10)}
	if q.After > math.MaxInt64 {
		return page, nil
	}
	if q.Floor == 0 {
		return page, nil
	}
	var last uint64
	rows, err := db.Tx(ctx).Query(ctx, `SELECT stream_seq,payload FROM run_events
 WHERE tenant_id=$1 AND session_id=$2 AND ($3='' OR run_id=NULLIF($3,'')::uuid)
 AND stream_seq>$4::bigint AND stream_seq<=$8::bigint
 AND (cardinality($5::text[])=0 OR type=ANY($5)) AND NOT(type=ANY($6::text[]))
 ORDER BY stream_seq LIMIT $7`, auth.Tenant(ctx), session, run, int64(q.After),
		q.Filter.Types, q.Filter.Exclude, q.Limit+1, int64(min(q.Floor, math.MaxInt64)))
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq uint64
		var raw []byte
		if err := rows.Scan(&seq, &raw); err != nil {
			return page, err
		}
		if len(page.Items) == q.Limit {
			page.NextCursor = strconv.FormatUint(last, 10)
			page.ResumeFrom = page.NextCursor
			break
		}
		page.Items = append(page.Items, jsontext.Value(raw))
		last = seq
	}
	return page, rows.Err()
}
