package api

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func statsTestContext(query string) *gin.Context {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/x?"+query, nil)
	return c
}

// TestForEachStatsDayWindow pins the window semantics: visit runs once per day
// from start_date onward, in order, with matching time.Time and ISO strings.
func TestForEachStatsDayWindow(t *testing.T) {
	s := &Server{}
	var visited []string
	days, missing, ok := s.forEachStatsDay(
		statsTestContext("start_date=260801&days=3"),
		func(date time.Time, day string) error {
			if date.Format("2006-01-02") != day {
				t.Fatalf("date/day mismatch: %v vs %s", date, day)
			}
			visited = append(visited, day)
			return nil
		})
	if !ok || days != 3 || len(missing) != 0 {
		t.Fatalf("ok=%v days=%d missing=%v, want ok 3 []", ok, days, missing)
	}
	want := []string{"2026-08-01", "2026-08-02", "2026-08-03"}
	for i, w := range want {
		if visited[i] != w {
			t.Fatalf("visited=%v, want %v", visited, want)
		}
	}
}

// TestForEachStatsDayCap clamps days to [1, maxDailyStatsDays] so one request
// cannot fan out over an unbounded number of per-day aggregate queries.
func TestForEachStatsDayCap(t *testing.T) {
	s := &Server{}
	count := 0
	days, _, ok := s.forEachStatsDay(
		statsTestContext("start_date=260101&days=999"),
		func(time.Time, string) error { count++; return nil })
	if !ok || days != maxDailyStatsDays || count != maxDailyStatsDays {
		t.Fatalf("days=%d count=%d, want both %d", days, count, maxDailyStatsDays)
	}

	count = 0
	days, _, ok = s.forEachStatsDay(
		statsTestContext("start_date=260101&days=-5"),
		func(time.Time, string) error { count++; return nil })
	if !ok || days != 1 || count != 1 {
		t.Fatalf("days=%d count=%d, want both 1", days, count)
	}
}

// TestForEachStatsDayMissingTable pins the retention-window behavior: MySQL
// error 1146 (table doesn't exist) marks the day missing and keeps going;
// any other error aborts the whole request.
func TestForEachStatsDayMissingTable(t *testing.T) {
	s := &Server{}
	days, missing, ok := s.forEachStatsDay(
		statsTestContext("start_date=260801&days=3"),
		func(_ time.Time, day string) error {
			if day == "2026-08-02" {
				return errors.New("Error 1146 (42S02): Table 'tron.transactions_260802' doesn't exist")
			}
			return nil
		})
	if !ok || days != 3 {
		t.Fatalf("ok=%v days=%d, want true 3", ok, days)
	}
	if len(missing) != 1 || missing[0] != "2026-08-02" {
		t.Fatalf("missing=%v, want [2026-08-02]", missing)
	}

	_, _, ok = s.forEachStatsDay(
		statsTestContext("start_date=260801&days=2"),
		func(time.Time, string) error { return errors.New("connection refused") })
	if ok {
		t.Fatal("non-1146 error must abort with ok=false")
	}
}
