package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"tron-tracker/common"
	"tron-tracker/database/models"

	"github.com/gin-gonic/gin"
)

func TestBuildConcentration(t *testing.T) {
	// stats are the per-day top-n candidates already fetched from the DB; total
	// is the network-wide denominator passed in separately (read from the total
	// row / SUM), NOT derived from these rows — they're only the head.
	stats := []*models.UserStatistic{
		{Address: "B", TRXTotal: 30},
		{Address: "A", TRXTotal: 50},
		{Address: "C", TRXTotal: 20},
	}
	const total = 200 // network-wide, larger than the visible head rows

	topNSum, list := buildConcentration(stats, "trx_total", total, 2)

	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2 (top n)", len(list))
	}
	if list[0].Address != "A" || list[1].Address != "B" {
		t.Fatalf("order = [%s %s], want [A B] (desc by metric)", list[0].Address, list[1].Address)
	}
	if topNSum != 80 {
		t.Fatalf("topNSum = %d, want 80", topNSum)
	}
	if list[0].Value != 50 {
		t.Fatalf("top value = %d, want 50", list[0].Value)
	}
	// percent uses the passed-in network total (200), not the sum of these rows.
	if want := common.FormatOfPercent(total, 50); list[0].Percent != want {
		t.Fatalf("top percent = %q, want %q (over network total, not head)", list[0].Percent, want)
	}
}

func TestBuildConcentrationClampsN(t *testing.T) {
	stats := []*models.UserStatistic{
		{Address: "A", TRXTotal: 5},
		{Address: "B", TRXTotal: 3},
	}

	// n larger than len: return all, no panic.
	if _, list := buildConcentration(stats, "trx_total", 8, 100); len(list) != 2 {
		t.Fatalf("n>len: list len = %d, want 2", len(list))
	}

	// n <= 0: empty list, zero head sum.
	topNSum, list := buildConcentration(stats, "trx_total", 8, 0)
	if len(list) != 0 || topNSum != 0 {
		t.Fatalf("n=0: list=%d topNSum=%d, want 0,0", len(list), topNSum)
	}

	// negative n must clamp to 0, not panic on list[:negative].
	if _, list := buildConcentration(stats, "trx_total", 8, -1); len(list) != 0 {
		t.Fatalf("n<0: list len = %d, want 0", len(list))
	}
}

// The handler must reject a bad metric or direction with a 400 BEFORE touching
// the DB, so these run with a nil-db Server without panicking.
func TestTopAddrsValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := &Server{}
	router := gin.New()
	router.GET("/top_addrs", s.topAddrs)

	serve := func(query string) string {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/top_addrs"+query, nil)
		router.ServeHTTP(w, req)
		return w.Body.String()
	}

	// Unknown metric: rejected, not silently ranked by a zero column.
	if body := serve("?metric=bogus"); !strings.Contains(body, "unsupported metric") {
		t.Fatalf("metric=bogus body = %s, want unsupported metric error", body)
	}

	// Direction must be from/to.
	if body := serve("?direction=sideways"); !strings.Contains(body, "direction must be") {
		t.Fatalf("direction=sideways body = %s, want direction error", body)
	}
}
