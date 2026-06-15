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
	stats := []*models.UserStatistic{
		{Address: "B", TRXTotal: 30},
		{Address: "A", TRXTotal: 50},
		{Address: "C", TRXTotal: 20},
	}

	total, topNSum, list := buildConcentration(stats, "trx_total", 2)

	// total is the network-wide denominator: summed over ALL stats, BEFORE the
	// top-n truncation. Summing the post-truncation slice would overstate every
	// address's share and make concentration look higher than it is.
	if total != 100 {
		t.Fatalf("total = %d, want 100 (sum over all, pre-truncation)", total)
	}
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
	// percent is value over the network total, in that argument order.
	if want := common.FormatOfPercent(100, 50); list[0].Percent != want {
		t.Fatalf("top percent = %q, want %q", list[0].Percent, want)
	}
}

func TestBuildConcentrationClampsN(t *testing.T) {
	stats := []*models.UserStatistic{
		{Address: "A", TRXTotal: 5},
		{Address: "B", TRXTotal: 3},
	}

	// n larger than len: return all, no panic.
	if _, _, list := buildConcentration(stats, "trx_total", 100); len(list) != 2 {
		t.Fatalf("n>len: list len = %d, want 2", len(list))
	}

	// n <= 0: empty list, but total stays network-wide.
	total, topNSum, list := buildConcentration(stats, "trx_total", 0)
	if len(list) != 0 || topNSum != 0 {
		t.Fatalf("n=0: list=%d topNSum=%d, want 0,0", len(list), topNSum)
	}
	if total != 8 {
		t.Fatalf("n=0: total = %d, want 8 (still full network)", total)
	}

	// negative n must clamp to 0, not panic on list[:negative].
	if _, _, list := buildConcentration(stats, "trx_total", -1); len(list) != 0 {
		t.Fatalf("n<0: list len = %d, want 0", len(list))
	}
}

// The handler must reject a bad metric or direction with a 400 BEFORE touching
// the DB, so these run with a nil-db Server without panicking.
func TestTopTransferConcentrationValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := &Server{}
	router := gin.New()
	router.GET("/top_transfer_concentration", s.topTransferConcentration)

	serve := func(query string) string {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/top_transfer_concentration"+query, nil)
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
