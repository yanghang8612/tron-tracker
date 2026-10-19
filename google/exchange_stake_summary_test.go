package google

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"tron-tracker/database/models"
	"tron-tracker/net"

	"google.golang.org/api/slides/v1"
)

func stakeSummaryPage(t *testing.T) *slides.Page {
	t.Helper()
	data, err := os.ReadFile("testdata/exchange_stake_summary.json")
	if err != nil {
		t.Fatal(err)
	}
	var page slides.Page
	if err := json.Unmarshal(data, &page); err != nil {
		t.Fatal(err)
	}
	return &page
}

func TestExchangeStakeSummaryAllExchangesAndUnits(t *testing.T) {
	current, err := sumExchangeStake(map[string]*models.ExchangeResourceStatistic{
		"Binance": {Bandwidth: 6_000_000_000_000_000, Energy: 3_000_000_000_000_000},
		"HTX":     {Bandwidth: 1_000_000_000_000_000, Energy: 2_000_000_000_000_000},
		"Small":   {Bandwidth: 1_000_000, Energy: 1_000_000},
	})
	if err != nil || current.bandwidth != 7_000_000_001_000_000 || current.energy != 5_000_000_001_000_000 {
		t.Fatalf("all-CEX sums = %+v, %v", current, err)
	}
	page := stakeSummaryPage(t)
	// Reordering and nesting groups must not redirect writes to other objects.
	page.PageElements[0], page.PageElements[5] = page.PageElements[5], page.PageElements[0]
	page.PageElements = []*slides.PageElement{{ElementGroup: &slides.Group{Children: page.PageElements}}}
	before, _ := json.Marshal(page)
	reqs, err := buildExchangeStakeSummaryRequests(page, current, exchangeStakeTotals{8_000_000_000_000_000, 4_000_000_000_000_000}, true, net.NetworkStakeTotals{BandwidthTRX: 28_000_000_000, EnergyTRX: 20_000_000_000})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"g3e67b95e10a_2_8":  "7.00 B TRX -12.50%",
		"g3e67b95e10a_2_11": "25.00% (total 28.00 B)",
		"g3e67b95e10a_2_14": "5.00 B TRX +25.00%",
		"g3e67b95e10a_2_17": "25.00% (total 20.00 B)",
		"g3e67b95e10a_2_20": "12.00 B TRX +0.00%",
		"g3e67b95e10a_2_23": "25.00% (total 48.00 B)",
	}
	got := map[string]string{}
	styles := map[string][]*slides.UpdateTextStyleRequest{}
	for _, req := range reqs {
		if req.InsertText != nil {
			got[req.InsertText.ObjectId] = req.InsertText.Text
		}
		if req.DeleteText != nil && req.DeleteText.TextRange.Type != "FIXED_RANGE" {
			t.Fatal("must preserve the final paragraph newline")
		}
		if req.UpdateTextStyle != nil {
			s := req.UpdateTextStyle
			styles[s.ObjectId] = append(styles[s.ObjectId], s)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("card writes = %#v, want %#v", got, want)
	}
	for id, text := range want {
		if len(styles[id]) != 2 || styles[id][0].Style.FontSize.Magnitude != 11 || styles[id][1].Style.FontSize.Magnitude != 7 || !styles[id][0].Style.Bold || styles[id][1].Style.Bold {
			t.Fatalf("card %s lost its mixed styles: %+v", id, styles[id])
		}
		if *styles[id][0].TextRange.StartIndex != 0 || *styles[id][0].TextRange.EndIndex != *styles[id][1].TextRange.StartIndex || *styles[id][1].TextRange.EndIndex != utf16Len(text) {
			t.Fatalf("style ranges for %s do not cover new text", id)
		}
	}
	if styles["g3e67b95e10a_2_8"][1].Style.ForegroundColor.OpaqueColor.RgbColor.Red != 1 || styles["g3e67b95e10a_2_14"][1].Style.ForegroundColor.OpaqueColor.RgbColor.Green != 1 {
		t.Fatal("week-over-week colors must reflect sign")
	}
	// The request builder must not change the fetched template objects.
	after, _ := json.Marshal(page)
	if string(before) != string(after) {
		t.Fatal("request builder mutated the input template")
	}
}

func TestExchangeStakeSummaryMissingDataAndTemplate(t *testing.T) {
	if _, err := sumExchangeStake(nil); err == nil {
		t.Fatal("missing snapshot must not become a zero total")
	}
	if _, err := sumExchangeStake(map[string]*models.ExchangeResourceStatistic{"bad": {Bandwidth: -1}}); err == nil {
		t.Fatal("negative totals must be rejected")
	}
	for _, tc := range []struct {
		previous, current int64
		available         bool
		want              string
	}{
		{0, 5, false, "N/A"}, {0, 5, true, "N/A"}, {0, 0, true, "+0.00%"}, {100, 0, true, "-100.00%"}, {100, 125, true, "+25.00%"},
	} {
		if got := exchangeStakeChange(tc.previous, tc.current, tc.available); got != tc.want {
			t.Fatalf("change(%+v) = %s", tc, got)
		}
	}
	network := net.NetworkStakeTotals{BandwidthTRX: 100, EnergyTRX: 100}
	page := stakeSummaryPage(t)
	page.PageElements = page.PageElements[:5]
	if reqs, err := buildExchangeStakeSummaryRequests(page, exchangeStakeTotals{}, exchangeStakeTotals{}, false, network); err == nil || reqs != nil {
		t.Fatal("incomplete template must fail without partial writes")
	}
	page = stakeSummaryPage(t)
	page.PageElements = append(page.PageElements, page.PageElements[0])
	if _, err := findExchangeStakeSummaryElements(page); err == nil {
		t.Fatal("duplicate card labels must be rejected")
	}
	if _, err := buildExchangeStakeSummaryRequests(stakeSummaryPage(t), exchangeStakeTotals{}, exchangeStakeTotals{}, false, net.NetworkStakeTotals{}); err == nil {
		t.Fatal("zero network denominators must be rejected")
	}
}
