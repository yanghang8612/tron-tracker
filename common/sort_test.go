package common

import "testing"

func TestTopN_BoundaryN(t *testing.T) {
	items := []int{5, 3, 9, 1, 7}
	desc := func(a, b int) bool { return a > b }

	// n < 0 must not panic (regression: make([]T, n) crashed on negative n).
	if got := TopN(items, -1, desc); len(got) != 0 {
		t.Fatalf("TopN(n=-1) = %v, want empty", got)
	}
	// n == 0 returns empty.
	if got := TopN(items, 0, desc); len(got) != 0 {
		t.Fatalf("TopN(n=0) = %v, want empty", got)
	}
	// n > len returns everything, sorted.
	got := TopN(items, 10, desc)
	if len(got) != len(items) || got[0] != 9 {
		t.Fatalf("TopN(n=10) = %v, want all sorted desc starting 9", got)
	}
	// normal n.
	if top2 := TopN(items, 2, desc); len(top2) != 2 || top2[0] != 9 || top2[1] != 7 {
		t.Fatalf("TopN(n=2) = %v, want [9 7]", top2)
	}
}
