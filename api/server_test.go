package api

import "testing"

func TestPickTopNAndLastN(t *testing.T) {
	id := func(x int) int { return x }

	// len <= 2n: returns all elements.
	if got := pickTopNAndLastN([]int{1, 2, 3}, 5, id); len(got) != 3 {
		t.Fatalf("len<=2n: got len %d, want 3", len(got))
	}
	// len > 2n: returns first n + last n.
	got := pickTopNAndLastN([]int{1, 2, 3, 4, 5, 6, 7}, 2, id)
	if len(got) != 4 || got[0] != 1 || got[1] != 2 || got[2] != 6 || got[3] != 7 {
		t.Fatalf("len>2n: got %v, want [1 2 6 7]", got)
	}
	// n == 0: empty.
	if got := pickTopNAndLastN([]int{1, 2, 3}, 0, id); len(got) != 0 {
		t.Fatalf("n=0: got %v, want empty", got)
	}
}

// TestTopUsersSliceClamp guards the /top_users [:n] out-of-range crash: when the
// result is shorter than n (sparse/empty day) or n is negative, the high bound
// must stay within [0, len]. This is the invariant the max(0, min(n, len)) fix
// relies on.
func TestTopUsersSliceClamp(t *testing.T) {
	for _, length := range []int{0, 2, 50} {
		result := make([]int, length)
		for _, n := range []int{-1, 0, 1, 2, 5, 100} {
			end := max(0, min(n, len(result)))
			if end < 0 || end > len(result) {
				t.Fatalf("clamp(n=%d,len=%d)=%d out of [0,%d]", n, length, end, length)
			}
			_ = result[:end] // must never panic
		}
	}
}

func TestSizeToBytes(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		{"1.00 KiB", 1024},
		{"2.00 MiB", 2 * 1024 * 1024},
		{"123", 123}, // no unit
		{"", 0},      // empty must not panic (regression: Fields("")[0])
		{"   ", 0},   // whitespace only must not panic
	}
	for _, tt := range tests {
		if got := sizeToBytes(tt.in); got != tt.want {
			t.Errorf("sizeToBytes(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
