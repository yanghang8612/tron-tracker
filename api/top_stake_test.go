package api

import (
	"testing"

	"tron-tracker/database/models"
)

func mkStakeTx(owner string, typ uint8, amount int64) *models.Transaction {
	tx := &models.Transaction{OwnerAddr: owner, Type: typ}
	tx.SetAmount(amount)
	return tx
}

// Stake board is ranked by stake2 + cancel (descending). A CancelAllUnfreezeV2
// (type 59) re-stakes pending unstakes, so it must count toward the stake score
// — mirroring java-tron's TopStakeServlet sort key.
func TestBuildTopStake_StakeRankingUsesStake2PlusCancel(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("addrLow", 54, 100), // stake2=100        -> score 100
		mkStakeTx("addrHigh", 54, 40), // stake2=40
		mkStakeTx("addrHigh", 59, 70), // cancel=70         -> score 110
	}

	topStake, _ := buildTopStake(txs, 20)

	if len(topStake) != 2 {
		t.Fatalf("got %d stake entries, want 2", len(topStake))
	}
	if topStake[0].Address != "addrHigh" {
		t.Fatalf("rank0 = %s (score %s), want addrHigh — cancel must count toward stake score",
			topStake[0].Address, topStake[0].Score)
	}
	if topStake[0].Score != "110" {
		t.Fatalf("addrHigh stake score = %s, want 110 (40+70)", topStake[0].Score)
	}
	if topStake[0].Stake2 != "40" || topStake[0].Cancel != "70" {
		t.Fatalf("addrHigh components = stake2:%s cancel:%s, want 40/70", topStake[0].Stake2, topStake[0].Cancel)
	}
}

// Unstake board is ranked by unstake (v1, type 12) + unstake2 (v2, type 55),
// descending — the other sort key in java-tron's TopStakeServlet.
func TestBuildTopStake_UnstakeRankingUsesUnstakePlusUnstake2(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("addrA", 55, 100), // unstake2=100              -> score 100
		mkStakeTx("addrB", 55, 80),  // unstake2=80
		mkStakeTx("addrB", 12, 30),  // unstake=30 (legacy v1)    -> score 110
	}

	_, topUnstake := buildTopStake(txs, 20)

	if len(topUnstake) != 2 {
		t.Fatalf("got %d unstake entries, want 2", len(topUnstake))
	}
	if topUnstake[0].Address != "addrB" {
		t.Fatalf("rank0 = %s (score %s), want addrB — v1 unstake must count toward unstake score",
			topUnstake[0].Address, topUnstake[0].Score)
	}
	if topUnstake[0].Score != "110" {
		t.Fatalf("addrB unstake score = %s, want 110 (30+80)", topUnstake[0].Score)
	}
	if topUnstake[0].Unstake != "30" || topUnstake[0].Unstake2 != "80" {
		t.Fatalf("addrB components = unstake:%s unstake2:%s, want 30/80", topUnstake[0].Unstake, topUnstake[0].Unstake2)
	}
}

// ENERGY-resource freeze/unfreeze carry type = base + 100 (e.g. 154). They must
// fold into the same component as their BANDWIDTH counterpart, i.e. by type%100.
func TestBuildTopStake_FoldsEnergyVariantsIntoBaseFields(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("addr", 54, 100), mkStakeTx("addr", 154, 50), // stake2   -> 150
		mkStakeTx("addr", 59, 10), mkStakeTx("addr", 159, 5), //   cancel   -> 15
		mkStakeTx("addr", 55, 200), mkStakeTx("addr", 155, 40), // unstake2 -> 240
		mkStakeTx("addr", 12, 7), mkStakeTx("addr", 112, 3), //   unstake   -> 10
	}

	topStake, topUnstake := buildTopStake(txs, 20)

	if len(topStake) != 1 {
		t.Fatalf("got %d stake entries, want 1", len(topStake))
	}
	e := topStake[0]
	if e.Stake2 != "150" || e.Cancel != "15" || e.Unstake2 != "240" || e.Unstake != "10" {
		t.Fatalf("folded components = stake2:%s cancel:%s unstake2:%s unstake:%s, want 150/15/240/10",
			e.Stake2, e.Cancel, e.Unstake2, e.Unstake)
	}
	if topStake[0].Score != "165" { // 150 + 15
		t.Fatalf("stake score = %s, want 165", topStake[0].Score)
	}
	if topUnstake[0].Score != "250" { // 240 + 10
		t.Fatalf("unstake score = %s, want 250", topUnstake[0].Score)
	}
}

// Guard the slice bounds: empty input, n larger than the population, and n=0 must
// all return cleanly without panicking, and n smaller than the population must
// truncate to exactly n.
func TestBuildTopStake_TruncatesToNAndHandlesEmpty(t *testing.T) {
	if topStake, topUnstake := buildTopStake(nil, 20); len(topStake) != 0 || len(topUnstake) != 0 {
		t.Fatalf("empty input: got %d/%d entries, want 0/0", len(topStake), len(topUnstake))
	}

	txs := []*models.Transaction{
		mkStakeTx("a", 54, 300),
		mkStakeTx("b", 54, 200),
		mkStakeTx("c", 54, 100),
	}

	top2, _ := buildTopStake(txs, 2)
	if len(top2) != 2 {
		t.Fatalf("n=2 over 3 stakers: got %d, want 2", len(top2))
	}
	if top2[0].Address != "a" || top2[1].Address != "b" {
		t.Fatalf("n=2 order = %s,%s, want a,b (descending by score)", top2[0].Address, top2[1].Address)
	}

	if all, _ := buildTopStake(txs, 100); len(all) != 3 {
		t.Fatalf("n>population: got %d, want all 3", len(all))
	}
	if none, _ := buildTopStake(txs, 0); len(none) != 0 {
		t.Fatalf("n=0: got %d, want 0", len(none))
	}
}

// An account that both stakes and unstakes in the window must appear on BOTH
// boards, each row carrying the full component breakdown — java-tron builds both
// rankings from the same merged per-address state.
func TestBuildTopStake_AddressOnBothBoardsKeepsAllComponents(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("dual", 54, 100), // stake2=100
		mkStakeTx("dual", 55, 50),  // unstake2=50
	}

	topStake, topUnstake := buildTopStake(txs, 20)

	if len(topStake) != 1 || len(topUnstake) != 1 {
		t.Fatalf("got %d stake / %d unstake entries, want 1/1", len(topStake), len(topUnstake))
	}
	if topStake[0].Score != "100" || topStake[0].Stake2 != "100" || topStake[0].Unstake2 != "50" {
		t.Fatalf("stake board row = score:%s stake2:%s unstake2:%s, want 100/100/50 (full components)",
			topStake[0].Score, topStake[0].Stake2, topStake[0].Unstake2)
	}
	if topUnstake[0].Score != "50" || topUnstake[0].Stake2 != "100" || topUnstake[0].Unstake2 != "50" {
		t.Fatalf("unstake board row = score:%s stake2:%s unstake2:%s, want 50/100/50 (full components)",
			topUnstake[0].Score, topUnstake[0].Stake2, topUnstake[0].Unstake2)
	}
}

// Equal scores must break ties by address so identical requests yield identical
// order, regardless of (random) map iteration. Repeated to catch nondeterminism.
func TestBuildTopStake_TiesAreOrderedDeterministicallyByAddress(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("ccc", 54, 100),
		mkStakeTx("aaa", 54, 100),
		mkStakeTx("bbb", 54, 100),
	}

	for i := 0; i < 50; i++ {
		topStake, _ := buildTopStake(txs, 20)
		if len(topStake) != 3 {
			t.Fatalf("got %d entries, want 3", len(topStake))
		}
		if topStake[0].Address != "aaa" || topStake[1].Address != "bbb" || topStake[2].Address != "ccc" {
			t.Fatalf("tie order = %s,%s,%s, want aaa,bbb,ccc (ascending address)",
				topStake[0].Address, topStake[1].Address, topStake[2].Address)
		}
	}
}

// Each of stake2/unstake/unstake2 is additionally split by resource:
// the ENERGY portion comes from the +100 type variants, and the BANDWIDTH
// portion is total - energy by construction, so energy + bandwidth always
// reconciles with the unsplit component. Cancel (59) has no resource
// dimension on-chain and is not split.
func TestBuildTopStake_SplitsComponentsByResource(t *testing.T) {
	txs := []*models.Transaction{
		mkStakeTx("addr", 54, 100), mkStakeTx("addr", 154, 50), //  stake2: B=100 E=50
		mkStakeTx("addr", 55, 200), mkStakeTx("addr", 155, 40), // unstake2: B=200 E=40
		mkStakeTx("addr", 12, 7), mkStakeTx("addr", 112, 3), //    unstake: B=7   E=3
	}

	topStake, _ := buildTopStake(txs, 20)

	if len(topStake) != 1 {
		t.Fatalf("got %d entries, want 1", len(topStake))
	}
	e := topStake[0]
	checks := []struct{ name, got, want string }{
		{"stake2_energy", e.Stake2Energy, "50"},
		{"stake2_bandwidth", e.Stake2Bandwidth, "100"},
		{"unstake2_energy", e.Unstake2Energy, "40"},
		{"unstake2_bandwidth", e.Unstake2Bandwidth, "200"},
		{"unstake_energy", e.UnstakeEnergy, "3"},
		{"unstake_bandwidth", e.UnstakeBandwidth, "7"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %s, want %s", c.name, c.got, c.want)
		}
	}
	// Reconciliation: energy + bandwidth must equal the unsplit component.
	if e.Stake2 != "150" || e.Unstake2 != "240" || e.Unstake != "10" {
		t.Fatalf("unsplit components changed: stake2=%s unstake2=%s unstake=%s",
			e.Stake2, e.Unstake2, e.Unstake)
	}
}
