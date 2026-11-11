// cold-compress performs a bounded, resumable, serial migration of closed daily
// InnoDB tables. Originals are retained until the promoted copy is verified.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	mysql "github.com/go-sql-driver/mysql"
)

var dailyName = regexp.MustCompile(`^(transactions|user_token_stats|from_stats|to_stats)_[0-9]{6}$`)
var simpleName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var errPause = errors.New("maintenance paused")
var cfgPath, workDir, tmpDir, jobDB, sourceDB string
var execute, ignoreWindow, selfTest bool
var maxTables int
var db *sql.DB
var ctl *controller
var root context.Context
var cancelRoot context.CancelFunc

type fingerprint struct {
	Rows, Bytes int64
	SHA256      string
}
type tableReport struct {
	Table, Status, Phase, Error, StartedUTC, UpdatedUTC, FinishedUTC string
	ShadowDB, Backup, OriginalDDL, SchemaSHA                         string
	Secondary                                                        []string
	Source, Compressed, Promoted                                     fingerprint
	BeforeBytes, AfterBytes, AutoIncrement                           uint64
	CopyLastID                                                       uint64
	ReusedPilot                                                      bool
}
type summary struct {
	Status, Phase, CurrentTable, Error, StartedUTC, UpdatedUTC, CutoffUTC string
	SourceDatabase, JobDatabase                                           string
	Planned, Completed, Skipped                                           int
	ReclaimedBytes                                                        uint64
}
type health struct {
	TimeUTC    string
	Block      uint64
	LagSeconds float64
	Ready      bool
	Reason     string
}
type controller struct {
	mu        sync.Mutex
	h         health
	good, bad int
	cancel    context.CancelFunc
	killID    uint64
	owner     *sql.Conn
	fatal     error
}

var sum summary
var publishReady bool
var jobInitialized bool

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func qi(s string) string               { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func qt(database, table string) string { return qi(database) + "." + qi(table) }
func stamp() string                    { return time.Now().UTC().Format(time.RFC3339) }
func writeJSON(path string, v any) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(raw); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(path+".tmp", path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e == nil {
		e = d.Sync()
		d.Close()
	}
	return e
}

// The private two-row/three-row status table exposes no credentials and lets
// operators inspect the job through the existing read-only SQL endpoint.
func publish(name string, v any) {
	if !publishReady {
		return
	}
	raw, e := json.Marshal(v)
	if e != nil {
		log.Printf("publish report: %v", e)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, e = db.ExecContext(ctx, "INSERT INTO "+qt(jobDB, "_job_state")+" (name,payload) VALUES (?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload)", name, string(raw))
	if e != nil {
		log.Printf("publish %s: %v", name, e)
	}
}

func saveSummary() error {
	sum.UpdatedUTC = stamp()
	if e := writeJSON(filepath.Join(workDir, "summary.json"), &sum); e != nil {
		return e
	}
	publish("summary", &sum)
	return nil
}
func save(r *tableReport, phase string) error {
	if r.Phase != phase {
		log.Printf("%s %s", r.Table, phase)
	}
	r.Phase = phase
	r.UpdatedUTC = stamp()
	sum.CurrentTable = r.Table
	sum.Phase = phase
	sum.Status = "running"
	if e := writeJSON(filepath.Join(workDir, "tables", r.Table+".json"), r); e != nil {
		return e
	}
	publish("current", r)
	return saveSummary()
}
func window(t time.Time) bool { h := t.UTC().Hour(); return h >= 23 || h < 3 }
func freeBytes() (uint64, error) {
	var s syscall.Statfs_t
	e := syscall.Statfs("/data/mysql", &s)
	return uint64(s.Bavail) * uint64(s.Bsize), e
}
func (c *controller) sample() {
	h := health{TimeUTC: stamp()}
	client := http.Client{Timeout: 4 * time.Second}
	resp, e := client.Get("http://127.0.0.1:8080/last-tracked-block-num")
	if e == nil {
		var v struct {
			Block uint64 `json:"last_tracked_block_number"`
			Time  string `json:"last_tracked_block_time"`
		}
		e = json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()
		if e == nil && resp.StatusCode != 200 {
			e = fmt.Errorf("tracker HTTP %d", resp.StatusCode)
		}
		if e == nil {
			var t time.Time
			t, e = time.ParseInLocation("2006-01-02 15:04:05", v.Time, time.UTC)
			h.Block = v.Block
			h.LagSeconds = time.Since(t).Seconds()
			if e == nil && (v.Block == 0 || h.LagSeconds > 30) {
				e = fmt.Errorf("tracker lag %.1fs", h.LagSeconds)
			}
		}
	}
	if e == nil && !selfTest {
		ctx, cancel := context.WithTimeout(root, 4*time.Second)
		var flushed string
		e = db.QueryRowContext(ctx, "SELECT val FROM "+qt(sourceDB, "meta")+" WHERE "+qi("key")+"='flushed_date'").Scan(&flushed)
		cancel()
		if e == nil {
			d, er := time.Parse("060102", flushed)
			yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
			if er != nil || len(flushed) != 6 {
				e = errors.New("invalid flushed_date")
			} else if d.Before(yesterday) {
				e = errors.New("daily statistics flush has not finished")
			}
		}
	}
	softPause := false
	if e != nil {
		h.Reason = e.Error()
	} else if h.LagSeconds > 15 {
		h.Reason = "waiting for tracker lag below 15 seconds"
		softPause = true
	}
	lockLost := false
	if c.owner != nil && root.Err() == nil {
		ctx, cancel := context.WithTimeout(root, 4*time.Second)
		var owner sql.NullInt64
		var self uint64
		er := c.owner.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?),CONNECTION_ID()", "tracker_serial_cold_compress_v1").Scan(&owner, &self)
		cancel()
		if er != nil || !owner.Valid || uint64(owner.Int64) != self {
			h.Reason = "migration advisory lock lost"
			softPause = false
			lockLost = true
		}
	}
	urgent := lockLost
	if !ignoreWindow && window(time.Now()) {
		h.Reason = "UTC 23:00-03:00 maintenance blackout"
		urgent = true
	}
	n, de := freeBytes()
	if de != nil {
		h.Reason = de.Error()
		urgent = true
	} else if n < 64<<30 {
		h.Reason = "less than 64 GiB free"
		urgent = true
	}
	c.mu.Lock()
	if lockLost {
		c.fatal = errors.New("migration advisory lock lost; refusing further changes")
	}
	if h.Reason == "" {
		c.good++
		c.bad = 0
	} else if softPause && !urgent {
		c.good = 0
		c.bad = 0
	} else {
		c.good = 0
		c.bad++
	}
	h.Ready = c.good >= 3
	var interrupt context.CancelFunc
	var killID uint64
	if urgent || c.bad >= 2 {
		interrupt = c.cancel
		killID = c.killID
		if lockLost {
			killID = 0
		} // Connection IDs may have been reused after a server restart.
	}
	c.h = h
	c.mu.Unlock()
	// Closing the client alone does not promptly stop every server-side DDL.
	// Interrupt only the dedicated connection owned by this worker.
	if killID != 0 {
		if er := killOwnQuery(killID); er != nil {
			log.Printf("interrupt own SQL connection %d: %v", killID, er)
		}
	}
	if interrupt != nil {
		interrupt()
	}
	if e := writeJSON(filepath.Join(workDir, "health.json"), h); e != nil {
		log.Printf("health report: %v", e)
	}
	publish("health", h)
}
func (c *controller) ready() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.h.Ready }
func (c *controller) gate() error {
	noted := ""
	for !c.ready() {
		c.mu.Lock()
		fatal := c.fatal
		c.mu.Unlock()
		if fatal != nil {
			return fatal
		}
		if e := root.Err(); e != nil {
			return e
		}
		c.mu.Lock()
		reason := c.h.Reason
		c.mu.Unlock()
		if reason == "" {
			reason = "waiting for three healthy tracker samples"
		}
		if reason != noted {
			log.Printf("PAUSED %s", reason)
			sum.Status = "paused"
			sum.Error = reason
			if e := saveSummary(); e != nil {
				return e
			}
			noted = reason
		}
		select {
		case <-root.Done():
			return root.Err()
		case <-time.After(time.Second):
		}
	}
	if noted != "" {
		sum.Status = "running"
		sum.Error = ""
		return saveSummary()
	}
	return nil
}
func operation(d time.Duration, fn func(context.Context) error) error {
	if e := ctl.gate(); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(root, d)
	defer cancel()
	ctl.mu.Lock()
	if ctl.fatal != nil {
		fatal := ctl.fatal
		ctl.mu.Unlock()
		return fatal
	}
	if !ctl.h.Ready {
		ctl.mu.Unlock()
		return errPause
	}
	ctl.cancel = cancel
	ctl.mu.Unlock()
	e := fn(ctx)
	ctl.mu.Lock()
	ctl.cancel = nil
	ready := ctl.h.Ready
	ctl.mu.Unlock()
	if e != nil && !ready && root.Err() == nil {
		return fmt.Errorf("%w: %v", errPause, e)
	}
	return e
}
func put(h hash.Hash, b []byte) int64 {
	var p [8]byte
	if b == nil {
		binary.LittleEndian.PutUint64(p[:], ^uint64(0))
	} else {
		binary.LittleEndian.PutUint64(p[:], uint64(len(b)))
	}
	h.Write(p[:])
	h.Write(b)
	return int64(len(b))
}
func digest(ctx context.Context, q queryer, statement string, args ...any) (fingerprint, error) {
	var f fingerprint
	rows, e := q.QueryContext(ctx, statement, args...)
	if e != nil {
		return f, e
	}
	defer rows.Close()
	cols, e := rows.Columns()
	if e != nil {
		return f, e
	}
	values := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range dest {
		dest[i] = &values[i]
	}
	h := sha256.New()
	for rows.Next() {
		if e = rows.Scan(dest...); e != nil {
			return f, e
		}
		for _, v := range values {
			f.Bytes += put(h, v)
		}
		f.Rows++
	}
	if e = rows.Err(); e != nil {
		return f, e
	}
	f.SHA256 = hex.EncodeToString(h.Sum(nil))
	return f, nil
}
func hashTable(q queryer, database, table string) (f fingerprint, e error) {
	e = operation(20*time.Minute, func(ctx context.Context) error {
		var er error
		f, er = digest(ctx, q, "SELECT * FROM "+qt(database, table)+" FORCE INDEX(PRIMARY) ORDER BY id")
		return er
	})
	return
}
func same(a, b fingerprint) bool { return a == b && a.SHA256 != "" }
func exists(database, table string) (bool, error) {
	var n int
	e := db.QueryRowContext(root, "SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", database, table).Scan(&n)
	return n == 1, e
}
func size(database, table string) (uint64, string, uint64, error) {
	var bytes uint64
	var format string
	var ai sql.NullInt64
	e := db.QueryRowContext(root, "SELECT s.ALLOCATED_SIZE,t.ROW_FORMAT,t.AUTO_INCREMENT FROM information_schema.tables t JOIN information_schema.innodb_tablespaces s ON s.NAME=CONCAT(t.TABLE_SCHEMA,'/',t.TABLE_NAME) WHERE t.TABLE_SCHEMA=? AND t.TABLE_NAME=?", database, table).Scan(&bytes, &format, &ai)
	return bytes, format, uint64(ai.Int64), e
}
func ddl(ctx context.Context, q queryer, database, table string) (string, error) {
	var name, s string
	e := q.QueryRowContext(ctx, "SHOW CREATE TABLE "+qt(database, table)).Scan(&name, &s)
	return s, e
}
func schema(database, table string) (string, error) {
	// Row format and allocation are intentionally excluded; all exposed columns,
	// defaults and index key parts (including expressions/visibility) must agree.
	a, e := digest(root, db, "SELECT ORDINAL_POSITION,COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,CHARACTER_SET_NAME,COLLATION_NAME,GENERATION_EXPRESSION FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", database, table)
	if e != nil {
		return "", e
	}
	b, e := digest(root, db, "SELECT INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,NON_UNIQUE,COLLATION,SUB_PART,INDEX_TYPE,IS_VISIBLE,EXPRESSION FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX", database, table)
	return a.SHA256 + ":" + b.SHA256, e
}

// splitDefinitions recognizes SQL quoted strings/identifiers, escaped quotes,
// parentheses and commas. Index expressions are retained verbatim for rebuild.
func splitDefinitions(s string) ([]string, error) {
	start := strings.IndexByte(s, '(')
	if start < 0 {
		return nil, errors.New("missing CREATE body")
	}
	begin := start + 1
	depth := 1
	var quote byte
	out := []string{}
	for i := begin; i < len(s); i++ {
		ch := s[i]
		if quote != 0 {
			if ch == '\\' && quote != '`' {
				i++
				continue
			}
			if ch == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if ch == '`' || ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[begin:i]))
				return out, nil
			}
		case ',':
			if depth == 1 {
				out = append(out, strings.TrimSpace(s[begin:i]))
				begin = i + 1
			}
		}
	}
	return nil, errors.New("unterminated CREATE body")
}

var autoIncrementOption = regexp.MustCompile(`(?m)^\) ENGINE=.*?\bAUTO_INCREMENT=([0-9]+)\b`)

func nextAutoIncrement(create string) (uint64, error) {
	v := autoIncrementOption.FindStringSubmatch(create)
	if len(v) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(v[1], 10, 64)
}

func secondary(s string) ([]string, []string, error) {
	defs, e := splitDefinitions(s)
	if e != nil {
		return nil, nil, e
	}
	var names, clauses []string
	for _, d := range defs {
		u := strings.ToUpper(d)
		if strings.HasPrefix(u, "KEY ") || strings.HasPrefix(u, "UNIQUE KEY ") {
			i := strings.IndexByte(d, '`')
			if i < 0 {
				return nil, nil, errors.New("unquoted index")
			}
			var b strings.Builder
			end := -1
			for j := i + 1; j < len(d); j++ {
				if d[j] == '`' {
					if j+1 < len(d) && d[j+1] == '`' {
						b.WriteByte('`')
						j++
						continue
					}
					end = j
					break
				}
				b.WriteByte(d[j])
			}
			if end < 0 {
				return nil, nil, errors.New("unterminated index")
			}
			names = append(names, b.String())
			clauses = append(clauses, d)
		} else if strings.HasPrefix(u, "FULLTEXT ") || strings.HasPrefix(u, "SPATIAL ") {
			return nil, nil, errors.New("unsupported index kind")
		}
	}
	return names, clauses, nil
}
func killOwnQuery(id uint64) error {
	ctx, cancel := context.WithTimeout(root, 4*time.Second)
	defer cancel()
	_, e := db.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", id))
	var me *mysql.MySQLError
	if errors.As(e, &me) && me.Number == 1094 {
		return nil
	}
	return e
}

func runDDL(statement string) error {
	return operation(30*time.Minute, func(ctx context.Context) error {
		conn, e := db.Conn(ctx)
		if e != nil {
			return e
		}
		defer conn.Close()
		var connectionID uint64
		if e = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID); e != nil {
			return e
		}
		ctl.mu.Lock()
		ctl.killID = connectionID
		ctl.mu.Unlock()
		defer func() { ctl.mu.Lock(); ctl.killID = 0; ctl.mu.Unlock() }()
		if _, e = conn.ExecContext(ctx, "SET SESSION innodb_tmpdir=?", tmpDir); e != nil {
			return e
		}
		if _, e = conn.ExecContext(ctx, "SET SESSION lock_wait_timeout=5"); e != nil {
			return e
		}
		_, e = conn.ExecContext(ctx, statement)
		return e
	})
}
func validTable(name string, cutoff time.Time) bool {
	if !dailyName.MatchString(name) {
		return false
	}
	d, e := time.Parse("060102", name[len(name)-6:])
	return e == nil && d.Before(cutoff)
}
func eligible(name string) (bool, error) {
	now := time.Now().UTC()
	cut := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -30)
	if !validTable(name, cut) {
		return false, nil
	}
	rows, e := db.QueryContext(root, "SELECT "+qi("key")+",val FROM "+qt(sourceDB, "meta")+" WHERE "+qi("key")+" IN ('tracking_date','flushed_date','counted_date','counted_week')")
	if e != nil {
		return false, e
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var k, v string
		if e = rows.Scan(&k, &v); e != nil {
			return false, e
		}
		d, er := time.Parse("060102", v)
		if er != nil || len(v) != 6 {
			return false, fmt.Errorf("invalid %s cursor", k)
		}
		if k == "flushed_date" || k == "counted_date" {
			d = d.AddDate(0, 0, 1)
		}
		if d.Before(cut) {
			cut = d
		}
		n++
	}
	if e = rows.Err(); e != nil {
		return false, e
	}
	if n != 4 {
		return false, errors.New("missing daily cursors")
	}
	return validTable(name, cut), nil
}
func preconditions(table string) error {
	var n int
	for _, s := range []string{
		"SELECT COUNT(*) FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?",
		"SELECT COUNT(*) FROM information_schema.key_column_usage WHERE (TABLE_SCHEMA=? AND TABLE_NAME=?) AND REFERENCED_TABLE_NAME IS NOT NULL",
		"SELECT COUNT(*) FROM information_schema.key_column_usage WHERE REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?",
		"SELECT COUNT(*) FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND GENERATION_EXPRESSION<>''",
	} {
		if e := db.QueryRowContext(root, s, sourceDB, table).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return errors.New("triggers, foreign keys or generated columns require manual review")
		}
	}
	var cols string
	if e := db.QueryRowContext(root, "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY'", sourceDB, table).Scan(&cols); e != nil {
		return e
	}
	if cols != "id" {
		return errors.New("requires a single id primary key")
	}
	return nil
}
func load(table string) (*tableReport, error) {
	r := &tableReport{Table: table, Status: "running", StartedUTC: stamp(), ShadowDB: jobDB, Backup: "old_" + table}
	b, e := os.ReadFile(filepath.Join(workDir, "tables", table+".json"))
	if e == nil {
		e = json.Unmarshal(b, r)
	} else if os.IsNotExist(e) {
		e = nil
	}
	if e == nil && (r.Table != table || r.Backup != "old_"+table || (r.ShadowDB != jobDB && r.ShadowDB != "tracker_space_pilot_20261111")) {
		return nil, errors.New("report identity does not match this migration")
	}
	return r, e
}
func finish(r *tableReport, status string) error {
	r.Status = status
	r.FinishedUTC = stamp()
	return save(r, status)
}
func migrate(r *tableReport) error {
	if r.Status == "complete" || strings.HasPrefix(r.Status, "skipped_") {
		return nil
	}
	if e := ctl.gate(); e != nil {
		return e
	}
	old, e := exists(jobDB, r.Backup)
	if e != nil {
		return e
	}
	if old {
		return promoted(r)
	} // A durable swap intent plus backup makes recovery unambiguous.
	present, e := exists(sourceDB, r.Table)
	if e != nil {
		return e
	}
	if !present {
		d, _ := time.Parse("060102", r.Table[len(r.Table)-6:])
		n := time.Now().UTC()
		ret := time.Date(n.Year()-1, n.Month(), n.Day(), 0, 0, 0, 0, time.UTC)
		if d.Before(ret) {
			return finish(r, "skipped_expired")
		}
		return errors.New("non-expired source table disappeared")
	}
	if r.Phase == "promoted" || r.Phase == "verify_promoted" || r.Phase == "verified" || r.Phase == "drop_backup" {
		return promoted(r)
	}
	ok, e := eligible(r.Table)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("source date is not safely finalized and older than 30 days")
	}
	bytes, format, ai, e := size(sourceDB, r.Table)
	if e != nil {
		return e
	}
	if strings.EqualFold(format, "Compressed") {
		if r.Source.SHA256 != "" && r.Phase == "swap_intent" {
			return promoted(r)
		}
		return finish(r, "skipped_already_compressed")
	}
	if e = preconditions(r.Table); e != nil {
		return e
	}
	if r.Source.SHA256 == "" {
		if e = save(r, "source_fingerprint"); e != nil {
			return e
		}
		r.BeforeBytes = bytes
		r.AutoIncrement = ai
		r.OriginalDDL, e = ddl(root, db, sourceDB, r.Table)
		if e != nil {
			return e
		}
		// SHOW CREATE preserves the real next counter, including reserved gaps;
		// information_schema's cached AUTO_INCREMENT can lag behind it.
		r.AutoIncrement, e = nextAutoIncrement(r.OriginalDDL)
		if e != nil {
			return e
		}
		r.SchemaSHA, e = schema(sourceDB, r.Table)
		if e != nil {
			return e
		}
		_, r.Secondary, e = secondary(r.OriginalDDL)
		if e != nil {
			return e
		}
		r.Source, e = hashTable(db, sourceDB, r.Table)
		if e != nil {
			return e
		}
		// Reuse only the two verified pilot copies; both are still rehashed here.
		if r.Table == "transactions_260601" || r.Table == "user_token_stats_260601" {
			yes, er := exists("tracker_space_pilot_20261111", r.Table)
			if er != nil {
				return er
			}
			if yes {
				r.ShadowDB = "tracker_space_pilot_20261111"
				r.ReusedPilot = true
			}
		}
		if e = save(r, "source_verified"); e != nil {
			return e
		}
	}
	space, e := freeBytes()
	if e != nil {
		return e
	}
	if space < r.BeforeBytes*4+(16<<30) {
		return fmt.Errorf("insufficient workspace for %s", r.Table)
	}
	present, e = exists(r.ShadowDB, r.Table)
	if e != nil {
		return e
	}
	if present && r.ShadowDB == jobDB && r.Phase == "source_verified" {
		return errors.New("unowned existing shadow table; refusing to modify it")
	}
	if !present {
		if r.ReusedPilot {
			return errors.New("pilot copy disappeared")
		}
		if e = save(r, "create_shadow"); e != nil {
			return e
		}
		if e = runDDL("CREATE TABLE " + qt(r.ShadowDB, r.Table) + " LIKE " + qt(sourceDB, r.Table)); e != nil {
			return e
		}
	}
	_, shadowFormat, _, e := size(r.ShadowDB, r.Table)
	if e != nil {
		return e
	}
	if !strings.EqualFold(shadowFormat, "Compressed") {
		names, _, er := secondary(r.OriginalDDL)
		if er != nil {
			return er
		}
		var count int
		if e = db.QueryRowContext(root, "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME<>'PRIMARY'", r.ShadowDB, r.Table).Scan(&count); e != nil {
			return e
		}
		if count > 0 {
			if count != len(names) {
				return errors.New("unexpected shadow index state")
			}
			clauses := []string{}
			for _, n := range names {
				clauses = append(clauses, "DROP INDEX "+qi(n))
			}
			if e = save(r, "defer_secondary_indexes"); e != nil {
				return e
			}
			if e = runDDL("ALTER TABLE " + qt(r.ShadowDB, r.Table) + " " + strings.Join(clauses, ", ")); e != nil {
				return e
			}
		}
		if e = save(r, "copy_primary_ranges"); e != nil {
			return e
		}
		for {
			var last sql.NullInt64
			if e = db.QueryRowContext(root, "SELECT MAX(id) FROM "+qt(r.ShadowDB, r.Table)).Scan(&last); e != nil {
				return e
			}
			r.CopyLastID = uint64(last.Int64)
			var affected int64
			e = operation(2*time.Minute, func(ctx context.Context) error {
				out, er := db.ExecContext(ctx, "INSERT INTO "+qt(r.ShadowDB, r.Table)+" SELECT * FROM "+qt(sourceDB, r.Table)+" FORCE INDEX(PRIMARY) WHERE id>? ORDER BY id LIMIT 100000", r.CopyLastID)
				if er != nil {
					return er
				}
				affected, er = out.RowsAffected()
				return er
			})
			if e != nil {
				return e
			}
			if e = save(r, "copy_primary_ranges"); e != nil {
				return e
			}
			if affected == 0 {
				break
			}
			select {
			case <-root.Done():
				return root.Err()
			case <-time.After(300 * time.Millisecond):
			}
		}
		if e = save(r, "compress_and_build_indexes"); e != nil {
			return e
		}
		clauses := []string{"ROW_FORMAT=COMPRESSED", "KEY_BLOCK_SIZE=8"}
		if r.AutoIncrement > 0 {
			clauses = append(clauses, fmt.Sprintf("AUTO_INCREMENT=%d", r.AutoIncrement))
		}
		for _, v := range r.Secondary {
			clauses = append(clauses, "ADD "+v)
		}
		statement := "ALTER TABLE " + qt(r.ShadowDB, r.Table) + " " + strings.Join(clauses, ", ")
		e = runDDL(statement + ", ALGORITHM=INPLACE, LOCK=NONE")
		var algorithmError *mysql.MySQLError
		if errors.As(e, &algorithmError) && algorithmError.Number == 1846 {
			// Functional indexes add hidden virtual columns. MySQL cannot combine
			// that action with row-format conversion using INPLACE. COPY locks
			// only the private shadow, leaving the business source available.
			if er := save(r, "compress_and_build_indexes_copy"); er != nil {
				return er
			}
			e = runDDL(statement + ", ALGORITHM=COPY, LOCK=SHARED")
		}
		if e != nil {
			return e
		}
	}
	if e = save(r, "verify_compressed_shadow"); e != nil {
		return e
	}
	r.Compressed, e = hashTable(db, r.ShadowDB, r.Table)
	if e != nil {
		return e
	}
	if !same(r.Source, r.Compressed) {
		return errors.New("compressed shadow full SHA256 differs; original retained")
	}
	sig, e := schema(r.ShadowDB, r.Table)
	if e != nil {
		return e
	}
	if sig != r.SchemaSHA {
		return errors.New("compressed column/index definitions differ")
	}
	r.AfterBytes, shadowFormat, _, e = size(r.ShadowDB, r.Table)
	if e != nil {
		return e
	}
	if !strings.EqualFold(shadowFormat, "Compressed") {
		return errors.New("shadow is not compressed")
	}
	if r.AfterBytes >= r.BeforeBytes {
		if e = save(r, "no_savings"); e != nil {
			return e
		}
		if e = runDDL("DROP TABLE " + qt(r.ShadowDB, r.Table)); e != nil {
			return e
		}
		return finish(r, "skipped_no_savings")
	}
	if e = save(r, "ready_for_cutover"); e != nil {
		return e
	}
	e = operation(10*time.Minute, func(ctx context.Context) error {
		conn, er := db.Conn(ctx)
		if er != nil {
			return er
		}
		defer conn.Close()
		if _, er = conn.ExecContext(ctx, "SET SESSION lock_wait_timeout=5"); er != nil {
			return er
		}
		// Only this closed daily table is locked. Current-day ingestion stays writable.
		// Keep both names WRITE-locked across the final source hash and atomic rename,
		// preventing an external historical writer from slipping between the checks.
		if _, er = conn.ExecContext(ctx, "LOCK TABLES "+qt(sourceDB, r.Table)+" WRITE, "+qt(r.ShadowDB, r.Table)+" WRITE"); er != nil {
			return er
		}
		defer conn.ExecContext(context.Background(), "UNLOCK TABLES")
		current, er := ddl(ctx, conn, sourceDB, r.Table)
		if er != nil {
			return er
		}
		if current != r.OriginalDDL {
			return errors.New("source definition changed during migration")
		}
		f, er := digest(ctx, conn, "SELECT * FROM "+qt(sourceDB, r.Table)+" FORCE INDEX(PRIMARY) ORDER BY id")
		if er != nil {
			return er
		}
		if !same(f, r.Source) {
			return errors.New("source changed during migration; original retained")
		}
		if er = save(r, "swap_intent"); er != nil {
			return er
		}
		_, er = conn.ExecContext(ctx, "RENAME TABLE "+qt(sourceDB, r.Table)+" TO "+qt(jobDB, r.Backup)+", "+qt(r.ShadowDB, r.Table)+" TO "+qt(sourceDB, r.Table))
		return er
	})
	if e != nil {
		return e
	}
	if e = save(r, "promoted"); e != nil {
		return e
	}
	return promoted(r)
}
func promoted(r *tableReport) error {
	if r.Source.SHA256 == "" || !same(r.Source, r.Compressed) {
		return errors.New("missing pre-cutover verification; preserving all copies")
	}
	if e := save(r, "verify_promoted"); e != nil {
		return e
	}
	f, e := hashTable(db, sourceDB, r.Table)
	if e != nil {
		return e
	}
	if !same(f, r.Source) {
		return errors.New("promoted data differs; backup retained, stopped for review")
	}
	r.Promoted = f
	sig, e := schema(sourceDB, r.Table)
	if e != nil {
		return e
	}
	if sig != r.SchemaSHA {
		return errors.New("promoted schema differs; backup retained")
	}
	n, format, _, e := size(sourceDB, r.Table)
	if e != nil {
		return e
	}
	if !strings.EqualFold(format, "Compressed") {
		return errors.New("promoted table not compressed; backup retained")
	}
	r.AfterBytes = n
	if e = save(r, "verified"); e != nil {
		return e
	}
	old, e := exists(jobDB, r.Backup)
	if e != nil {
		return e
	}
	if old {
		if e = save(r, "drop_backup"); e != nil {
			return e
		}
		if e = runDDL("DROP TABLE " + qt(jobDB, r.Backup)); e != nil {
			return e
		}
	}
	return finish(r, "complete")
}
func openDB() error {
	var doc map[string]any
	if _, e := toml.DecodeFile(cfgPath, &doc); e != nil {
		return e
	}
	v, ok := doc["database"].(map[string]any)
	if !ok {
		return errors.New("database configuration missing")
	}
	get := func(k string) string { x, _ := v[k].(string); return x }
	sourceDB = get("db")
	if !simpleName.MatchString(sourceDB) || !simpleName.MatchString(jobDB) || sourceDB == jobDB {
		return errors.New("invalid source/job database")
	}
	c := mysql.NewConfig()
	c.User = get("user")
	c.Passwd = get("password")
	c.Net = "tcp"
	c.Addr = get("host")
	c.DBName = sourceDB
	c.ParseTime = true
	c.ReadTimeout = 35 * time.Minute
	c.WriteTimeout = 35 * time.Minute
	var e error
	db, e = sql.Open("mysql", c.FormatDSN())
	if e != nil {
		return e
	}
	db.SetMaxOpenConns(3)
	return db.PingContext(root)
}
func makePlan() ([]string, error) {
	path := filepath.Join(workDir, "plan.json")
	b, e := os.ReadFile(path)
	if e == nil {
		var p []string
		e = json.Unmarshal(b, &p)
		return p, e
	}
	if !os.IsNotExist(e) {
		return nil, e
	}
	rows, e := db.QueryContext(root, "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' AND ENGINE='InnoDB'", sourceDB)
	if e != nil {
		return nil, e
	}
	var p []string
	for rows.Next() {
		var t string
		if e = rows.Scan(&t); e != nil {
			rows.Close()
			return nil, e
		}
		if validTable(t, time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -30)) {
			p = append(p, t)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	sort.Slice(p, func(i, j int) bool {
		a, b := p[i][len(p[i])-6:], p[j][len(p[j])-6:]
		if a == b {
			return p[i] < p[j]
		}
		return a > b
	})
	first := []string{"transactions_260601", "user_token_stats_260601", "from_stats_260601", "to_stats_260601"}
	out := []string{}
	seen := map[string]bool{}
	for _, a := range first {
		for _, b := range p {
			if a == b {
				out = append(out, a)
				seen[a] = true
			}
		}
	}
	for _, t := range p {
		if !seen[t] {
			out = append(out, t)
		}
	}
	return out, writeJSON(path, out)
}
func recordFailure(e error) {
	if jobInitialized {
		if er := saveSummary(); er != nil {
			log.Printf("save failure report: %v", er)
		}
		return
	}
	// A failed duplicate launch, unavailable DB or wrong config must never
	// replace the durable owner summary from an existing job.
	er := writeJSON(filepath.Join(workDir, "startup-error.json"), map[string]string{"TimeUTC": stamp(), "Error": e.Error()})
	if er != nil {
		log.Printf("save startup error: %v", er)
	}
}

func main() {
	flag.StringVar(&cfgPath, "config", "/data/tracker/config.toml", "existing tracker configuration")
	flag.StringVar(&workDir, "work-dir", "/data/tracker/cold-compress-20261111", "durable job reports")
	flag.StringVar(&jobDB, "job-db", "tracker_cold_compress_20261111", "private shadow/backup schema")
	flag.StringVar(&tmpDir, "tmp-dir", "/data/mysql-cold-compress-tmp", "MySQL-owned directory outside datadir")
	flag.BoolVar(&execute, "execute", false, "explicitly execute the serial migration")
	flag.BoolVar(&ignoreWindow, "ignore-window", false, "test-only: bypass the UTC 23-03 blackout")
	flag.IntVar(&maxTables, "max-tables", 0, "stop after this many newly completed tables; zero means full fixed plan")
	flag.BoolVar(&selfTest, "self-test", false, "exercise copy, locked cross-schema cutover, recovery and source-write rejection in disposable schemas")
	flag.Parse()
	root, cancelRoot = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancelRoot()
	if e := run(); e != nil {
		sum.Status = "failed"
		if root.Err() != nil {
			sum.Status = "stopped"
		}
		sum.Error = e.Error()
		recordFailure(e)
		log.Print(e)
		if db != nil {
			db.Close()
		}
		os.Exit(1)
	}
	if db != nil {
		db.Close()
	}
}
func run() error {
	if e := os.MkdirAll(filepath.Join(workDir, "tables"), 0700); e != nil {
		return e
	}
	if e := openDB(); e != nil {
		return e
	}
	if selfTest {
		return runSelfTest()
	}
	lock, e := db.Conn(root)
	if e != nil {
		return e
	}
	defer lock.Close()
	var acquired int
	if e = lock.QueryRowContext(root, "SELECT GET_LOCK(?,0)", "tracker_serial_cold_compress_v1").Scan(&acquired); e != nil {
		return e
	}
	if acquired != 1 {
		return errors.New("another cold compression worker holds the lock")
	}
	p, e := makePlan()
	if e != nil {
		return e
	}
	sum = summary{Status: "planned", StartedUTC: stamp(), CutoffUTC: time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -30).Format("2006-01-02"), SourceDatabase: sourceDB, JobDatabase: jobDB, Planned: len(p)}
	if b, er := os.ReadFile(filepath.Join(workDir, "summary.json")); er == nil {
		var old summary
		if json.Unmarshal(b, &old) != nil {
			return errors.New("invalid saved job summary")
		}
		if old.SourceDatabase != sourceDB || old.JobDatabase != jobDB {
			return errors.New("saved job belongs to another database")
		}
		if old.StartedUTC != "" {
			sum.StartedUTC = old.StartedUTC
		}
		if old.CutoffUTC != "" {
			sum.CutoffUTC = old.CutoffUTC
		}
	}
	jobInitialized = true
	if e = saveSummary(); e != nil {
		return e
	}
	log.Printf("plan: %d daily tables, cutoff %s", len(p), sum.CutoffUTC)
	if !execute {
		return nil
	}
	if _, e = db.ExecContext(root, "CREATE DATABASE IF NOT EXISTS "+qi(jobDB)+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); e != nil {
		return e
	}
	if _, e = db.ExecContext(root, "CREATE TABLE IF NOT EXISTS "+qt(jobDB, "_job_state")+" (name varchar(16) NOT NULL PRIMARY KEY,payload longtext NOT NULL) ENGINE=InnoDB"); e != nil {
		return e
	}
	publishReady = true
	publish("summary", &sum)
	ctl = &controller{owner: lock}
	ctl.sample()
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-root.Done():
				return
			case <-tick.C:
				ctl.sample()
			}
		}
	}()
	fresh := 0
	for _, table := range p {
		r, er := load(table)
		if er != nil {
			return er
		}
		doneBefore := r.Status == "complete"
		tries := 0
		for {
			er = migrate(r)
			if errors.Is(er, errPause) {
				log.Printf("%s interrupted for health/window; preserving checkpoint", table)
				if e = ctl.gate(); e != nil {
					return e
				}
				continue
			}
			var me *mysql.MySQLError
			if errors.As(er, &me) && (me.Number == 1205 || me.Number == 1213) && tries < 5 {
				tries++
				log.Printf("%s lock retry %d", table, tries)
				select {
				case <-root.Done():
					return root.Err()
				case <-time.After(30 * time.Second):
				}
				continue
			}
			break
		}
		if er != nil {
			r.Status = "failed"
			r.Error = er.Error()
			save(r, r.Phase)
			return fmt.Errorf("%s: %w", table, er)
		}
		if r.Status == "complete" {
			sum.Completed++
			if r.BeforeBytes > r.AfterBytes {
				sum.ReclaimedBytes += r.BeforeBytes - r.AfterBytes
			}
			if !doneBefore {
				fresh++
			}
		} else {
			sum.Skipped++
		}
		if e = saveSummary(); e != nil {
			return e
		}
		if maxTables > 0 && fresh >= maxTables {
			sum.Status = "batch_limit"
			return saveSummary()
		}
	}
	sum.Status = "complete"
	sum.Phase = "all planned tables processed"
	sum.CurrentTable = ""
	sum.Error = ""
	return saveSummary()
}
