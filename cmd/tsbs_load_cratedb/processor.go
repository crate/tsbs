package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	tableDefs   map[string]*tableDef
	connCfg     *pgx.ConnConfig
	conn        *pgx.Conn
	insertStmts map[string]string
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	conn, err := pgx.ConnectConfig(context.Background(), p.connCfg)
	if err != nil {
		fatal("cannot create a new connection pool: %v", err)
		panic(err)
	}
	p.conn = conn
	p.insertStmts = map[string]string{}
}

// createInsertStmt builds a bulk insert reading one array parameter per
// column via unnest — CrateDB's recommended bulk-load form over the
// Postgres wire protocol. Parameter count stays constant regardless of
// batch size (single-row inserts per batch row are ~an order of magnitude
// slower and multi-row VALUES hits the 65535 wire-parameter limit at
// full-profile batch sizes).
//
// Parameters carry explicit array casts — CrateDB's type inference for
// unnest parameters over the wire protocol is unreliable with many columns
// (it JSON-parses timestamp strings as objects without them).
//
//	INSERT INTO t (tags, ts, m1, ...)
//	  (SELECT * FROM unnest(?::array(object), ?::array(timestamp), ?::array(double precision), ...))
func (p *processor) createInsertStmt(table *tableDef) string {
	cols := append([]string{"tags", "ts"}, table.cols...)
	params := make([]string, 0, len(cols))
	params = append(params, "?::array(object)", "?::array(timestamp)")
	for range table.cols {
		params = append(params, "?::array(double precision)")
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) (SELECT * FROM unnest(%s))",
		table.fqn(),
		strings.Join(cols, ","),
		strings.Join(params, ", "),
	)
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	eb := b.(*eventsBatch)
	rowCnt := uint64(0)
	metricCnt := uint64(0)

	for table, rows := range eb.batches {
		rowCnt += uint64(len(rows))
		if doLoad {
			metricCnt += p.InsertBatch(table, rows)
		}
	}
	return metricCnt, rowCnt
}

// load.Processor interface implementation
func (p *processor) InsertBatch(table string, rows []*row) uint64 {
	def := p.tableDefs[table]
	stmt, ok := p.insertStmts[table]
	if !ok {
		stmt = p.createInsertStmt(def)
		p.insertStmts[table] = stmt
	}

	// pivot rows into one array per column: tags (JSON strings, implicitly
	// cast to the object column), timestamps, and one float64 array per metric
	n := len(rows)
	tags := make([]string, n)
	tss := make([]time.Time, n)
	metrics := make([][]float64, len(def.cols))
	for i := range metrics {
		metrics[i] = make([]float64, n)
	}

	metricCnt := uint64(0)
	for i, r := range rows {
		tags[i] = string((*r)[0].([]byte))
		tss[i] = (*r)[1].(time.Time)
		for c := range def.cols {
			metrics[c][i] = (*r)[c+2].(float64)
		}
		// a number of metric values is all row values minus tags and timestamp
		// this is required by the framework to count the number of inserted
		// metric values
		metricCnt += uint64(len(*r) - 2)
	}

	args := make([]interface{}, 0, len(def.cols)+2)
	args = append(args, tags, tss)
	for _, m := range metrics {
		args = append(args, m)
	}

	if _, err := p.conn.Exec(context.Background(), stmt, args...); err != nil {
		fatal("failed to insert batch into %s: %v", table, err)
	}
	return metricCnt
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.conn.Close(context.Background())
	}
}
