package ghostferry

import (
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/sirupsen/logrus"
)

type BinlogWriter struct {
	DB               *sql.DB
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	Throttler        Throttler

	BatchSize    int
	WriteRetries int

	ErrorHandler ErrorHandler
	StateTracker *StateTracker

	binlogEventBuffer chan DMLEvent
	// Useful for debugging binlog writer lag, if diverged from binlog streamer lag
	lastProcessedEventTime time.Time
	logger                 *logrus.Entry
	lastSeenSchemaByTable  TableSchemaCache
}

func (b *BinlogWriter) Run() {
	b.logger = logrus.WithField("tag", "binlog_writer")
	b.binlogEventBuffer = make(chan DMLEvent, b.BatchSize)

	batch := make([]DMLEvent, 0, b.BatchSize)
	for {
		firstEvent := <-b.binlogEventBuffer
		if firstEvent == nil {
			// Channel is closed, no more events to write
			break
		}

		batch = append(batch, firstEvent)
		wantMoreEvents := true
		for wantMoreEvents && len(batch) < b.BatchSize {
			select {
			case event := <-b.binlogEventBuffer:
				if event != nil {
					batch = append(batch, event)
				} else {
					// Channel is closed, finish writing batch.
					wantMoreEvents = false
				}
			default: // Nothing in the buffer so just write it
				wantMoreEvents = false
			}
		}

		err := WithRetries(b.WriteRetries, 0, b.logger, "write events to target", func() error {
			return b.writeEvents(batch)
		})
		if err != nil {
			b.ErrorHandler.Fatal("binlog_writer", err)
		}

		batch = make([]DMLEvent, 0, b.BatchSize)
	}
}

func (b *BinlogWriter) Stop() {
	close(b.binlogEventBuffer)
}

func (b *BinlogWriter) BufferBinlogEvents(events []DMLEvent) error {
	for _, event := range events {
		b.binlogEventBuffer <- event
	}

	return nil
}

func (b *BinlogWriter) writeEvents(events []DMLEvent) error {
	WaitForThrottle(b.Throttler)

	queryBuffer := []byte(sql.AnnotateStmt("BEGIN;\n", b.DB.Marginalia))

	for _, ev := range events {
		eventDatabaseName := ev.Database()
		if targetDatabaseName, exists := b.DatabaseRewrites[eventDatabaseName]; exists {
			eventDatabaseName = targetDatabaseName
		}

		eventTableName := ev.Table()
		if targetTableName, exists := b.TableRewrites[eventTableName]; exists {
			eventTableName = targetTableName
		}

		// tableName => lastTableSchemaSeed

		// lastTableSchemaSeed != e.schema
		//   reload target??

		// target = (id, shop_id, blah)
		// source = (id, shop_id, blah)

		table, found := b.lastSeenSchemaByTable[fmt.Sprintf("%s.%s", eventDatabaseName, eventTableName)]
		if found {
			if table != ev.TableSchema() {
				// reload and compare again
				// return fmt.Errorf("incompatible!")
			}
		} else {
			b.lastSeenSchemaByTable[fmt.Sprintf("%s.%s", eventDatabaseName, eventTableName)] = e.table
		}

		sqlStmt, err := ev.AsSQLString(eventDatabaseName, eventTableName)
		if err != nil {
			return fmt.Errorf("generating sql query at pos %v: %v", ev.BinlogPosition(), err)
		}

		queryBuffer = append(queryBuffer, sql.AnnotateStmt(sqlStmt, b.DB.Marginalia)...)
		queryBuffer = append(queryBuffer, ";\n"...)
	}

	queryBuffer = append(queryBuffer, "COMMIT"...)

	startEv := events[0]
	endEv := events[len(events)-1]
	query := string(queryBuffer)
	_, err := b.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("exec query at pos %v -> %v (%d bytes): %v", startEv.BinlogPosition(), endEv.BinlogPosition(), len(query), err)
	}

	if b.StateTracker != nil {
		b.StateTracker.UpdateLastResumableSourceBinlogPosition(events[len(events)-1].ResumableBinlogPosition())
	}

	b.lastProcessedEventTime = events[len(events)-1].Timestamp()

	return nil
}
