package main

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
)

// ProgressTracker tracks backfill progress and finds gaps
type ProgressTracker struct {
	fromLedger int
	toLedger   int
}

func NewProgressTracker(fromLedger, toLedger int) *ProgressTracker {
	return &ProgressTracker{
		fromLedger: fromLedger,
		toLedger:   toLedger,
	}
}

// FindGaps finds ledger indices that are missing in the specified range
// Returns a list of missing ledger indices (excluding ledgers in empty_ledgers table)
// Note: Assumes ClickHouse connection is already initialized and will be closed by caller
func (pt *ProgressTracker) FindGaps() ([]int, error) {
	log.Printf("[PROGRESS] Finding gaps in range %d-%d", pt.fromLedger, pt.toLedger)

	// Ensure ClickHouse connection is initialized
	if connections.ClickHouseConn == nil {
		config.EnvLoad()
		logger.New()
		connections.NewClickHouseConnection()
	}

	ctx := context.Background()

	// Query ClickHouse to get all indexed ledger indices in the range
	// We query money_flow table which contains ledger_index
	query := fmt.Sprintf(`
		SELECT DISTINCT ledger_index 
		FROM xrpl.money_flow 
		WHERE ledger_index >= %d AND ledger_index <= %d 
		ORDER BY ledger_index
	`, pt.fromLedger, pt.toLedger)

	rows, err := connections.ClickHouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query ClickHouse: %w", err)
	}
	defer rows.Close()

	// Collect all indexed ledger indices
	indexedLedgers := make(map[int]bool)
	for rows.Next() {
		var ledgerIndex uint32
		if err := rows.Scan(&ledgerIndex); err != nil {
			return nil, fmt.Errorf("failed to scan ledger index: %w", err)
		}
		indexedLedgers[int(ledgerIndex)] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Query empty_ledgers table to get ledgers that were checked but have no Payment transactions
	emptyQuery := fmt.Sprintf(`
		SELECT DISTINCT ledger_index 
		FROM xrpl.empty_ledgers 
		WHERE ledger_index >= %d AND ledger_index <= %d 
		ORDER BY ledger_index
	`, pt.fromLedger, pt.toLedger)

	emptyRows, err := connections.ClickHouseConn.Query(ctx, emptyQuery)
	if err != nil {
		// If table doesn't exist yet, that's okay - just log and continue
		log.Printf("[PROGRESS] Could not query empty_ledgers table (may not exist yet): %v", err)
	} else {
		defer emptyRows.Close()

		// Collect all empty ledger indices (these are intentionally skipped)
		for emptyRows.Next() {
			var ledgerIndex uint32
			if err := emptyRows.Scan(&ledgerIndex); err != nil {
				log.Printf("[PROGRESS] Failed to scan empty ledger index: %v", err)
				continue
			}
			indexedLedgers[int(ledgerIndex)] = true // Mark as "processed" (even though empty)
		}

		if err := emptyRows.Err(); err != nil {
			log.Printf("[PROGRESS] Error iterating empty ledger rows: %v", err)
		}
	}

	// Find gaps - ledgers that should be in range but are not indexed and not in empty_ledgers
	gaps := []int{}
	for ledgerIndex := pt.fromLedger; ledgerIndex <= pt.toLedger; ledgerIndex++ {
		if !indexedLedgers[ledgerIndex] {
			gaps = append(gaps, ledgerIndex)
		}
	}

	log.Printf("[PROGRESS] Found %d gaps out of %d total ledgers in range %d-%d",
		len(gaps), pt.toLedger-pt.fromLedger+1, pt.fromLedger, pt.toLedger)

	return gaps, nil
}

// GetRemainingRange calculates the remaining range that needs to be processed
// It finds gaps and returns the range from the first gap to toLedger
func (pt *ProgressTracker) GetRemainingRange() (int, int, error) {
	log.Printf("[PROGRESS] Calculating remaining range from %d to %d", pt.fromLedger, pt.toLedger)

	// Ensure ClickHouse connection is initialized
	// Note: Connection should be initialized by orchestrator, but we check just in case
	if connections.ClickHouseConn == nil {
		config.EnvLoad()
		logger.New()
		connections.NewClickHouseConnection()
	}
	// Don't close connection here - it's managed by orchestrator

	// Find gaps in the range
	gaps, err := pt.FindGaps()
	if err != nil {
		return pt.fromLedger, pt.toLedger, err
	}

	if len(gaps) == 0 {
		// No gaps found, check if we've reached the end
		// Find the last indexed ledger
		lastQuery := fmt.Sprintf(`
			SELECT MAX(ledger_index) as last_indexed
			FROM (
				SELECT DISTINCT ledger_index 
				FROM xrpl.money_flow 
				WHERE ledger_index >= %d AND ledger_index <= %d
			)
		`, pt.fromLedger, pt.toLedger)

		ctx := context.Background()
		var lastIndexed *uint32
		err := connections.ClickHouseConn.QueryRow(ctx, lastQuery).Scan(&lastIndexed)
		if err != nil || lastIndexed == nil {
			// No ledgers indexed at all, return full range
			log.Printf("[PROGRESS] No indexed ledgers found, starting from %d", pt.fromLedger)
			return pt.fromLedger, pt.toLedger, nil
		}

		if int(*lastIndexed) < pt.toLedger {
			// Continue from after the last indexed ledger
			remainingFrom := int(*lastIndexed) + 1
			log.Printf("[PROGRESS] All ledgers up to %d are indexed, continuing from %d",
				*lastIndexed, remainingFrom)
			return remainingFrom, pt.toLedger, nil
		} else {
			// All ledgers in range are indexed
			log.Printf("[PROGRESS] All ledgers in range %d-%d are indexed", pt.fromLedger, pt.toLedger)
			return pt.toLedger + 1, pt.toLedger, nil
		}
	}

	// Return range starting from first gap to last gap
	// This ensures we process all gaps, not just from first gap to end
	remainingFrom := gaps[0]
	remainingTo := gaps[len(gaps)-1]

	// Log information about gaps for debugging
	if len(gaps) <= 20 {
		// If not too many gaps, show them all
		log.Printf("[PROGRESS] Found %d gaps: %v", len(gaps), gaps)
	} else {
		// Show first and last few gaps
		firstGaps := gaps[:10]
		lastGaps := gaps[len(gaps)-10:]
		log.Printf("[PROGRESS] Found %d gaps (showing first 10 and last 10): first=%v ... last=%v",
			len(gaps), firstGaps, lastGaps)
	}

	log.Printf("[PROGRESS] Gap range: %d-%d (from first gap to last gap, total gaps: %d)",
		remainingFrom, remainingTo, len(gaps))
	log.Printf("[PROGRESS] Will process range %d-%d to fill all gaps", remainingFrom, remainingTo)
	return remainingFrom, remainingTo, nil
}

// SplitRange splits a range into N approximately equal parts
func SplitRange(from, to, n int) []Range {
	if n <= 0 {
		return []Range{{From: from, To: to}}
	}

	ranges := make([]Range, 0, n)
	total := to - from + 1
	chunkSize := total / n
	remainder := total % n

	currentFrom := from
	for i := 0; i < n; i++ {
		chunk := chunkSize
		if i < remainder {
			chunk++
		}

		currentTo := currentFrom + chunk - 1
		if currentTo > to {
			currentTo = to
		}

		ranges = append(ranges, Range{
			From: currentFrom,
			To:   currentTo,
		})

		currentFrom = currentTo + 1
		if currentFrom > to {
			break
		}
	}

	return ranges
}

type Range struct {
	From int
	To   int
}

// SortGaps sorts gaps and groups consecutive gaps into ranges
func GroupGapsIntoRanges(gaps []int) []Range {
	if len(gaps) == 0 {
		return []Range{}
	}

	// Sort gaps
	sort.Ints(gaps)

	ranges := []Range{}
	start := gaps[0]
	end := gaps[0]

	for i := 1; i < len(gaps); i++ {
		if gaps[i] == end+1 {
			// Consecutive gap, extend range
			end = gaps[i]
		} else {
			// Gap in sequence, save current range and start new one
			ranges = append(ranges, Range{From: start, To: end})
			start = gaps[i]
			end = gaps[i]
		}
	}

	// Add last range
	ranges = append(ranges, Range{From: start, To: end})

	return ranges
}

// CountRemainingLedgers counts how many ledgers are still not indexed in a given range
// Returns the count of remaining ledgers (excluding empty_ledgers)
func CountRemainingLedgers(fromLedger, toLedger int) (int, error) {
	// Ensure ClickHouse connection is initialized
	if connections.ClickHouseConn == nil {
		config.EnvLoad()
		logger.New()
		connections.NewClickHouseConnection()
	}

	ctx := context.Background()

	// Count indexed ledgers (from money_flow and empty_ledgers)
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT ledger_index) as indexed_count
		FROM (
			SELECT DISTINCT ledger_index 
			FROM xrpl.money_flow 
			WHERE ledger_index >= %d AND ledger_index <= %d
			UNION ALL
			SELECT DISTINCT ledger_index 
			FROM xrpl.empty_ledgers 
			WHERE ledger_index >= %d AND ledger_index <= %d
		)
	`, fromLedger, toLedger, fromLedger, toLedger)

	var indexedCount *uint64
	err := connections.ClickHouseConn.QueryRow(ctx, query).Scan(&indexedCount)
	if err != nil {
		// If query fails (e.g., empty_ledgers table doesn't exist), try without it
		query = fmt.Sprintf(`
			SELECT COUNT(DISTINCT ledger_index) as indexed_count
			FROM xrpl.money_flow 
			WHERE ledger_index >= %d AND ledger_index <= %d
		`, fromLedger, toLedger)
		err = connections.ClickHouseConn.QueryRow(ctx, query).Scan(&indexedCount)
		if err != nil {
			return 0, fmt.Errorf("failed to count indexed ledgers: %w", err)
		}
	}

	totalLedgers := toLedger - fromLedger + 1
	var indexed int
	if indexedCount != nil {
		indexed = int(*indexedCount)
	}
	remaining := totalLedgers - indexed

	return remaining, nil
}
