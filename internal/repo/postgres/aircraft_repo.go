package postgres

import (
	"backend/domain"
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/lib/pq"
)

type aircraftRepository struct {
	db *sql.DB // database connection pool
}

type flightKey struct {
	callsign      string
	detectionTime int64
}

func NewAircraftRepository(db *sql.DB) domain.AircraftRepository {
	return &aircraftRepository{
		db: db,
	}
}

func latestAircraftByFlight(aircrafts []domain.Aircraft) []domain.Aircraft {
	latestByKey := make(map[flightKey]domain.Aircraft, len(aircrafts))
	for _, ac := range aircrafts {
		key := flightKey{callsign: ac.Callsign, detectionTime: ac.DetectionTime}
		current, exists := latestByKey[key]
		if !exists || ac.LastTimestamp >= current.LastTimestamp {
			latestByKey[key] = ac
		}
	}

	latest := make([]domain.Aircraft, 0, len(latestByKey))
	for _, ac := range latestByKey {
		latest = append(latest, ac)
	}
	sort.Slice(latest, func(i, j int) bool {
		if latest[i].Callsign == latest[j].Callsign {
			return latest[i].DetectionTime < latest[j].DetectionTime
		}
		return latest[i].Callsign < latest[j].Callsign
	})
	return latest
}

func (r *aircraftRepository) SaveAircraftColdData(ctx context.Context, aircrafts []domain.Aircraft) error {
	if len(aircrafts) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	querySummary := `
		INSERT INTO archived_flight_summary (callsign, detection_time, category, classification, start_time, end_time)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (callsign, detection_time) DO UPDATE SET
			end_time = GREATEST(archived_flight_summary.end_time, EXCLUDED.end_time),
			category = EXCLUDED.category,
			classification = EXCLUDED.classification
	`
	stmtSummary, err := tx.PrepareContext(ctx, querySummary)
	if err != nil {
		return fmt.Errorf("fail to prepare context summary: %w", err)
	}
	defer stmtSummary.Close()

	for _, ac := range latestAircraftByFlight(aircrafts) {
		_, err := stmtSummary.ExecContext(ctx,
			ac.Callsign, ac.DetectionTime, ac.Category, ac.Classification,
			ac.DetectionTime, ac.LastTimestamp,
		)
		if err != nil {
			return fmt.Errorf("fail to execute summary statement for %s: %w", ac.Callsign, err)
		}

	}

	copyStmt, err := tx.PrepareContext(ctx, pq.CopyIn(
		"archived_position",
		"aircraft_callsign",
		"aircraft_detection_time",
		"lat",
		"lng",
		"alt",
		"speed",
		"heading",
		"timestamp",
	))
	if err != nil {
		return fmt.Errorf("fail to prepare archived position copy: %w", err)
	}

	for _, ac := range aircrafts {
		_, err = copyStmt.ExecContext(ctx,
			ac.Callsign, ac.DetectionTime, ac.LastLat, ac.LastLng, ac.LastAlt, ac.Speed, ac.Heading, ac.LastTimestamp,
		)
		if err != nil {
			return fmt.Errorf("fail to execute archived position statement for %s: %w", ac.Callsign, err)
		}
	}
	if _, err = copyStmt.ExecContext(ctx); err != nil {
		copyStmt.Close()
		return fmt.Errorf("fail to flush archived position copy: %w", err)
	}
	if err = copyStmt.Close(); err != nil {
		return fmt.Errorf("fail to close archived position copy: %w", err)
	}

	return tx.Commit()
}

func (r *aircraftRepository) MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error {
	query := `
		UPDATE archived_position 
		SET is_permanent = true 
		WHERE aircraft_callsign = $1 
		  AND aircraft_detection_time = $2
		  AND timestamp >= $3 
		  AND timestamp <= $4
	`
	res, err := r.db.ExecContext(ctx, query, callsign, detectionTime, fromTs, toTs)
	if err != nil {
		return fmt.Errorf("failed to mark positions as permanent: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	fmt.Printf("Pinned %d positions for aircraft %s between %d and %d\n", rowsAffected, callsign, fromTs, toTs)

	return nil
}

// Don dep bang cold data
func (r *aircraftRepository) CleanupExpiredPositions(ctx context.Context, cutoffTimestamp int64) error {
	query := `
		DELETE FROM archived_position 
		WHERE is_permanent = false AND timestamp < $1
	`
	res, err := r.db.ExecContext(ctx, query, cutoffTimestamp)
	if err != nil {
		return fmt.Errorf("failed to clean up expired positions: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected > 0 {
		fmt.Printf("Garbage Collector: Cleared %d expired coordinate points\n", rowsAffected)
	}

	return nil
}

func (r *aircraftRepository) SaveAircraftHotData(ctx context.Context, aircrafts []domain.Aircraft) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Cập nhật bảng hot data aircraft
	query1 := `
		INSERT INTO aircraft (callsign, detection_time, category, mode_3a, classification, last_lat, last_lng, last_alt, last_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (callsign, detection_time) DO UPDATE SET
			category = EXCLUDED.category,
			mode_3a = EXCLUDED.mode_3a,
			classification = EXCLUDED.classification,
			last_lat = EXCLUDED.last_lat,
			last_lng = EXCLUDED.last_lng,
			last_alt = EXCLUDED.last_alt,
			last_timestamp = EXCLUDED.last_timestamp
		WHERE aircraft.last_timestamp <= EXCLUDED.last_timestamp
	`
	stmtCurrent, err := tx.PrepareContext(ctx, query1)
	if err != nil {
		return fmt.Errorf("fail to prepare context query1: %w", err)
	}
	defer stmtCurrent.Close()

	for _, ac := range latestAircraftByFlight(aircrafts) {
		_, err := stmtCurrent.ExecContext(ctx,
			ac.Callsign, ac.DetectionTime, ac.Category, ac.Mode3A, ac.Classification,
			ac.LastLat, ac.LastLng, ac.LastAlt, ac.LastTimestamp,
		)
		if err != nil {
			return fmt.Errorf("fail to execute current statement for %s: %w", ac.Callsign, err)
		}

	}

	copyStmt, err := tx.PrepareContext(ctx, pq.CopyIn(
		"history_position",
		"aircraft_callsign",
		"aircraft_detection_time",
		"lat",
		"lng",
		"alt",
		"speed",
		"heading",
		"timestamp",
	))
	if err != nil {
		return fmt.Errorf("fail to prepare history copy: %w", err)
	}

	for _, ac := range aircrafts {
		_, err = copyStmt.ExecContext(ctx,
			ac.Callsign, ac.DetectionTime, ac.LastLat, ac.LastLng, ac.LastAlt, ac.Speed, ac.Heading, ac.LastTimestamp,
		)
		if err != nil {
			return fmt.Errorf("fail to execute history statement for %s: %w", ac.Callsign, err)
		}
	}
	if _, err = copyStmt.ExecContext(ctx); err != nil {
		copyStmt.Close()
		return fmt.Errorf("fail to flush history copy: %w", err)
	}
	if err = copyStmt.Close(); err != nil {
		return fmt.Errorf("fail to close history copy: %w", err)
	}
	return tx.Commit()
}

func (r *aircraftRepository) GetAircraft(ctx context.Context, callsign string, detectionTime int64) (domain.Aircraft, error) {
	var ac domain.Aircraft
	query := `
		SELECT callsign, detection_time, category, mode_3a, classification, last_lat, last_lng, last_alt, last_timestamp
		FROM aircraft
		WHERE callsign = $1 AND detection_time = $2
	`

	err := r.db.QueryRowContext(ctx, query, callsign, detectionTime).Scan(
		&ac.Callsign, &ac.DetectionTime, &ac.Category, &ac.Mode3A, &ac.Classification,
		&ac.LastLat, &ac.LastLng, &ac.LastAlt, &ac.LastTimestamp,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return domain.Aircraft{}, fmt.Errorf("aircraft not found")
		}
		return domain.Aircraft{}, fmt.Errorf("failed to get aircraft: %w", err)
	}

	return ac, nil
}

func (r *aircraftRepository) DeleteAircrafts(ctx context.Context, callsigns []string) error {
	query := `DELETE FROM aircraft WHERE callsign = ANY($1)`
	res, err := r.db.ExecContext(ctx, query, pq.Array(callsigns))
	if err != nil {
		return fmt.Errorf("failed to delete aircrafts: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("deleted 0/%d aircrafts", len(callsigns))
	}
	return nil
}

func (r *aircraftRepository) GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]domain.Aircraft, error) {
	query := `
		SELECT lat, lng, alt, speed, heading, timestamp
		FROM history_position
		WHERE aircraft_callsign = $1 AND aircraft_detection_time = $2
		ORDER BY timestamp ASC
	`
	rows, err := r.db.QueryContext(ctx, query, callsign, detectionTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get history positions: %w", err)
	}
	defer rows.Close()

	var history []domain.Aircraft
	for rows.Next() {
		var ac domain.Aircraft
		err := rows.Scan(&ac.LastLat, &ac.LastLng, &ac.LastAlt, &ac.Speed, &ac.Heading, &ac.LastTimestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan history position: %w", err)
		}

		ac.Callsign = callsign
		ac.DetectionTime = detectionTime

		history = append(history, ac)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating history positions: %w", err)
	}

	return history, nil
}

func (r *aircraftRepository) GetAllArchivedPositionsByTimeWindow(ctx context.Context, fromTs int64, toTs int64) ([]domain.Aircraft, error) {
	query := `
		SELECT aircraft_callsign, aircraft_detection_time, lat, lng, alt, speed, heading, timestamp, is_permanent
		FROM archived_position
		WHERE timestamp >= $1 AND timestamp <= $2
		ORDER BY timestamp ASC
	`
	rows, err := r.db.QueryContext(ctx, query, fromTs, toTs)
	if err != nil {
		return nil, fmt.Errorf("failed to get all archived positions by time window: %w", err)
	}
	defer rows.Close()

	var flatData []domain.Aircraft
	for rows.Next() {
		var ac domain.Aircraft
		err := rows.Scan(
			&ac.Callsign,
			&ac.DetectionTime,
			&ac.LastLat,
			&ac.LastLng,
			&ac.LastAlt,
			&ac.Speed,
			&ac.Heading,
			&ac.LastTimestamp,
			&ac.IsPermanent,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}
		flatData = append(flatData, ac)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating global positions: %w", err)
	}

	return flatData, nil
}
