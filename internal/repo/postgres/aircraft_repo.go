package postgres

import (
	"backend/domain"
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type aircraftRepository struct {
	db *sql.DB // database connection pool
}

func NewAircraftRepository(db *sql.DB) domain.AircraftRepository {
	return &aircraftRepository{
		db: db,
	}
}

func (r *aircraftRepository) SaveAircraftFrame(ctx context.Context, aircrafts []domain.Aircraft) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("Failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// cau lenh de them hoac cap nhat vao bang aircraft
	query1 := `	insert into aircraft (id, category, last_lat, last_lng, last_alt, last_timestamp)
				values ($1, $2, $3, $4, $5, $6)
				on conflict (id) do update set
					category = EXCLUDED.category,
					last_lat = EXCLUDED.last_lat,
					last_lng = EXCLUDED.last_lng,
					last_alt = EXCLUDED.last_alt,
					last_timestamp = EXCLUDED.last_timestamp
			`
	stmtCurrent, err := tx.PrepareContext(ctx, query1)
	if err != nil {
		return fmt.Errorf("fail to prepare context query1: %w", err)
	}
	defer stmtCurrent.Close()

	query2 := `insert into history_position (aircraft_id, lat, lng, alt, timestamp)
				values ($1, $2, $3, $4, $5)
			`
	stmtHistory, err := tx.PrepareContext(ctx, query2)
	if err != nil {
		return fmt.Errorf("fail to prepare context: %w", err)
	}
	defer stmtHistory.Close()

	// duyet qua tung may bay va thuc hien cau lenh
	for _, aircraft := range aircrafts {
		_, err := stmtCurrent.ExecContext(ctx, aircraft.Id, aircraft.Category, aircraft.Lat, aircraft.Lng, aircraft.Alt, aircraft.Timestamp)
		if err != nil {
			return fmt.Errorf("fail to execute current statement: %w", err)
		}
		_, err = stmtHistory.ExecContext(ctx, aircraft.Id, aircraft.Lat, aircraft.Lng, aircraft.Alt, aircraft.Timestamp)
		if err != nil {
			return fmt.Errorf("fail to execute history statement: %w", err)
		}
	}
	return tx.Commit()
}

func (r *aircraftRepository) GetAircraft(ctx context.Context, id int) (domain.Aircraft, error) {
	var ac domain.Aircraft
	query := `
		select id, last_lat, last_lng, last_alt, category, last_timestamp
		from aircraft
		where id = $1
	`

	err := r.db.QueryRowContext(ctx, query, id).Scan(&ac.Id, &ac.Lat, &ac.Lng, &ac.Alt, &ac.Category, &ac.Timestamp)
	if err != nil {
		return domain.Aircraft{}, fmt.Errorf("failed to get aircraft: %w", err)
	}

	return ac, nil
}

func (r *aircraftRepository) DeleteAircrafts(ctx context.Context, ids []int32) error {
	query := `delete from aircraft where id = any($1)`
	res, err := r.db.ExecContext(ctx, query, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("failed to delete aircraft: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("deleted %d/%d aircraft", 0, len(ids))
	}
	return nil
}

func (r *aircraftRepository) GetHistoryPositions(ctx context.Context, aircraftId int) ([]domain.Aircraft, error) {
	query := `
		select lat, lng, alt, timestamp
		from history_position
		where aircraft_id = $1
		order by timestamp desc
	`
	rows, err := r.db.QueryContext(ctx, query, aircraftId)
	if err != nil {
		return nil, fmt.Errorf("failed to get history positions: %w", err)
	}
	defer rows.Close()

	var history []domain.Aircraft
	for rows.Next() {
		var ac domain.Aircraft
		err := rows.Scan(&ac.Lat, &ac.Lng, &ac.Alt, &ac.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan history position: %w", err)
		}
		ac.Id = aircraftId
		history = append(history, ac)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating history positions: %w", err)
	}

	return history, nil

}
