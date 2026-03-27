package postgres

import (
	"backend/domain"
	"context"
	"database/sql"
	"fmt"
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
