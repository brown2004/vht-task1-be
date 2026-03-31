package domain

import "context"

const (
	Unknown  int = 0
	Friendly int = 1
	Hostile  int = 2
)

type Aircraft struct {
	Id        int
	Lat       float64
	Lng       float64
	Alt       float64
	Category  int
	Timestamp int64
}

type AircraftRepository interface {
	SaveAircraftFrame(ctx context.Context, aircrafts []Aircraft) error
	GetAircraft(ctx context.Context, id int) (Aircraft, error)
	DeleteAircrafts(ctx context.Context, ids []int32) error
	GetHistoryPositions(ctx context.Context, aircraftId int) ([]Aircraft, error)
}

type AircraftUsecase interface {
	// ProcessAircraftFrame(ctx context.Context, aircrafts []Aircraft) error
	ProcessAircraftUpdate(ctx context.Context, aircraft Aircraft)
	GetAircraft(ctx context.Context, id int) (Aircraft, error)
	DeleteAircrafts(ctx context.Context, ids []int32) error
	GetHistoryPositions(ctx context.Context, aircraftId int) ([]Aircraft, error)
}

type NatsPublisher interface {
	PublishLiveFrame(data []byte) error
}
