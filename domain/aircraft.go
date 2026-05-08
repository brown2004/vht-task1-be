package domain

import "context"

const (
	Unknown  int = 0
	Friendly int = 1
	Hostile  int = 2
)

type AircraftIdentity struct {
	Callsign      string
	DetectionTime int64
}

type Aircraft struct {
	Callsign       string
	DetectionTime  int64
	Category       int
	Mode3A         string
	Classification string
	LastLat        float64
	LastLng        float64
	LastAlt        float64
	Speed          float64
	Heading        float64
	LastTimestamp  int64
	IsPermanent    bool
}

type Position struct {
	Lat         float64
	Lng         float64
	Alt         float64
	Speed       float64
	Heading     float64
	Timestamp   int64
	IsPermanent bool
}

type FlightPlayback struct {
	Callsign      string
	DetectionTime int64
	Positions     []Position
}

type AircraftRepository interface {
	SaveAircraftColdData(ctx context.Context, aircrafts []Aircraft) error
	SaveAircraftHotData(ctx context.Context, aircrafts []Aircraft) error
	MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error
	GetAircraft(ctx context.Context, callsign string, detectionTime int64) (Aircraft, error)
	DeleteAircrafts(ctx context.Context, callsign []string) error
	GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]Aircraft, error)
	CleanupExpiredPositions(ctx context.Context, cutoffTimestamp int64) error
	GetAllArchivedPositionsByTimeWindow(ctx context.Context, fromTs int64, toTs int64) ([]Aircraft, error)
	GetAircraftsByTimeWindow(ctx context.Context, fromTs, toTs int64) ([]AircraftIdentity, error)
	GetPlaybackDataByTimeWindow(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs int64) ([]FlightPlayback, error)
	GetPlaybackDataBySession(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs, sampleIntervalMs int64) ([]FlightPlayback, error)
}

type AircraftUsecase interface {
	// ProcessAircraftFrame(ctx context.Context, aircrafts []Aircraft) error
	SaveAircraftColdData(ctx context.Context, aircrafts []Aircraft) error
	ProcessAircraftUpdate(ctx context.Context, aircraft Aircraft)
	GetAircraft(ctx context.Context, callsign string, detectionTime int64) (Aircraft, error)
	DeleteAircrafts(ctx context.Context, callsign []string) error
	GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]Aircraft, error)
	MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error
	GetGlobalPlaybackData(ctx context.Context, fromTs int64, toTs int64) ([]FlightPlayback, error)
	GetAircraftsByTimeWindow(ctx context.Context, fromTs, toTs int64) ([]AircraftIdentity, error)
	GetPlaybackDataByTimeWindow(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs int64) ([]FlightPlayback, error)
	GetPlaybackDataBySession(ctx context.Context, aircraftIdentities []AircraftIdentity, fromTs, toTs, sampleIntervalMs int64) ([]FlightPlayback, error)
}

type NatsPublisher interface {
	PublishLiveFrame(data []byte) error
}
