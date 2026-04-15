package nats

import (
	"backend/domain"
	"context"
	"time"

	pb "backend/pb/aircraft"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type natsHandler struct {
	aircraftUsecase domain.AircraftUsecase
}

func NewNatsHandler(aircraftUsecase domain.AircraftUsecase) *natsHandler {
	return &natsHandler{
		aircraftUsecase: aircraftUsecase,
	}
}

func (h *natsHandler) HandleAircraftMessage(msg *nats.Msg) {
	var aircraftUpdate pb.AircraftUpdate

	err := proto.Unmarshal(msg.Data, &aircraftUpdate)
	if err != nil {
		return
	}

	// chuyen tu dinh dang protobuf sang domain model
	aircraft := domain.Aircraft{
		Callsign:       aircraftUpdate.Callsign,
		DetectionTime:  aircraftUpdate.DetectionTime,
		Category:       int(aircraftUpdate.Category),
		Mode3A:         aircraftUpdate.Mode_3A,
		Classification: aircraftUpdate.Classification,
		LastLat:        aircraftUpdate.GetPosition().GetLat(),
		LastLng:        aircraftUpdate.GetPosition().GetLng(),
		LastAlt:        aircraftUpdate.GetPosition().GetAlt(),
		Speed:          aircraftUpdate.GetPosition().GetSpeed(),
		Heading:        aircraftUpdate.GetPosition().GetHeading(),
		LastTimestamp:  aircraftUpdate.Timestamp,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h.aircraftUsecase.ProcessAircraftUpdate(ctx, aircraft)

}

type natsPublisher struct {
	nc *nats.Conn
}

func NewNatsPublisher(nc *nats.Conn) *natsPublisher {
	return &natsPublisher{
		nc: nc,
	}
}

func (p *natsPublisher) PublishLiveFrame(data []byte) error {

	return p.nc.Publish("flight.live", data)
}
