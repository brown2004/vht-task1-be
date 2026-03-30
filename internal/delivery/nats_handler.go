package nats

import (
	"backend/domain"
	"context"
	"log"
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
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	aircraft := domain.Aircraft{
		Id:        int(aircraftUpdate.Id),
		Lat:       aircraftUpdate.Position.Lat,
		Lng:       aircraftUpdate.Position.Lng,
		Alt:       aircraftUpdate.Position.Alt,
		Category:  int(aircraftUpdate.Category),
		Timestamp: aircraftUpdate.Timestamp,
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
