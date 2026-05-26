package grpc_deliverry

import (
	"backend/domain"
	pb "backend/proto/pb/aircraft"
	"context"
)

type AircraftGrpcHandler struct {
	pb.UnimplementedAircraftServicesServer
	usecase domain.AircraftUsecase
}

func NewAircraftGrpchandler(usecase domain.AircraftUsecase) *AircraftGrpcHandler {
	return &AircraftGrpcHandler{
		usecase: usecase,
	}
}

func (h *AircraftGrpcHandler) GetAircraft(ctx context.Context, req *pb.GetAircraftRequest) (*pb.GetAircraftResponse, error) {
	aircraft, err := h.usecase.GetAircraft(ctx, req.Aircraft.Callsign, req.Aircraft.DetectionTime)
	if err != nil {
		return nil, err
	}
	return &pb.GetAircraftResponse{
		Data: &pb.AircraftUpdate{
			Callsign:      aircraft.Callsign,
			DetectionTime: aircraft.DetectionTime,
			Timestamp:     aircraft.LastTimestamp,
			Position: &pb.Position{
				Lat:     aircraft.LastLat,
				Lng:     aircraft.LastLng,
				Alt:     aircraft.LastAlt,
				Speed:   aircraft.Speed,
				Heading: aircraft.Heading,
			},
			Category:       pb.Category(aircraft.Category),
			Mode_3A:        aircraft.Mode3A,
			Classification: aircraft.Classification,
		},
	}, nil
}

func (h *AircraftGrpcHandler) DeleteAircrafts(ctx context.Context, req *pb.DeleteAircraftsRequest) (*pb.DeleteAircraftsResponse, error) {
	var callsigns []string
	for _, identity := range req.GetAircrafts() {
		if identity != nil {
			callsigns = append(callsigns, identity.Callsign)
		}
	}
	err := h.usecase.DeleteAircrafts(ctx, callsigns)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteAircraftsResponse{Deleted: true}, nil
}

func (h *AircraftGrpcHandler) GetHistory(ctx context.Context, req *pb.GetHistoryRequest) (*pb.GetHistoryResponse, error) {
	history, err := h.usecase.GetHistoryPositions(ctx, req.Aircraft.Callsign, req.Aircraft.DetectionTime)
	if err != nil {
		return nil, err
	}

	resp := &pb.GetHistoryResponse{
		Positions: make([]*pb.Position, 0, len(history)),
	}

	for _, ac := range history {
		resp.Positions = append(resp.Positions, &pb.Position{
			Lat:     ac.LastLat,
			Lng:     ac.LastLng,
			Alt:     ac.LastAlt,
			Speed:   ac.Speed,
			Heading: ac.Heading,
		})
	}
	return resp, nil
}

func (h *AircraftGrpcHandler) GetGlobalPlayback(ctx context.Context, req *pb.GlobalPlaybackRequest) (*pb.GlobalPlaybackResponse, error) {
	data, err := h.usecase.GetGlobalPlaybackData(ctx, req.FromTs, req.ToTs)
	if err != nil {
		return nil, err
	}

	resp := &pb.GlobalPlaybackResponse{
		Flights: make([]*pb.FlightPlayback, 0, len(data)),
	}

	for _, f := range data {
		pbPositions := make([]*pb.PlaybackPosition, 0, len(f.Positions))
		for _, p := range f.Positions {
			pbPositions = append(pbPositions, &pb.PlaybackPosition{
				Timestamp:   p.Timestamp,
				Lat:         p.Lat,
				Lng:         p.Lng,
				Alt:         p.Alt,
				Speed:       p.Speed,
				Heading:     p.Heading,
				IsPermanent: p.IsPermanent,
			})
		}

		resp.Flights = append(resp.Flights, &pb.FlightPlayback{
			Callsign:       f.Callsign,
			DetectionTime:  f.DetectionTime,
			Positions:      pbPositions,
			Category:       pb.Category(f.Category),
			Classification: f.Classification,
		})
	}

	return resp, nil
}

func (h *AircraftGrpcHandler) UpdateIsPermanentAircrafts(ctx context.Context, req *pb.UpdateIsPermanentAircraftsRequest) (*pb.UpdateIsPermanentAircraftsResponse, error) {
	if req.Aircraft == nil {
		return &pb.UpdateIsPermanentAircraftsResponse{Success: false}, nil
	}

	err := h.usecase.MarkPositionsAsPermanent(
		ctx,
		req.Aircraft.Callsign,
		req.Aircraft.DetectionTime,
		req.FromTs,
		req.ToTs,
	)

	if err != nil {
		return &pb.UpdateIsPermanentAircraftsResponse{Success: false}, err
	}

	return &pb.UpdateIsPermanentAircraftsResponse{Success: true}, nil
}

func (h *AircraftGrpcHandler) GetAircraftsByTimeWindow(ctx context.Context, req *pb.Session) (*pb.GetAircraftsByTimeWindowResponse, error) {
	aircrafts, err := h.usecase.GetAircraftsByTimeWindow(ctx, req.FromTs, req.ToTs)
	if err != nil {
		return nil, err
	}

	resp := &pb.GetAircraftsByTimeWindowResponse{
		AircraftIdentities: make([]*pb.AircraftIdentity, 0, len(aircrafts)),
	}

	for _, aircraft := range aircrafts {
		resp.AircraftIdentities = append(resp.AircraftIdentities, &pb.AircraftIdentity{
			Callsign:      aircraft.Callsign,
			DetectionTime: aircraft.DetectionTime,
		})
	}

	return resp, nil
}

func (h *AircraftGrpcHandler) GetPlaybackDataByTimeWindow(ctx context.Context, req *pb.GetPlaybackDataByTimeWindowRequest) (*pb.GlobalPlaybackResponse, error) {
	aircraftIdentities := aircraftIdentitiesFromPB(req.GetAircrafts())
	data, err := h.usecase.GetPlaybackDataByTimeWindow(ctx, aircraftIdentities, req.FromTs, req.ToTs)
	if err != nil {
		return nil, err
	}

	return playbackResponseFromDomain(data), nil
}

func (h *AircraftGrpcHandler) GetPlaybackDataBySession(ctx context.Context, req *pb.GetPlaybackDataBySessionRequest) (*pb.GlobalPlaybackResponse, error) {
	aircraftIdentities := aircraftIdentitiesFromPB(req.GetAircrafts())
	data, err := h.usecase.GetPlaybackDataBySession(
		ctx,
		aircraftIdentities,
		req.FromTs,
		req.ToTs,
		req.GetSampleIntervalMs(),
	)
	if err != nil {
		return nil, err
	}

	return playbackResponseFromDomain(data), nil
}

func aircraftIdentitiesFromPB(aircrafts []*pb.AircraftIdentity) []domain.AircraftIdentity {
	aircraftIdentities := make([]domain.AircraftIdentity, 0, len(aircrafts))
	for _, aircraft := range aircrafts {
		if aircraft == nil {
			continue
		}
		aircraftIdentities = append(aircraftIdentities, domain.AircraftIdentity{
			Callsign:      aircraft.Callsign,
			DetectionTime: aircraft.DetectionTime,
		})
	}
	return aircraftIdentities
}

func playbackResponseFromDomain(data []domain.FlightPlayback) *pb.GlobalPlaybackResponse {
	resp := &pb.GlobalPlaybackResponse{
		Flights: make([]*pb.FlightPlayback, 0, len(data)),
	}

	for _, flight := range data {
		positions := make([]*pb.PlaybackPosition, 0, len(flight.Positions))
		for _, position := range flight.Positions {
			positions = append(positions, &pb.PlaybackPosition{
				Timestamp:   position.Timestamp,
				Lat:         position.Lat,
				Lng:         position.Lng,
				Alt:         position.Alt,
				Speed:       position.Speed,
				Heading:     position.Heading,
				IsPermanent: position.IsPermanent,
			})
		}

		resp.Flights = append(resp.Flights, &pb.FlightPlayback{
			Callsign:       flight.Callsign,
			DetectionTime:  flight.DetectionTime,
			Positions:      positions,
			Category:       pb.Category(flight.Category),
			Classification: flight.Classification,
		})
	}

	return resp
}
