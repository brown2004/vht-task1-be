package grpc_deliverry

import (
	"backend/domain"
	pb "backend/pb/aircraft"
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
			Callsign:      f.Callsign,
			DetectionTime: f.DetectionTime,
			Positions:     pbPositions,
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
