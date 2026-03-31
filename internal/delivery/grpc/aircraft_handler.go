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
	aircraft, err := h.usecase.GetAircraft(ctx, int(req.Id))
	if err != nil {
		return nil, err
	}
	return &pb.GetAircraftResponse{
		Data: &pb.AircraftUpdate{
			Id: int32(aircraft.Id),
			Position: &pb.Position{ // Phải bọc Lat, Lng, Alt vào struct Position
				Lat: aircraft.Lat,
				Lng: aircraft.Lng,
				Alt: aircraft.Alt,
			},
			Category:  pb.Category(aircraft.Category),
			Timestamp: aircraft.Timestamp,
		},
	}, nil
}

func (h *AircraftGrpcHandler) DeleteAircrafts(ctx context.Context, req *pb.DeleteAircraftsRequest) (*pb.DeleteAircraftsResponse, error) {
	err := h.usecase.DeleteAircrafts(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	return &pb.DeleteAircraftsResponse{}, nil
}

func (h *AircraftGrpcHandler) GetHistory(ctx context.Context, req *pb.GetHistoryRequest) (*pb.GetHistoryResponse, error) {
	history, err := h.usecase.GetHistoryPositions(ctx, int(req.Id))
	if err != nil {
		return nil, err
	}

	resp := &pb.GetHistoryResponse{
		Positions: make([]*pb.Position, 0, len(history)),
	}

	for _, ac := range history {
		resp.Positions = append(resp.Positions, &pb.Position{
			Lat: ac.Lat,
			Lng: ac.Lng,
			Alt: ac.Alt,
		})
	}
	return resp, nil
}
