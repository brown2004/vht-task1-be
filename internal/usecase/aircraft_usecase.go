package usecase

import (
	"backend/domain"
	"context"
	"fmt"
	"time"

	pb "backend/pb/aircraft"

	"google.golang.org/protobuf/proto"
)

// dya la buoc tiem phu thuoc
type aircraftUseCase struct {
	repo      domain.AircraftRepository
	publisher domain.NatsPublisher
	dbChan    chan domain.Aircraft
	natsChan  chan domain.Aircraft
}

// ham khoi tao
func NewAircraftUseCase(repo domain.AircraftRepository, publisher domain.NatsPublisher) domain.AircraftUsecase {

	aircraftUc := &aircraftUseCase{
		repo:      repo,
		publisher: publisher,
		dbChan:    make(chan domain.Aircraft, 5000),
		natsChan:  make(chan domain.Aircraft, 5000),
	}

	go aircraftUc.startBatchWorker()
	go aircraftUc.sendFrameToNATS()

	return aircraftUc

}

// Ham xu ly luu 1 cuc may bay
// func (u *aircraftUseCase) ProcessAircraftFrame(ctx context.Context, aircrafts []domain.Aircraft) error {
// 	if len(aircrafts) == 0 {
// 		return nil
// 	}
// 	fmt.Println("Usecase ProcessAircraftFrame called with", len(aircrafts), "aircrafts")
// 	err := u.repo.SaveAircraftFrame(ctx, aircrafts)
// 	if err != nil {
// 		return fmt.Errorf("Failed to save aircraft frame: %w", err)
// 	}
// 	return nil
// }

// ham xu ly tung may bay
// ham xu ly tung may bay
func (u *aircraftUseCase) ProcessAircraftUpdate(ctx context.Context, aircraft domain.Aircraft) {
	//dtb
	select {
	case u.dbChan <- aircraft:
	default:

		fmt.Printf("DB channel is full, dropping update for aircraft ID %d\n", aircraft.Id)
	}

	//nats
	select {
	case u.natsChan <- aircraft:
	default:
		fmt.Printf("NATS channel is full, dropping update for aircraft ID %d\n", aircraft.Id)
	}

}

// goroutine chay nen de luu du lieu theo batch
func (u *aircraftUseCase) startBatchWorker() {
	ticker := time.NewTicker(3000 * time.Millisecond)
	defer ticker.Stop()

	var batch []domain.Aircraft

	for {
		select {
		case ac := <-u.dbChan:
			batch = append(batch, ac)

			if len(batch) >= 1000 {
				u.flushToDB(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				u.flushToDB(batch)
				batch = nil
			}
		}
	}
}

func (u *aircraftUseCase) flushToDB(batch []domain.Aircraft) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := u.repo.SaveAircraftFrame(ctx, batch)
	if err != nil {
		fmt.Printf("Failed to save aircraft frame: %v\n", err)
	} else {
		fmt.Printf("Successfully saved batch of %d aircrafts\n", len(batch))
	}
}

func (u *aircraftUseCase) sendFrameToNATS() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var currentFrame []domain.Aircraft

	for {
		select {
		case ac := <-u.natsChan:
			currentFrame = append(currentFrame, ac)

		case <-ticker.C:
			frameMsg := &pb.AircraftFrame{
				FrameTimestamp: time.Now().UnixMilli(),
				Data:           make([]*pb.AircraftUpdate, 0, len(currentFrame)),
			}

			for _, ac := range currentFrame {
				frameMsg.Data = append(frameMsg.Data, &pb.AircraftUpdate{
					Id:       int32(ac.Id),
					Category: pb.Category(ac.Category),
					Position: &pb.Position{
						Lat: ac.Lat,
						Lng: ac.Lng,
						Alt: ac.Alt,
					},
					Timestamp: ac.Timestamp,
				})
			}

			// DOng goi vao protobuf
			data, err := proto.Marshal(frameMsg)
			if err != nil {
				fmt.Printf("Failed to marshal NATS frame: %v\n", err)
			} else {
				err = u.publisher.PublishLiveFrame(data) // goi ham publish cua nats publisher
				if err != nil {
					fmt.Printf("Failed to publish live frame: %v\n", err)
				}
			}

			// don dep
			currentFrame = nil
		}
	}
}
