package usecase

import (
	"backend/domain"
	"context"
	"fmt"
	"time"
)

// dya la buoc tiem phu thuoc
type aircraftUseCase struct {
	repo       domain.AircraftRepository
	updateChan chan domain.Aircraft
}

// ham khoi tao
func NewAircraftUseCase(repo domain.AircraftRepository) domain.AircraftUsecase {

	aircraftUc := &aircraftUseCase{
		repo:       repo,
		updateChan: make(chan domain.Aircraft, 5000),
	}

	go aircraftUc.startBatchWorker()

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
func (u *aircraftUseCase) ProcessAircraftUpdate(ctx context.Context, aircraft domain.Aircraft) error {
	select {
	case u.updateChan <- aircraft:
		return nil
	default:
		return fmt.Errorf("update channel is full, dropping update for aircraft ID %d", aircraft.Id)

	}
}

// goroutine chay nen de luu du lieu theo batch
func (u *aircraftUseCase) startBatchWorker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var batch []domain.Aircraft

	for {
		select {
		case ac := <-u.updateChan:
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
