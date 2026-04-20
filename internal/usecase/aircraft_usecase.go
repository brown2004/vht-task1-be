package usecase

import (
	"backend/domain"
	"context"
	"fmt"
	"log"
	"sort" // Bắt buộc phải import thư viện sort
	"sync"
	"sync/atomic"
	"time"

	pb "backend/pb/aircraft"

	"google.golang.org/protobuf/proto"
)

type aircraftKey struct {
	Callsign      string
	DetectionTime int64
}

// dya la buoc tiem phu thuoc
type aircraftUseCase struct {
	repo        domain.AircraftRepository
	publisher   domain.NatsPublisher
	dbChan      chan domain.Aircraft
	hotBatches  chan []domain.Aircraft
	coldBatches chan []domain.Aircraft
	natsChan    chan domain.Aircraft
	lastSeen    map[aircraftKey]time.Time
	mt          sync.Mutex

	//log
	receivedUpdates    uint64
	droppedDBUpdates   uint64
	droppedNATSUpdates uint64
	enqueuedBatches    uint64
	enqueuedUpdates    uint64
	droppedHotBatches  uint64
	droppedColdBatches uint64
	hotFlushOK         uint64
	hotFlushErr        uint64
	hotFlushUpdates    uint64
	coldFlushOK        uint64
	coldFlushErr       uint64
	coldFlushUpdates   uint64
}

// ham khoi tao
func NewAircraftUseCase(repo domain.AircraftRepository, publisher domain.NatsPublisher) domain.AircraftUsecase {

	aircraftUc := &aircraftUseCase{
		repo:        repo,
		publisher:   publisher,
		dbChan:      make(chan domain.Aircraft, 200000),
		hotBatches:  make(chan []domain.Aircraft, 256),
		coldBatches: make(chan []domain.Aircraft, 1024),
		natsChan:    make(chan domain.Aircraft, 200000),
		lastSeen:    make(map[aircraftKey]time.Time),
	}

	go aircraftUc.startBatchWorker()
	aircraftUc.startHotDataWorkers(1)
	aircraftUc.startColdDataWorkers(1)
	go aircraftUc.sendFrameToNATS()
	go aircraftUc.startTimeoutWorker()
	go aircraftUc.startColdDataCleanupWorker()
	go aircraftUc.startPipelineStatsWorker()

	return aircraftUc

}

// ham xu ly tung may bay
func (u *aircraftUseCase) ProcessAircraftUpdate(ctx context.Context, aircraft domain.Aircraft) {
	atomic.AddUint64(&u.receivedUpdates, 1)

	u.mt.Lock()
	key := aircraftKey{Callsign: aircraft.Callsign, DetectionTime: aircraft.DetectionTime}
	u.lastSeen[key] = time.Now()
	u.mt.Unlock()

	//dtb
	select {
	case u.dbChan <- aircraft:
	default:
		atomic.AddUint64(&u.droppedDBUpdates, 1)
		log.Printf("[USECASE ERROR] db channel full, dropped update callsign=%s detection_time=%d db_queue=%d", aircraft.Callsign, aircraft.DetectionTime, len(u.dbChan))
	}

	//nats
	select {
	case u.natsChan <- aircraft:
	default:
		atomic.AddUint64(&u.droppedNATSUpdates, 1)
		log.Printf("[USECASE ERROR] live nats channel full, dropped update callsign=%s detection_time=%d nats_queue=%d", aircraft.Callsign, aircraft.DetectionTime, len(u.natsChan))
	}
}

// goroutine chay nen de luu du lieu theo batch
func (u *aircraftUseCase) startBatchWorker() {
	batchSize := 1000
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	batch := make([]domain.Aircraft, 0, batchSize)

	//worker o vong lap vo han nay
	for {
		select {
		case ac := <-u.dbChan:
			batch = append(batch, ac)

			if len(batch) >= batchSize {
				batchToProcess := make([]domain.Aircraft, len(batch))
				copy(batchToProcess, batch)

				u.enqueueDataBatches(batchToProcess)

				batch = make([]domain.Aircraft, 0, batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				batchToProcess := make([]domain.Aircraft, len(batch))
				copy(batchToProcess, batch)

				u.enqueueDataBatches(batchToProcess)

				batch = make([]domain.Aircraft, 0, batchSize)
			}
		}
	}
}

func (u *aircraftUseCase) enqueueDataBatches(batch []domain.Aircraft) {
	atomic.AddUint64(&u.enqueuedBatches, 1)
	atomic.AddUint64(&u.enqueuedUpdates, uint64(len(batch)))

	hotBatch := make([]domain.Aircraft, len(batch))
	copy(hotBatch, batch)
	select {
	case u.hotBatches <- hotBatch:
	default:
		atomic.AddUint64(&u.droppedHotBatches, 1)
		log.Printf("[USECASE ERROR] hot batch queue full, dropped batch updates=%d hot_queue=%d", len(hotBatch), len(u.hotBatches))
	}

	coldBatch := make([]domain.Aircraft, len(batch))
	copy(coldBatch, batch)
	select {
	case u.coldBatches <- coldBatch:
	default:
		atomic.AddUint64(&u.droppedColdBatches, 1)
		log.Printf("[USECASE ERROR] cold batch queue full, dropped batch updates=%d cold_queue=%d", len(coldBatch), len(u.coldBatches))
	}
}

func (u *aircraftUseCase) startHotDataWorkers(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for batch := range u.hotBatches {
				u.flushHotData(batch)
			}
		}()
	}
}

func (u *aircraftUseCase) startColdDataWorkers(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for batch := range u.coldBatches {
				u.flushColdData(batch)
			}
		}()
	}
}

func sortAircraftBatch(batch []domain.Aircraft) {
	sort.Slice(batch, func(i, j int) bool {
		if batch[i].Callsign == batch[j].Callsign {
			return batch[i].DetectionTime < batch[j].DetectionTime
		}
		return batch[i].Callsign < batch[j].Callsign
	})
}

func (u *aircraftUseCase) flushHotData(batch []domain.Aircraft) {
	sortAircraftBatch(batch)

	hotCtx, hotCancel := context.WithTimeout(context.Background(), 60*time.Second)
	errHot := u.repo.SaveAircraftHotData(hotCtx, batch)
	hotCancel()
	if errHot != nil {
		atomic.AddUint64(&u.hotFlushErr, 1)
		log.Printf("[USECASE ERROR] hot flush failed updates=%d hot_queue=%d err=%v", len(batch), len(u.hotBatches), errHot)
		return
	}
	atomic.AddUint64(&u.hotFlushOK, 1)
	atomic.AddUint64(&u.hotFlushUpdates, uint64(len(batch)))
}

func (u *aircraftUseCase) flushColdData(batch []domain.Aircraft) {
	sortAircraftBatch(batch)

	coldCtx, coldCancel := context.WithTimeout(context.Background(), 60*time.Second)
	errCold := u.repo.SaveAircraftColdData(coldCtx, batch)
	coldCancel()
	if errCold != nil {
		atomic.AddUint64(&u.coldFlushErr, 1)
		log.Printf("[USECASE ERROR] cold flush failed updates=%d cold_queue=%d err=%v", len(batch), len(u.coldBatches), errCold)
		return
	}
	atomic.AddUint64(&u.coldFlushOK, 1)
	atomic.AddUint64(&u.coldFlushUpdates, uint64(len(batch)))
}

func (u *aircraftUseCase) startPipelineStatsWorker() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastReceived uint64
	var lastHot uint64
	var lastCold uint64

	for range ticker.C {
		received := atomic.LoadUint64(&u.receivedUpdates)
		hotUpdates := atomic.LoadUint64(&u.hotFlushUpdates)
		coldUpdates := atomic.LoadUint64(&u.coldFlushUpdates)

		receivedDelta := received - lastReceived
		hotDelta := hotUpdates - lastHot
		coldDelta := coldUpdates - lastCold

		lastReceived = received
		lastHot = hotUpdates
		lastCold = coldUpdates

		dbQueue := len(u.dbChan)
		natsQueue := len(u.natsChan)
		hotQueue := len(u.hotBatches)
		coldQueue := len(u.coldBatches)
		hotErr := atomic.LoadUint64(&u.hotFlushErr)
		coldErr := atomic.LoadUint64(&u.coldFlushErr)
		droppedDB := atomic.LoadUint64(&u.droppedDBUpdates)
		droppedNATS := atomic.LoadUint64(&u.droppedNATSUpdates)
		droppedHot := atomic.LoadUint64(&u.droppedHotBatches)
		droppedCold := atomic.LoadUint64(&u.droppedColdBatches)
		level := "OK"
		if hotErr > 0 || coldErr > 0 || droppedDB > 0 || droppedNATS > 0 || droppedHot > 0 || droppedCold > 0 {
			level = "ERROR"
		}

		log.Printf("[USECASE %s] input total=%d rate=%.0f/s | queues db=%d/%d live=%d/%d hot=%d/%d cold=%d/%d | batches=%d | hot saved=%d rate=%.0f/s errors=%d | cold saved=%d rate=%.0f/s errors=%d | drops db=%d live=%d hot_batch=%d cold_batch=%d",
			level,
			received,
			float64(receivedDelta)/2.0,
			dbQueue,
			cap(u.dbChan),
			natsQueue,
			cap(u.natsChan),
			hotQueue,
			cap(u.hotBatches),
			coldQueue,
			cap(u.coldBatches),
			atomic.LoadUint64(&u.enqueuedBatches),
			hotUpdates,
			float64(hotDelta)/2.0,
			hotErr,
			coldUpdates,
			float64(coldDelta)/2.0,
			coldErr,
			droppedDB,
			droppedNATS,
			droppedHot,
			droppedCold,
		)
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
			if len(currentFrame) == 0 {
				continue
			}

			frameMsg := &pb.AircraftFrame{
				FrameTimestamp: time.Now().UnixMilli(),
				Data:           make([]*pb.AircraftUpdate, 0, len(currentFrame)),
			}

			for _, ac := range currentFrame {
				frameMsg.Data = append(frameMsg.Data, &pb.AircraftUpdate{
					Callsign:      ac.Callsign,
					DetectionTime: ac.DetectionTime,
					Category:      pb.Category(ac.Category),
					Position: &pb.Position{
						Lat:   ac.LastLat,
						Lng:   ac.LastLng,
						Alt:   ac.LastAlt,
						Speed: ac.Speed,
					},
					Timestamp: ac.LastTimestamp,
				})
			}

			// Dong goi vao protobuf
			data, err := proto.Marshal(frameMsg)
			if err != nil {
				log.Printf("[USECASE ERROR] marshal live frame failed updates=%d err=%v", len(currentFrame), err)
			} else {
				err = u.publisher.PublishLiveFrame(data) // goi ham publish cua nats publisher
				if err != nil {
					log.Printf("[USECASE ERROR] publish live frame failed updates=%d bytes=%d err=%v", len(currentFrame), len(data), err)
				}
			}
			currentFrame = nil
		}
	}
}

func (u *aircraftUseCase) GetAircraft(ctx context.Context, callsign string, detectionTime int64) (domain.Aircraft, error) {
	return u.repo.GetAircraft(ctx, callsign, detectionTime)
}

func (u *aircraftUseCase) DeleteAircrafts(ctx context.Context, callsigns []string) error {
	err := u.repo.DeleteAircrafts(ctx, callsigns)
	if err != nil {
		fmt.Printf("Failed to delete aircraft with IDs %v: %v\n", callsigns, err)
	}
	return err
}

func (u *aircraftUseCase) GetHistoryPositions(ctx context.Context, callsign string, detectionTime int64) ([]domain.Aircraft, error) {
	return u.repo.GetHistoryPositions(ctx, callsign, detectionTime)
}

func (u *aircraftUseCase) startTimeoutWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		u.mt.Lock()
		now := time.Now()
		var keysToDelete []string
		for id, last := range u.lastSeen {
			if now.Sub(last) > 5*time.Second {
				keysToDelete = append(keysToDelete, id.Callsign)
				delete(u.lastSeen, id)
			}
		}
		u.mt.Unlock()

		if len(keysToDelete) > 0 {
			log.Printf("[USECASE OK] target lost, deleting stale aircraft count=%d callsigns=%v", len(keysToDelete), keysToDelete)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := u.DeleteAircrafts(ctx, keysToDelete)
			if err != nil {
				log.Printf("[USECASE ERROR] delete stale aircraft failed count=%d callsigns=%v err=%v", len(keysToDelete), keysToDelete, err)
			}
			cancel()
		}
	}
}

func (u *aircraftUseCase) MarkPositionsAsPermanent(ctx context.Context, callsign string, detectionTime int64, fromTs int64, toTs int64) error {
	return u.repo.MarkPositionsAsPermanent(ctx, callsign, detectionTime, fromTs, toTs)
}

func (u *aircraftUseCase) startColdDataCleanupWorker() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		twentyDaysAgo := time.Now().Add(-20 * 24 * time.Hour).UnixMilli()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

		err := u.repo.CleanupExpiredPositions(ctx, twentyDaysAgo)
		if err != nil {
			log.Printf("[USECASE ERROR] cleanup expired positions failed cutoff=%d err=%v", twentyDaysAgo, err)
		}

		cancel()
	}
}

// SaveAircraftColdData implements [domain.AircraftUsecase].
func (u *aircraftUseCase) SaveAircraftColdData(ctx context.Context, aircrafts []domain.Aircraft) error {
	if len(aircrafts) == 0 {
		return nil
	}
	err := u.repo.SaveAircraftColdData(ctx, aircrafts)
	if err != nil {
		log.Printf("[USECASE ERROR] save cold data from client failed updates=%d err=%v", len(aircrafts), err)
	}
	return err
}

func (u *aircraftUseCase) GetGlobalPlaybackData(ctx context.Context, fromTs int64, toTs int64) ([]domain.FlightPlayback, error) {
	flatData, err := u.repo.GetAllArchivedPositionsByTimeWindow(ctx, fromTs, toTs)
	if err != nil {
		return nil, fmt.Errorf("failed to get flat positions: %w", err)
	}

	groupedMap := make(map[string]*domain.FlightPlayback)

	for _, ac := range flatData {
		key := fmt.Sprintf("%s_%d", ac.Callsign, ac.DetectionTime)

		if _, exists := groupedMap[key]; !exists {
			groupedMap[key] = &domain.FlightPlayback{
				Callsign:      ac.Callsign,
				DetectionTime: ac.DetectionTime,
				Positions:     make([]domain.Position, 0),
			}
		}

		pos := domain.Position{
			Lat:         ac.LastLat,
			Lng:         ac.LastLng,
			Alt:         ac.LastAlt,
			Speed:       ac.Speed,
			Heading:     ac.Heading,
			Timestamp:   ac.LastTimestamp,
			IsPermanent: ac.IsPermanent,
		}

		groupedMap[key].Positions = append(groupedMap[key].Positions, pos)
	}

	var result []domain.FlightPlayback
	for _, group := range groupedMap {
		result = append(result, *group)
	}

	return result, nil
}

func (u *aircraftUseCase) startSlowArchiveWorker() {
	// Để trống nếu chưa implement
}
