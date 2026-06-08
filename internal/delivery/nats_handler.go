package nats

import (
	"backend/domain"
	"context"
	"log"
	"net/http"
	"time"

	pb "backend/proto/pb/aircraft"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

var (
	messagesReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aircraft_messages_received_total",
			Help: "Tổng số lượng message mục tiêu bay nhận được từ giả lập",
		},
	)
	processingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aircraft_message_processing_duration_seconds",
			Help:    "Thời gian (giây) để xử lý và lưu 1 mục tiêu bay vào DB",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5}, // Tập trung đo dải ms
		},
	)
)

// Khởi tạo đăng ký metrics với Prometheus
func init() {
	prometheus.MustRegister(messagesReceived)
	prometheus.MustRegister(processingDuration)
}

type natsHandler struct {
	aircraftUsecase domain.AircraftUsecase
}

func NewNatsHandler(aircraftUsecase domain.AircraftUsecase) *natsHandler {
	return &natsHandler{
		aircraftUsecase: aircraftUsecase,
	}
}

func (h *natsHandler) HandleAircraftMessage(msg *nats.Msg) {

	startTime := time.Now() // Bắt đầu đếm thời gian

	messagesReceived.Inc() // Tăng biến đếm tổng số message lên 1
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

	duration := time.Since(startTime).Seconds()
	processingDuration.Observe(duration)

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

func StartMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Prometheus Metrics Server chạy tại port :2112")
	if err := http.ListenAndServe(":2112", nil); err != nil {
		log.Fatalf("Lỗi chạy metrics server: %v", err)
	}
}
