package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	pb "backend/pb/aircraft"
)

const (
	// Dùng port 4223 nếu code đang chạy ngoài Docker kết nối vào Docker
	NATS_URL = "nats://127.0.0.1:4223"
	SUBJECT  = "flight.data"
)

func main() {
	// 1. Kết nối tới NATS Server
	nc, err := nats.Connect(NATS_URL)
	if err != nil {
		log.Fatalf("Không thể kết nối tới NATS: %v", err)
	}
	defer nc.Close()
	fmt.Println("🚀 [Backend Consumer] Đã kết nối NATS thành công! Đang chờ dữ liệu...")

	// 2. Lắng nghe dữ liệu (Subscribe) trên topic "flight.data"
	_, err = nc.Subscribe(SUBJECT, func(msg *nats.Msg) {
		// Khởi tạo đối tượng AircraftFrame để hứng dữ liệu
		var frame pb.AircraftFrame

		// Giải mã (Unmarshal) mảng byte nhị phân thành Protobuf struct
		if err := proto.Unmarshal(msg.Data, &frame); err != nil {
			log.Printf("Lỗi giải mã Protobuf: %v\n", err)
			return
		}

		// Duyệt qua mảng Data (gồm các AircraftUpdate) và in ra màn hình
		for _, update := range frame.Data {
			// update.Category.String() sẽ tự map enum số thành chữ (VD: CATEGORY_FRIENDLY)
			fmt.Printf("[NATS RCV] ID: %d | Phe: %s | Tọa độ: (%.4f, %.4f) | Alt: %.0f\n",
				update.Id,
				update.Category.String(),
				update.Position.Lat,
				update.Position.Lng,
				update.Position.Alt,
			)
		}
	})

	if err != nil {
		log.Fatalf("Lỗi khi Subscribe NATS: %v", err)
	}

	// 3. Giữ chương trình chạy ngầm để liên tục lắng nghe
	// Dùng channel để bắt tín hiệu tắt an toàn từ bàn phím (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Đã ngắt kết nối NATS. Chương trình Consumer kết thúc.")
}
