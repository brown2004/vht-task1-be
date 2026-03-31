package main

import (
	"database/sql"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	nats "backend/internal/delivery"
	grpc_delivery "backend/internal/delivery/grpc"

	"backend/internal/repo/postgres"
	"backend/internal/usecase"
	pb "backend/pb/aircraft"

	_ "github.com/lib/pq" // Driver PostgreSQL
	natsio "github.com/nats-io/nats.go"
	"google.golang.org/grpc"
)

func main() {
	dsn := "postgres://admin:secretpassword@localhost:5432/aircraft_tracking?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL database!")

	nc, err := natsio.Connect("nats://localhost:4223")
	if err != nil {
		log.Fatalf("Failed to connect to NATS Server: %v", err)
	}
	defer nc.Close()
	log.Println("Successfully connected to NATS Server!")

	repo := postgres.NewAircraftRepository(db)

	pub := nats.NewNatsPublisher(nc)

	ucase := usecase.NewAircraftUseCase(repo, pub)

	handler := nats.NewNatsHandler(ucase)

	subject := "flight.data" // subject NATs

	_, err = nc.Subscribe(subject, handler.HandleAircraftMessage)
	if err != nil {
		log.Fatalf("Failed to subscribe to NATS topic [%s]: %v", subject, err)
	}
	log.Printf("Listening on subject [%s]...", subject)

	grpc_handler := grpc_delivery.NewAircraftGrpchandler(ucase)
	grpc_server := grpc.NewServer()

	pb.RegisterAircraftServicesServer(grpc_server, grpc_handler)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	//go routine de chay grpc server
	go func() {
		log.Println("Starting gRPC server on port 50051...")
		if err := grpc_server.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Shutting down server...")
	grpc_server.GracefulStop()
	log.Println("Server stopped gracefully")

}

// test gprc: evans --path ./proto --proto aircrafts.proto -p 50051 repl
