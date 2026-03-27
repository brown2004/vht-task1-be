package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	nats "backend/internal/delivery"

	"backend/internal/repo/postgres"
	"backend/internal/usecase"

	_ "github.com/lib/pq" // Driver PostgreSQL (phải có dấu _ để chạy ngầm)
	natsio "github.com/nats-io/nats.go"
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

	ucase := usecase.NewAircraftUseCase(repo)

	handler := nats.NewNatsHandler(ucase)

	subject := "flight.data" // subject NATs

	_, err = nc.Subscribe(subject, handler.HandleAircraftMessage)
	if err != nil {
		log.Fatalf("Failed to subscribe to NATS topic [%s]: %v", subject, err)
	}
	log.Printf("Listening on subject [%s]...", subject)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	log.Println("Failed to subscribe to NATS topic [%s]: %v", subject, err)
}
