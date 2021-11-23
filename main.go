package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// number of sync workers
const defaultWorkers = 8

func main() {
	from, err := NewIpfsClient(os.Getenv("IPFS_HOST"))
	if err != nil {
		log.Fatalf("failed to create ipfs client: %v", err)
	}
	to, err := NewEstuaryClient(os.Getenv("ESTUARY_HOST"), os.Getenv("ESTUARY_API_KEY"))
	if err != nil {
		log.Fatalf("failed to create estuary client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// start an initial sync before waiting for ticker
	go Sync(ctx, from, to, defaultWorkers)

	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
			Sync(ctx, from, to, defaultWorkers)
		}
	}
}
