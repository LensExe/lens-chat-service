package main

import (
	"context"
	"go-app/internal/initialize"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := initialize.Run(ctx); err != nil {
		slog.Error("chat service stopped", "error", err)
		os.Exit(1)
	}
}
