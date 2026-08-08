package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Duke-ECE/session-manager/internal/infrastructure/postgrest"
	"github.com/Duke-ECE/session-manager/internal/session"
	transportgrpc "github.com/Duke-ECE/session-manager/internal/transport/grpc"
)

func main() {
	supabaseURL := os.Getenv("SUPABASE_URL")
	serviceKey := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	if supabaseURL == "" || serviceKey == "" {
		log.Fatal("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
	}
	serviceToken := os.Getenv("SERVICE_TOKEN")
	if serviceToken == "" {
		log.Println("WARNING: SERVICE_TOKEN unset; AppendTurn and non-owner GetTranscript will always fail")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "50053"
	}
	retentionDays := 0
	if v := os.Getenv("RETENTION_DAYS"); v != "" {
		var err error
		retentionDays, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("RETENTION_DAYS=%q is not an integer", v)
		}
	}

	st := postgrest.NewClient(supabaseURL, serviceKey, nil)
	svc := session.NewService(st, serviceToken)

	ctx, stopJanitor := context.WithCancel(context.Background())
	defer stopJanitor()
	if j := session.NewJanitor(st, retentionDays); j != nil {
		log.Printf("retention janitor enabled: ended sessions older than %d days are swept daily", retentionDays)
		go j.Run(ctx)
	}

	addr := ":" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	s := transportgrpc.NewServer(svc)

	go func() {
		log.Printf("session-manager gRPC listening on %s (supabase=%s, service_token_set=%t)", addr, supabaseURL, serviceToken != "")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	stopJanitor()
	s.GracefulStop()
}
