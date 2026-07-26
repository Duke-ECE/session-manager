package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/session"
	"github.com/Duke-ECE/session-manager/internal/store"
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

	st := store.NewPostgREST(supabaseURL, serviceKey, nil)
	srv := session.NewServer(st, serviceToken)

	addr := ":" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	s := grpc.NewServer()
	v1.RegisterSessionServiceServer(s, srv)
	reflection.Register(s)

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
	s.GracefulStop()
}
