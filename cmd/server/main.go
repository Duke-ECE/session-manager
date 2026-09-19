package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/postgrest"
	"github.com/Duke-ECE/session-manager/internal/session"
	transportgrpc "github.com/Duke-ECE/session-manager/internal/transport/grpc"
)

// store is the persistence the binary needs: the v1 lifecycle port plus the
// durable-context (v2) port. One implementation serves both transports.
type store interface {
	session.Store
	session.DurableStore
}

func main() {
	supabaseURL := os.Getenv("SUPABASE_URL")
	serviceKey := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")

	var st store
	if supabaseURL == "" || serviceKey == "" {
		// Zero-dependency local run: same state machine, no durability. Production
		// (k8s.yaml) always sets both, so this path is dev-only and says so loudly.
		log.Println("WARNING: SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY unset; using the in-memory store (nothing is durable)")
		st = memory.New()
	} else {
		st = postgrest.NewClient(supabaseURL, serviceKey, nil)
	}

	serviceToken := os.Getenv("SERVICE_TOKEN")
	if serviceToken == "" {
		log.Println("WARNING: SERVICE_TOKEN unset; internal operations and non-owner reads will always fail")
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

	svcV1 := session.NewService(st, serviceToken)
	svcV2 := session.NewDurableService(st, serviceToken)

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

	s := transportgrpc.NewServer(svcV1, svcV2)

	go func() {
		log.Printf("session-manager gRPC listening on %s (supabase_configured=%t, service_token_set=%t)",
			addr, supabaseURL != "" && serviceKey != "", serviceToken != "")
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
