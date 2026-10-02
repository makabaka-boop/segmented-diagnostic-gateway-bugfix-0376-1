// Command diaggate runs the diagnostic gateway with 2..4 in-memory virtual
// devices and exposes the TCP client protocol on the chosen listen address.
//
// Usage:
//
//	diaggate -addr 127.0.0.1:7000 -devices 3
//
// This binary never touches real hardware: every device is an in-process
// VirtualDevice that echoes requests, with an injected, configurable delay.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"diaggate"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "TCP listen address for diagnostic clients")
	devices := flag.Int("devices", 3, "number of virtual devices (2..4)")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout")
	delay := flag.Duration("device-delay", 0, "artificial processing delay per virtual device")
	flag.Parse()

	if *devices < 2 || *devices > 4 {
		log.Fatalf("-devices must be in 2..4, got %d", *devices)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clock := diaggate.RealClock{}
	tracer := diaggate.NewLoggingTracer(log.New(os.Stdout, "frame ", log.LstdFlags|log.Lmicroseconds))

	configs := make([]diaggate.VirtualDeviceConfig, *devices)
	for i := range configs {
		configs[i] = diaggate.VirtualDeviceConfig{
			ID:     byte(i + 1),
			Delay:  *delay,
			Clock:  clock,
			Tracer: tracer,
		}
	}
	gw, devs, err := diaggate.NewVirtualTopologyWith(*timeout, ctx, clock, tracer, configs...)
	if err != nil {
		log.Fatalf("build topology: %v", err)
	}
	defer func() {
		_ = gw.Close()
		for _, d := range devs {
			_ = d.Close()
		}
	}()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := diaggate.ServeTCP(ln, diaggate.ServerConfig{Gateway: gw, Clock: clock})
	log.Printf("diagnostic gateway listening on %s with %d virtual devices", ln.Addr(), *devices)

	<-ctx.Done()
	log.Println("shutting down")
	_ = srv.Close()
}
