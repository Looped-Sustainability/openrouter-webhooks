package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	Addr, DataDir, Token, OpenRouterKey, WebhookSecret     string
	WebhookHosts                                           map[string]bool
	Workers, DeliveryWorkers, MaxJobs, MaxDeliveryAttempts int
	ModelTimeout, WebhookTimeout, Retention                time.Duration
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func loadConfig() (config, error) {
	c := config{Addr: env("ADDR", ":8080"), DataDir: env("DATA_DIR", "./data"), Token: os.Getenv("API_TOKEN"), OpenRouterKey: os.Getenv("OPENROUTER_API_KEY"), WebhookSecret: os.Getenv("WEBHOOK_SECRET"), WebhookHosts: map[string]bool{}}
	if len(c.Token) < 32 || len(c.WebhookSecret) < 32 || c.OpenRouterKey == "" {
		return c, errors.New("API_TOKEN and WEBHOOK_SECRET must each have at least 32 characters; OPENROUTER_API_KEY is required")
	}
	for _, host := range strings.Split(os.Getenv("WEBHOOK_HOSTS"), ",") {
		if host = strings.ToLower(strings.TrimSpace(host)); host != "" {
			c.WebhookHosts[host] = true
		}
	}
	if len(c.WebhookHosts) == 0 {
		return c, errors.New("WEBHOOK_HOSTS must list allowed webhook host names")
	}
	for _, item := range []struct {
		key, def string
		dst      *int
	}{{"MODEL_CONCURRENCY", "300", &c.Workers}, {"WEBHOOK_CONCURRENCY", "50", &c.DeliveryWorkers}, {"MAX_JOBS", "10000", &c.MaxJobs}, {"WEBHOOK_ATTEMPTS", "12", &c.MaxDeliveryAttempts}} {
		v, err := strconv.Atoi(env(item.key, item.def))
		if err != nil || v < 1 {
			return c, errors.New("invalid positive integer: " + item.key)
		}
		*item.dst = v
	}
	for _, item := range []struct {
		key, def string
		dst      *time.Duration
	}{{"MODEL_TIMEOUT", "10m", &c.ModelTimeout}, {"WEBHOOK_TIMEOUT", "15s", &c.WebhookTimeout}, {"RETENTION", "24h", &c.Retention}} {
		v, err := time.ParseDuration(env(item.key, item.def))
		if err != nil || v <= 0 {
			return c, errors.New("invalid positive duration: " + item.key)
		}
		*item.dst = v
	}
	return c, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := loadConfig()
	if err != nil {
		return err
	}
	s, err := newService(c)
	if err != nil {
		return err
	}
	defer s.close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.start(workerCtx)
	server := &http.Server{Addr: c.Addr, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("service ready", "address", c.Addr, "model_workers", c.Workers, "webhook_workers", c.DeliveryWorkers)
	select {
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			cancel()
			s.wg.Wait()
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, finish := context.WithTimeout(context.Background(), 10*time.Second)
	defer finish()
	_ = server.Shutdown(shutdownCtx)
	cancel()
	s.wg.Wait()
	return nil
}
func authorized(r *http.Request, token string) bool {
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) == 1
}
