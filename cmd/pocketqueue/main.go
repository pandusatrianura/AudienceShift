package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/demo"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/pandusatrianura/pocketqueue/internal/features/tui"
	"github.com/pandusatrianura/pocketqueue/internal/platform/logger"
)

func main() {
	bootStart := time.Now()
	cfg := parseFlags()

	// If user passed -topic, run in CLI producer mode
	if cfg.ProduceTopic != "" {
		if cfg.ProduceValue == "" {
			fmt.Fprintln(os.Stderr, "Error: -value is required when publishing to -topic")
			os.Exit(1)
		}
		if err := PublishCLI(cfg.TargetBroker, cfg.ProduceTopic, cfg.ProduceKey, cfg.ProduceValue); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Initialize Logger
	slogLevel := parseSlogLevel(cfg.LogLevel)
	isTerminal := isatty.IsTerminal(os.Stdout.Fd()) && !cfg.Headless

	if isTerminal {
		// In TUI mode, route logs to pocketqueue.log to avoid corrupting terminal rendering
		logFile, err := os.OpenFile("pocketqueue.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			defer logFile.Close()
			logger.Setup(logFile, slogLevel, cfg.JSONLog)
		} else {
			logger.Setup(os.Stderr, slogLevel, cfg.JSONLog)
		}
	} else {
		logger.Setup(os.Stdout, slogLevel, cfg.JSONLog)
	}

	// Root context for lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Storage Engine (WAL vs In-Memory)
	var engine storage.Engine
	var err error
	if cfg.DataDir != "" {
		engine, err = storage.NewWALEngine(cfg.DataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing WAL storage in %s: %v\n", cfg.DataDir, err)
			os.Exit(1)
		}
	} else {
		engine = storage.NewMemoryEngine(cfg.MaxMessages)
	}

	bus := broker.NewEventBus(1000)
	brokerSvc := broker.NewService(engine, bus, 1)
	defer brokerSvc.Close()

	// If --demo is requested, start background demo event generator
	if cfg.WithDemo {
		gen := demo.NewGenerator(brokerSvc)
		go gen.Start(ctx, 800*time.Millisecond)
	}

	// 2. Start Kafka Protocol Server (port 9092)
	var kafkaServer *kafka.Server
	if cfg.Port > 0 {
		listenAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		handler := kafka.NewHandler(brokerSvc, cfg.Host, int32(cfg.Port), nil)
		kafkaServer = kafka.NewServer(listenAddr, handler, nil)
		if err := kafkaServer.Start(); err != nil {
			if errors.Is(err, kafka.ErrPortInUse) {
				fmt.Fprintf(os.Stderr, "\n❌ Kafka port %d is already in use by another process (e.g. Docker Kafka/local service).\n   👉 Use --port <alt> to specify a different port (e.g. --port 9094)\n\n", cfg.Port)
			} else {
				fmt.Fprintf(os.Stderr, "Error starting Kafka protocol server: %v\n", err)
			}
			os.Exit(1)
		}
		defer kafkaServer.Close()
	}

	// 3. Start Redis Streams Protocol Server (port 6379)
	var redisServer *redis.Server
	if cfg.RedisPort > 0 {
		redisAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort)
		redisServer = redis.NewServer(redisAddr, brokerSvc, nil)
		if err := redisServer.Start(); err != nil {
			if errors.Is(err, redis.ErrPortInUse) {
				fmt.Fprintf(os.Stderr, "\n❌ Redis port %d is already in use by another process (e.g. Docker Redis/local service).\n   👉 Use --redis-port <alt> to specify a different port (e.g. --redis-port 6380)\n\n", cfg.RedisPort)
			} else {
				fmt.Fprintf(os.Stderr, "Error starting Redis Streams protocol server: %v\n", err)
			}
			os.Exit(1)
		}
		defer redisServer.Close()
	}

	// Setup OS interrupt trap
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	bootDuration := time.Since(bootStart)

	if isTerminal {
		// Run Charm Bubbletea TUI
		appModel := tui.NewAppModel(brokerSvc, bus, cfg.MaxMessages)
		p := tea.NewProgram(appModel, tea.WithAltScreen())

		go func() {
			<-sigChan
			cancel()
			p.Quit()
		}()

		if _, err := p.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error running TUI: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Headless mode
		endpoints := ""
		if kafkaServer != nil {
			endpoints += fmt.Sprintf("Kafka:%s ", kafkaServer.Addr())
		}
		if redisServer != nil {
			endpoints += fmt.Sprintf("Redis:%s ", redisServer.Addr())
		}
		storageMode := "In-Memory"
		if cfg.DataDir != "" {
			storageMode = fmt.Sprintf("WAL(%s)", cfg.DataDir)
		}
		demoNote := ""
		if cfg.WithDemo {
			demoNote = " [Demo Active]"
		}

		fmt.Printf("⚡ PocketQueue running (%s, Storage: %s)%s (booted in %v, Press Ctrl+C to stop)\n",
			endpoints, storageMode, demoNote, bootDuration)
		<-sigChan
		cancel()
		fmt.Println("\nShutting down PocketQueue gracefully...")
	}
}
