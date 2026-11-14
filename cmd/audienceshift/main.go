package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(0)
	}

	command := os.Args[1]

	switch command {
	case "server":
		runServer(os.Args[2:])
	case "listen":
		runListen(os.Args[2:])
	case "mock":
		runMock(os.Args[2:])
	case "trigger":
		runTrigger(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	case "version", "-v", "--version":
		printVersion()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %q\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`AudienceShift — Self-Hosted Webhook Studio & Tunnel

Usage:
  audienceshift <command> [arguments]

Commands:
  server    Start the public tunnel relay server on your VPS
  listen    Connect local service to tunnel and launch Webhook Studio
  mock      Start offline standalone webhook receiver (no tunnel/VPS required)
  trigger   Send simulated webhook from preset catalog to localhost
  verify    Run automated webhook compliance & signature security test suite
  version   Show version and build information
  help      Show this help message

Server Examples:
  audienceshift server --addr :8080 --domain tunnel.example.com
  audienceshift server --addr :443 --domain tunnel.example.com --cert cert.pem --key key.pem

Client Examples:
  audienceshift listen 3000 --server wss://tunnel.example.com/tunnel/connect
  audienceshift listen 8000 --server ws://localhost:8080/tunnel/connect --web
  audienceshift listen http://localhost:8080/webhooks --headless --export session.json

Verification Suite Examples:
  audienceshift verify http://localhost:8080/webhooks/stripe --provider stripe --secret whsec_test
  audienceshift verify http://localhost:3000/api/github --provider github --secret my_github_secret --json

Offline Mock Examples:
  audienceshift mock 3000
  audienceshift mock 8080 --port 9000 --web
  audienceshift mock --port 9000 --forward=false --headless

Trigger & Chaos Examples:
  audienceshift trigger -list
  audienceshift trigger stripe:payment_intent.succeeded 3000 --secret whsec_test
  audienceshift trigger midtrans:settlement 8080 --duplicate

Web Studio:
  Pass the '--web' flag during 'listen' to open the embedded browser dashboard at http://localhost:4040.`)
}
