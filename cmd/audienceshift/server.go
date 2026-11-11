package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/crypto/acme/autocert"

	"github.com/pandusatrianura/AudienceShift/internal/features/tunnel"
)

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "TCP address for HTTP/HTTPS ingress and tunnel connection")
	domain := fs.String("domain", "", "Base domain name for subdomain routing (e.g. tunnel.mycompany.com)")
	token := fs.String("token", "", "Shared authentication secret required for tunnel clients")
	certFile := fs.String("cert", "", "Path to custom TLS certificate file")
	keyFile := fs.String("key", "", "Path to custom TLS private key file")
	acme := fs.Bool("acme", false, "Enable automatic Let's Encrypt TLS certificate provisioning")
	acmeEmail := fs.String("acme-email", "", "Contact email for Let's Encrypt registration and notifications")
	acmeCache := fs.String("acme-cache", "./certs", "Directory path to cache issued TLS certificates")
	acmeHosts := fs.String("acme-hosts", "", "Allowed hostnames for ACME certificates, comma-separated (default: domain)")

	_ = fs.Parse(args)

	tunnelServer := tunnel.NewServer(*domain, *token)

	fmt.Println("──────────────────────────────────────────────────────────")
	fmt.Printf("🚀 AudienceShift Tunnel Relay Server running on %s\n", *addr)
	if *domain != "" {
		fmt.Printf("🌐 Subdomain Base: *.%s\n", *domain)
	}
	if *token != "" {
		fmt.Println("🔒 Client authentication enabled (token required)")
	}
	fmt.Println("──────────────────────────────────────────────────────────")

	var err error
	if *acme {
		if *domain == "" && *acmeHosts == "" {
			log.Fatalf("Error: --domain or --acme-hosts must be specified when using --acme")
		}

		if err := os.MkdirAll(*acmeCache, 0700); err != nil {
			log.Fatalf("Failed to create ACME cache directory %s: %v", *acmeCache, err)
		}

		allowedHosts := []string{*domain}
		if *acmeHosts != "" {
			allowedHosts = strings.Split(*acmeHosts, ",")
			for i := range allowedHosts {
				allowedHosts[i] = strings.TrimSpace(allowedHosts[i])
			}
		}

		certManager := &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(allowedHosts...),
			Cache:      autocert.DirCache(*acmeCache),
			Email:      *acmeEmail,
		}

		// Stand up HTTP-01 challenge responder & HTTP->HTTPS redirect on port 80
		go func() {
			log.Println("Starting HTTP-to-HTTPS redirect & ACME HTTP-01 responder on :80...")
			httpServer := &http.Server{
				Addr:    ":80",
				Handler: certManager.HTTPHandler(nil),
			}
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("ACME HTTP listener error: %v", err)
			}
		}()

		httpsServer := &http.Server{
			Addr:    *addr,
			Handler: tunnelServer,
			TLSConfig: &tls.Config{
				GetCertificate: certManager.GetCertificate,
				NextProtos:     []string{"h2", "http/1.1", "acme-tls/1"},
			},
		}

		fmt.Printf("🔒 Let's Encrypt TLS enabled for %v (cache: %s)\n", allowedHosts, *acmeCache)
		err = httpsServer.ListenAndServeTLS("", "")
	} else if *certFile != "" && *keyFile != "" {
		err = http.ListenAndServeTLS(*addr, *certFile, *keyFile, tunnelServer)
	} else {
		err = http.ListenAndServe(*addr, tunnelServer)
	}

	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server exited with error: %v", err)
	}
}
