package main

import (
	"fmt"
	"runtime"
)

var (
	Version   = "v0.1.0"
	GitCommit = "dev"
	BuildDate = "2026-11-10"
)

func printVersion() {
	fmt.Printf("AudienceShift Webhook Studio & Tunnel %s (%s, built %s) %s/%s\n",
		Version, GitCommit, BuildDate, runtime.GOOS, runtime.GOARCH)
}
