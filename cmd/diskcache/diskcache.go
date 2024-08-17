// Program diskcache implements the Go toolchain cache protocol using a
// local disk directory for storage.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/creachadair/gocache"
	"github.com/creachadair/gocache/cachedir"
	"github.com/creachadair/mds/value"
)

var (
	cacheDir  = flag.String("cache-dir", "", "Cache directory (required)")
	maxAge    = flag.Duration("x", 24*time.Hour, "Age after which cache entries expire")
	doVerbose = flag.Bool("v", false, "Enable verbose logging")
)

func main() {
	flag.Parse()

	if *cacheDir == "" {
		log.Fatal("You must provide a --cache-dir")
	}

	dir, err := cachedir.New(*cacheDir)
	if err != nil {
		log.Fatalf("Create cache dir: %v", err)
	}
	s := &gocache.Server{
		Get:   dir.Get,
		Put:   dir.Put,
		Close: dir.Cleanup(*maxAge),

		Logf: value.Cond(*doVerbose, log.Printf, nil),
	}

	if err := s.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Printf("Server exited with error: %v", err)
	}
	if *doVerbose {
		fmt.Fprintln(os.Stderr, s.Metrics())
	}
}
