// Program diskcache implements the Go toolchain cache protocol using a
// local disk directory for storage.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/flax"
	"github.com/creachadair/gocache"
	"github.com/creachadair/gocache/cachedir"
	"github.com/creachadair/mds/value"
)

var flags = struct {
	CacheDir    string        `flag:"cache-dir,Cache directory (required)"`
	Concurrency int           `flag:"c,default=*,Maximum number of concurrent requests"`
	MaxAge      time.Duration `flag:"x,Age after which cache entries expire"`
	Metrics     bool          `flag:"m,Print cache metrics to stderr on exit"`
	Verbose     bool          `flag:"v,Enable verbose logging"`
	DebugLog    bool          `flag:"debug,Enable detailed debug logs (noisy)"`
}{
	Concurrency: runtime.NumCPU(),
}

func main() {
	root := &command.C{
		Name:     command.ProgramName(),
		Usage:    "--cache-dir d [options]\nhelp",
		Help:     `Serve a GOCACHEPROG plugin on stdin/stdout.`,
		SetFlags: command.Flags(flax.MustBind, &flags),
		Run: command.Adapt(func(env *command.Env) error {
			if flags.CacheDir == "" {
				return env.Usagef("You must provide a --cache-dir")
			}

			dir, err := cachedir.New(flags.CacheDir)
			if err != nil {
				return fmt.Errorf("create cache dir: %w", err)
			}
			s := &gocache.Server{
				Get:         dir.Get,
				Put:         dir.Put,
				Close:       dir.Cleanup(flags.MaxAge),
				MaxRequests: flags.Concurrency,
				Logf:        value.Cond(flags.Verbose, log.Printf, nil),
				LogRequests: flags.DebugLog,
			}

			if err := s.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
				log.Printf("Server exited with error: %v", err)
			}
			if flags.Verbose || flags.Metrics {
				fmt.Fprintln(os.Stderr, s.Metrics())
			}
			return nil
		}),
		Commands: []*command.C{
			command.HelpCommand(nil),
			command.VersionCommand(),
		},
	}
	command.RunOrFail(root.NewEnv(nil), os.Args[1:])
}
