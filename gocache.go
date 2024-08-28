// Package gocache implements a server stub for the Go toolchain
// cache process protocol.
//
// As of 15-Aug-2024, support for the GOCACHEPROG environment variable still
// requires building a custom Go toolchain with GOEXPERIMENT=cacheprog set.
// See https://github.com/golang/go/issues/64876 for discussion.
//
// The implementation in this package is based on the code from the internal
// https://pkg.go.dev/cmd/go/internal/cache package.
package gocache

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
)

// Server defines callbacks to process cache requests from the client.
type Server struct {
	// Get fetches the object for the specified action ID.
	// If nil, the server reports a cache miss for all actions.
	//
	// On success, Get must return the object ID for the specified action, and
	// the path of a local file containing the object's contents.
	//
	// To report a cache miss, Get must return "", "", nil.
	//
	// API: "get"
	Get func(ctx context.Context, actionID string) (objectID, diskPath string, _ error)

	// Put stores the specified object for an action.
	// If nil, the server will reject requests to write to the cache.
	//
	// On success, Put must return the path of a local file containing the
	// cached contents for the object.
	//
	// API: "put"
	Put func(ctx context.Context, req Object) (diskPath string, _ error)

	// Close is called once when the client closes its channel to the server.
	// If nil, the server stops immediately without waiting.
	//
	// API: "close"
	Close func(context.Context) error

	// SetMetrics, if non-nil, is called once when the server starts up.  The
	// function should populate the provided map with any metrics it wishes to
	// expose via the service's Metrics method (under "host").
	SetMetrics func(ctx context.Context, m *expvar.Map)

	// Logf, if non-nil, is used to write log messages.  If nil, logs are
	// discarded.
	Logf func(string, ...any)

	// MaxRequests determines the maximum number of requests that may be
	// serviced concurrently by the server. If zero, it uses runtime.NumCPU.
	MaxRequests int

	// LogRequests, if true, enables detailed (but noisy) debug logging of all
	// requests received and handled by the server.
	//
	// Each request is presented as a pair of logs:
	//
	//    B <command> R:<id> ...
	//    E <command> R:<id> ... err <error>, <time> elapsed
	//
	// The "B" line is logged when the request begins and describes the inputs,
	// The "E" line is logged when the request ends and reports the results, any
	// error that occurred, and the time elapsed. Fields are given in a brief
	// format:
	//
	//    R:<request-id>
	//    A:<action-id>
	//    O:<object-id>
	//    S:<size>         -- for "put" requests, object size in bytes
	//    M:<miss>         -- for "get" requests, true/false
	//    DP:"<diskpath>"
	//
	LogRequests bool

	// Metrics
	getRequests expvar.Int
	getHits     expvar.Int
	getHitBytes expvar.Int
	getMisses   expvar.Int
	getErrors   expvar.Int
	putRequests expvar.Int
	putBytes    expvar.Int
	putErrors   expvar.Int
	hostMetrics expvar.Map
}

// Metrics returns a map of server metrics. The caller is responsible for
// exporting these metrics.
func (s *Server) Metrics() *expvar.Map {
	m := new(expvar.Map)
	m.Set("host", &s.hostMetrics)

	sm := new(expvar.Map)
	sm.Set("get_requests", &s.getRequests)
	sm.Set("get_hits", &s.getHits)
	sm.Set("get_hit_bytes", &s.getHitBytes)
	sm.Set("get_misses", &s.getMisses)
	sm.Set("get_errors", &s.getErrors)
	sm.Set("put_requests", &s.putRequests)
	sm.Set("put_bytes", &s.putBytes)
	sm.Set("put_errors", &s.putErrors)
	m.Set("server", sm)

	return m
}

// Run starts the server reading requests from in and writing responses to
// out. Each valid request is passed to the corresponding callback, if defined.
// Run blocks running the server until ctx ends, reading in reports an error,
// or decoding a client request fails.
//
// If in reports io.EOF, Run returns nil; otherwise it reports the error that
// terminated the service.
func (s *Server) Run(ctx context.Context, in io.Reader, out io.Writer) (xerr error) {
	if s.SetMetrics != nil {
		s.SetMetrics(ctx, &s.hostMetrics)
	}
	rd := bufio.NewReader(in)
	dec := json.NewDecoder(rd)

	var emu sync.Mutex // lock to write to enc
	wr := bufio.NewWriter(out)
	enc := json.NewEncoder(wr)
	encode := func(v any) error {
		emu.Lock()
		defer emu.Unlock()
		if err := enc.Encode(v); err != nil {
			return err
		}
		return wr.Flush()
	}

	// Write the initial message advertising available methods.
	if err := encode(&progResponse{ID: 0, KnownCommands: s.commands()}); err != nil {
		return fmt.Errorf("write server init: %w", err)
	}

	s.logf("cache server started")
	start := time.Now()
	defer func() {
		s.logf("cache server exiting (%v elapsed, err=%v)",
			time.Since(start).Round(100*time.Microsecond), xerr)
	}()

	g, run := taskgroup.New(nil).Limit(s.maxRequests())
	defer g.Wait()

	runCtx := WithLogf(ctx, s.logf)
	for {
		var req progRequest
		if err := dec.Decode(&req); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}

		// A "put" request with a non-zero body size is followed immediately by
		// the contents of the body as a JSON string (base64).
		if req.Command == "put" && req.BodySize > 0 {
			var body []byte
			if err := dec.Decode(&body); err != nil {
				return fmt.Errorf("request %d: decode body: %w", req.ID, err)
			}
			if int64(len(body)) != req.BodySize {
				return fmt.Errorf("request %d body: got %d bytes, want %d", req.ID, len(body), req.BodySize)
			}
			s.putBytes.Add(req.BodySize)
			req.Body = bytes.NewReader(body)
		}

		run(func() error {
			rsp, err := s.handleRequest(runCtx, &req)
			if err != nil {
				s.logf("request %d failed: %v", req.ID, err)
				rsp = &progResponse{ID: req.ID, Err: err.Error()}
			} else {
				rsp.ID = req.ID
			}
			return encode(rsp)
		})
	}
}

// handleRequest returns the response corresponding to req, or an error.
func (s *Server) handleRequest(ctx context.Context, req *progRequest) (pr *progResponse, oerr error) {
	start := time.Now()
	switch req.Command {
	case "get":
		s.vlogf("bc B GET R:%d, A:%x", req.ID, req.ActionID)
		defer func() {
			isMiss := pr != nil && pr.Miss
			if isMiss {
				s.getMisses.Add(1)
			}
			if oerr != nil {
				s.getErrors.Add(1)
			}
			s.vlogf("bc E GET R:%d, A:%x, M:%v, err %v, %v elapsed, DP:%q",
				req.ID, req.ActionID, isMiss, oerr, time.Since(start), value.At(pr).DiskPath)
		}()
		s.getRequests.Add(1)

		if s.Get == nil {
			return &progResponse{Miss: true}, nil
		}
		objectID, diskPath, err := s.Get(ctx, fmt.Sprintf("%x", req.ActionID))
		if err != nil {
			return nil, fmt.Errorf("get %x: %w", req.ActionID, err)
		} else if objectID == "" && diskPath == "" {
			return &progResponse{Miss: true}, nil
		}

		// Safety check: The object ID should be hex-encoded and non-empty.
		if objectID == "" {
			return nil, errors.New("get: empty object ID")
		} else if _, err := hex.DecodeString(objectID); err != nil {
			return nil, fmt.Errorf("get: invalid object ID: %w", err)
		}

		// Safety check: The object file must exist and be a regular file.
		fi, err := os.Stat(diskPath)
		if errors.Is(err, os.ErrNotExist) {
			// Treat a missing object as a normal cache miss, to allow for the
			// possibility that the action record has gone out of sync due to
			// cache pruning or a concurrent update to the same ID.
			return &progResponse{Miss: true}, nil
		} else if err != nil {
			return nil, fmt.Errorf("get: verify path: %w", err)
		} else if !fi.Mode().IsRegular() {
			return nil, fmt.Errorf("get: verify path: not a regular file: %q", diskPath)
		}

		// Cache hit.
		s.getHits.Add(1)
		s.getHitBytes.Add(fi.Size())
		added := fi.ModTime().UTC()
		return &progResponse{Size: fi.Size(), Time: &added, DiskPath: diskPath}, nil

	case "put":
		s.vlogf("bc B PUT R:%d, A:%x, O:%x, S:%d", req.ID, req.ActionID, req.ObjectID, req.BodySize)
		defer func() {
			if oerr != nil {
				s.putErrors.Add(1)
			}
			s.vlogf("bc E PUT R:%d, err %v, %v elapsed, DP:%q",
				req.ID, oerr, time.Since(start), value.At(pr).DiskPath)
		}()
		s.putRequests.Add(1)

		// If no body was provided, swap in an empty reader.
		body := cmp.Or(req.Body, io.Reader(strings.NewReader("")))
		defer io.Copy(io.Discard, body)
		if s.Put == nil {
			return nil, errors.New("put: cache is read-only")
		}

		diskPath, err := s.Put(ctx, Object{
			ActionID: fmt.Sprintf("%x", req.ActionID),
			ObjectID: fmt.Sprintf("%x", req.ObjectID),
			Size:     req.BodySize,
			Body:     body,
		})
		if err != nil {
			return nil, fmt.Errorf("put %x: %w", req.ActionID, err)
		}

		// Safety check: The object file must exist and match the provided size.
		fi, err := os.Stat(diskPath)
		if err != nil {
			return nil, fmt.Errorf("put action %x verify: %w", req.ActionID, err)
		} else if fi.Size() != req.BodySize {
			return nil, fmt.Errorf("put action %x verify %q: got %d bytes, want %d",
				req.ActionID, diskPath, fi.Size(), req.BodySize)
		}

		// Write successful.
		s.putBytes.Add(fi.Size())
		return &progResponse{DiskPath: diskPath}, nil

	case "close":
		if s.Close != nil {
			s.vlogf("bc B CLOSE R:%d", req.ID)
			defer func() {
				s.vlogf("bc E CLOSE R:%d, err %v, %v elapsed", req.ID, oerr, time.Since(start))
			}()
			return &progResponse{}, s.Close(ctx)
		}
		return &progResponse{}, nil

	default:
		return nil, fmt.Errorf("unknown command %q", req.Command)
	}
}

func (s *Server) logf(msg string, args ...any) {
	if s.Logf != nil {
		s.Logf(msg, args...)
	}
}

func (s *Server) vlogf(msg string, args ...any) {
	if s.LogRequests {
		s.logf(msg, args...)
	}
}

func (s *Server) maxRequests() int {
	if s.MaxRequests > 0 {
		return s.MaxRequests
	}
	return runtime.NumCPU()
}

func (s *Server) commands() []string {
	var out []string
	if s.Get != nil {
		out = append(out, "get")
	}
	if s.Put != nil {
		out = append(out, "put")
	}
	if s.Close != nil {
		out = append(out, "close")
	}
	return out
}

// An Object defines an object to be stored into the cache.
type Object struct {
	ActionID string    // non-empty; lower-case hexadecimal digits
	ObjectID string    // non-empty; lower-case hexadecimal digits
	Size     int64     // object size in bytes
	Body     io.Reader // always non-nil
	ModTime  time.Time // if non-zero, set the object mod-time to this
}

// Logf writes a log to the logger associated with ctx, if one is defined.
// The context passed to the callbacks of a Server supports this.
func Logf(ctx context.Context, msg string, args ...any) {
	logf, ok := ctx.Value(logKey{}).(func(string, ...any))
	if ok {
		logf(msg, args...)
	}
}

// WithLogf returns a child of ctx with the specified log function attached.
// The resulting context can be used with [Logf] to send logs to f.
func WithLogf(ctx context.Context, f func(string, ...any)) context.Context {
	return context.WithValue(ctx, logKey{}, f)
}

type logKey struct{}
