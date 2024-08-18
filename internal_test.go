package gocache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/taskgroup"
	gocmp "github.com/google/go-cmp/cmp"
)

func TestServer(t *testing.T) {
	const (
		actionMiss  = "01"
		actionHit   = "02"
		actionError = "99"
		testObject  = "0b1ec7"
	)

	// Create an output file for a test object, so that the server plumbing can
	// successfully find it when it's reported back.
	// Note we set the modification time here so that the output will be stable.
	objTime := time.Date(2024, 8, 17, 14, 42, 45, 0, time.UTC)
	objPath := filepath.Join(t.TempDir(), testObject)
	if err := os.WriteFile(objPath, []byte("xyzzy"), 0600); err != nil {
		t.Fatalf("Create test object: %v", err)
	}
	if err := os.Chtimes(objPath, time.Time{} /* atime */, objTime); err != nil {
		t.Fatalf("Set timestamp: %v", err)
	}

	// Wire up a known value to the context, to exercise context plumbing
	type testKey struct{}
	ctx := context.WithValue(context.Background(), testKey{}, true)
	checkContext := func(ctx context.Context) {
		t.Helper()
		if ctx.Value(testKey{}) == nil {
			t.Error("Context plumbing did not work")
		}
	}

	var logBuf bytes.Buffer
	var didClose, didMiss, didError, didSetMetrics atomic.Bool
	s := &Server{
		Get: func(ctx context.Context, actionID string) (objectID, diskPath string, _ error) {
			checkContext(ctx)
			switch actionID {
			case actionMiss:
				didMiss.Store(true)
				return "", "", nil // cache miss
			case actionHit:
				return testObject, objPath, nil
			case actionError:
				didError.Store(true)
				return "", "", errors.New("erroneous condition")
			default:
				panic("unexpected case")
			}
		},
		Put: func(ctx context.Context, obj Object) (diskPath string, _ error) {
			checkContext(ctx)
			return objPath, nil
		},
		Close: func(ctx context.Context) error {
			checkContext(ctx)
			didClose.Store(true)
			Logf(ctx, "context-logger-present")
			return nil
		},
		SetMetrics: func(ctx context.Context, m *expvar.Map) {
			checkContext(ctx)
			didSetMetrics.Store(true)
		},
		Logf: log.New(&logBuf, "", log.LstdFlags).Printf,
	}
	cr, sw := io.Pipe() // server to client
	sr, cw := io.Pipe() // client to server
	dec := json.NewDecoder(cr)
	enc := json.NewEncoder(cw)
	var wmu sync.Mutex
	send := func(v any) {
		wmu.Lock()
		defer wmu.Unlock()
		if err := enc.Encode(v); err != nil {
			t.Errorf("Encode: unexpected error: %v", err)
		}
	}
	receive := func() *progResponse {
		var rsp progResponse
		if err := dec.Decode(&rsp); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			t.Fatalf("Decode: unexpected error: %v", err)
		}
		return &rsp
	}

	srv := taskgroup.Go(func() error {
		return s.Run(ctx, sr, sw)
	})

	// Check that the server reports the initial banner listing the supported
	// commands. Do this separately to ensure it arrives first.
	if diff := gocmp.Diff(receive(), &progResponse{
		ID: 0, KnownCommands: []string{"get", "put", "close"},
	}); diff != "" {
		t.Fatalf("Server banner (-got, +want):\n%s", diff)
	}

	// Receive and buffer responses concurrently.
	rsps := make(map[int64]*progResponse)
	rcv := taskgroup.Go(taskgroup.NoError(func() {
		for {
			msg := receive()
			if msg == nil {
				return
			}
			if rsps[msg.ID] != nil {
				t.Errorf("Duplicate ID %d received", msg.ID)
			} else {
				rsps[msg.ID] = msg
			}
		}
	}))

	// Send a bunch of known requests...
	program := []*progRequest{
		{ID: 1, Command: "get", ActionID: []byte("\x01")},
		{ID: 2, Command: "get", ActionID: []byte("\x02")},
		{ID: 3, Command: "get", ActionID: []byte("\x99")},
		{ID: 4, Command: "put",
			ActionID: []byte("\x03"),
			ObjectID: []byte("\x0b\x1e\xc7"),
			BodySize: 5,
			Body:     strings.NewReader("xyzzy"),
		},
		{ID: 999, Command: "close"},
	}
	var g taskgroup.Group
	for i, req := range program {
		g.Go(taskgroup.NoError(func() {
			send(req)
			if req.BodySize > 0 {
				b, err := io.ReadAll(req.Body)
				if err != nil {
					t.Errorf("Send [%d]: read body: %v", i+1, err)
				} else {
					send(b)
				}
			}
		}))
	}
	g.Wait()
	// sends complete

	cw.Close()
	if err := srv.Wait(); err != nil {
		t.Errorf("Server exit: unexpected error: %v", err)
	}
	// server complete

	sw.Close()
	rcv.Wait()
	// receiver complete

	// Verify that everything got called.
	if !didClose.Load() {
		t.Error("Close handler was not called")
	}
	if !didMiss.Load() {
		t.Error("Get did not report a miss")
	}
	if !didError.Load() {
		t.Error("Get did not report an error")
	}
	if !didSetMetrics.Load() {
		t.Error("SetMetrics was not called")
	}

	// Check that we got the desired responses.
	if diff := gocmp.Diff(rsps, map[int64]*progResponse{
		1:   {ID: 1, Miss: true},
		2:   {ID: 2, Size: 5, Time: &objTime, DiskPath: objPath},
		3:   {ID: 3, Err: "get 99: erroneous condition"},
		4:   {ID: 4, DiskPath: objPath},
		999: {ID: 999}, // close response
	}); diff != "" {
		t.Errorf("Responses (-got, +want):\n%s", diff)
	}

	// Check that log messages got propagated.
	logText := logBuf.String()
	t.Logf("Log output:\n%s", logText)
	for _, want := range []string{
		"cache server started",
		"erroneous condition",
		"cache server exiting",
		"context-logger-present",
	} {
		if !strings.Contains(logText, want) {
			t.Errorf("Missing log string: %v", want)
		}
	}
}
