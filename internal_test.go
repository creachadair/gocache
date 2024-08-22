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
		Logf:        log.New(&logBuf, "", log.LstdFlags).Printf,
		LogRequests: true,
	}
	cr, sw := io.Pipe() // server to client
	sr, cw := io.Pipe() // client to server
	dec := json.NewDecoder(cr)
	enc := json.NewEncoder(cw)

	send := func(v any) {
		t.Helper()
		if err := enc.Encode(v); err != nil {
			t.Errorf("Encode: unexpected error: %v", err)
		}
	}
	recv := func() *progResponse {
		t.Helper()
		var rsp progResponse
		if err := dec.Decode(&rsp); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			t.Fatalf("Decode: unexpected error: %v", err)
		}
		return &rsp
	}

	// Run the server...
	srv := taskgroup.Go(func() error {
		return s.Run(ctx, sr, sw)
	})

	// Run the client...
	rsps := make(map[int64]*progResponse)
	cli := taskgroup.Go(taskgroup.NoError(func() {
		defer cw.Close() // close the channel to the server

		// The test program specifies the order of operations the client executes.
		// Each step sends and/or receives a message.
		// The total number of receives must match the number of sends.
		program := []struct {
			send *progRequest  // send this request to the server
			want *progResponse // wait for a response and ensure it matches
			wait bool          // wait for an arbitrary response
		}{
			{want: &progResponse{ID: 0, KnownCommands: []string{"get", "put", "close"}}},
			{
				send: &progRequest{ID: 1, Command: "get", ActionID: []byte("\x01")},
				want: &progResponse{ID: 1, Miss: true},
			},
			{send: &progRequest{ID: 2, Command: "get", ActionID: []byte("\x02")}},
			{send: &progRequest{ID: 3, Command: "get", ActionID: []byte("\x99")}},
			{wait: true},
			{send: &progRequest{ID: 4, Command: "put",
				ActionID: []byte("\x03"),
				ObjectID: []byte("\x0b\x1e\xc7"),
				BodySize: 5,
				Body:     strings.NewReader("xyzzy"),
			}},
			{wait: true},
			{send: &progRequest{ID: 999, Command: "close"}},
			{wait: true},
			{wait: true},
		}
		for i, tc := range program {
			if tc.send != nil {
				send(tc.send)
				if tc.send.BodySize > 0 {
					b, err := io.ReadAll(tc.send.Body)
					if err != nil {
						t.Errorf("Send [%d]: read body: %v", i+1, err)
					} else {
						send(b)
					}
				}
			}
			if tc.wait || tc.want != nil {
				rsp := recv()
				if tc.want != nil {
					if diff := gocmp.Diff(rsp, tc.want); diff != "" {
						t.Errorf("Recv [%d] (-got, +want):\n%s", i+1, diff)
					}
				} else {
					rsps[rsp.ID] = rsp
				}
			}
		}
	}))
	cli.Wait()
	// client complete

	if err := srv.Wait(); err != nil {
		t.Errorf("Server exit: unexpected error: %v", err)
	}
	sw.Close()
	// server complete

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

		// Check for output from the detailed request logs.
		"B PUT R:4",
		"E PUT R:4, err <nil>",
		"B CLOSE R:999",
		"E CLOSE R:999, err <nil>",
	} {
		if !strings.Contains(logText, want) {
			t.Errorf("Missing log string: %v", want)
		}
	}
}
