package gocache

import (
	"io"
	"time"
)

// progRequest is a JSON encoded request from the client.
//
// Copied from: https://pkg.go.dev/cmd/go/internal/cache#ProgRequest with minor
// edits.
type progRequest struct {
	// ID is a unique number per process across all requests.
	// It must be echoed in the ProgResponse from the child.
	ID int64

	// Command is the type of request.
	// The cmd/go tool will only send commands that were declared
	// as supported by the child.
	Command string

	// ActionID is non-nil for get and puts.
	ActionID []byte `json:",omitempty"` // or nil if not used

	// OutputID is set for Type "put" and "output-file".
	//
	// TODO(creachadair): Remove the name override once the default changes.
	OutputID []byte `json:"ObjectID,omitempty"` // or nil if not used

	// Body is the body for "put" requests. It's sent after the JSON object
	// as a base64-encoded JSON string when BodySize is non-zero.
	// It's sent as a separate JSON value instead of being a struct field
	// send in this JSON object so large values can be streamed in both directions.
	// The base64 string body of a ProgRequest will always be written
	// immediately after the JSON object and a newline.
	Body io.Reader `json:"-"`

	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitempty"`
}

// progResponse is a JSON encoded response to the client.
//
// Copied from: https://pkg.go.dev/cmd/go/internal/cache#ProgResponse with
// minor edits.
type progResponse struct {
	ID int64 // that corresponds to ProgRequest; they can be answered out of order

	Err string `json:",omitempty"` // if non-empty, the error

	// KnownCommands is included in the first message that cache helper program
	// writes to stdout on startup (with ID==0). It includes the
	// ProgRequest.Command types that are supported by the program.
	//
	// This lets us extend the protocol gracefully over time (adding "get2",
	// etc), or fail gracefully when needed. It also lets us verify the program
	// wants to be a cache helper.
	KnownCommands []string `json:",omitempty"`

	// For Get requests.
	Miss     bool       `json:",omitempty"` // cache miss
	OutputID []byte     `json:",omitempty"`
	Size     int64      `json:",omitempty"` // in bytes
	Time     *time.Time `json:",omitempty"` // an Entry.Time; when the object was added to the docs

	// DiskPath is the absolute path on disk of the ObjectID corresponding
	// a "get" request's ActionID (on cache hit) or a "put" request's
	// provided ObjectID.
	DiskPath string `json:",omitempty"`
}
