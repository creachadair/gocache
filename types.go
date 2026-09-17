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
	// It must be echoed in the progResponse from the child.
	ID int64

	// Command is the type of request.
	// The cmd/go tool will only send commands that were declared
	// as supported by the cache program.
	Command string

	// ActionID is non-empty for "get" and "put" requests.
	ActionID []byte `json:",omitempty"` // or nil if not used

	// OutputID is set for "put" requests.
	OutputID []byte `json:"OutputID,omitempty"`

	// OldOutputID is a workaround for the rename of "ObjectID" to "OutputID"
	// between Go 1.23 and Go 1.24. Do not use this field, it will be removed.
	//
	// TODO(creachadair): Remove after Go 1.24 is released with cacheprog as a
	// non-experimental feature.
	OldOutputID []byte `json:"ObjectID,omitempty"`

	// Body is the body for "put" requests.
	//
	// When BodySize is non-zero, the request object is immediately followed in
	// the input stream, after a terminating newline, by a base64-encoded JSON
	// string containing the body.
	Body io.Reader `json:"-"`

	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitzero"`
}

// outputID returns the output ID from r, preferring OutputID if it is present,
// and otherwise falling back to OldOutputID..
func (r *progRequest) outputID() []byte {
	if len(r.OutputID) != 0 {
		return r.OutputID
	}
	return r.OldOutputID
}

// progResponse is a JSON encoded response to the client.
//
// Copied from: https://pkg.go.dev/cmd/go/internal/cache#ProgResponse with
// minor edits.
type progResponse struct {
	// ID is the request ID for which this is the response.
	// Responses may be sent out of order.
	ID int64

	// Error, if non-empty, indicates that the request failed, and gives an
	// explanatory message. If this is set, the other fields below are not.
	Err string `json:",omitzero"`

	// KnownCommands lists the command names understood by the cache.
	//
	// This field is set in the first message that cache program writes on
	// startup (with ID==0). It includes the progRequest.Command types that are
	// supported by the program. The cmd/go tool will only send commands that
	// are declared in the initial greeting.
	KnownCommands []string `json:",omitempty"`

	// For Get requests.
	Miss     bool       `json:",omitzero"` // cache miss
	OutputID []byte     `json:",omitempty"`
	Size     int64      `json:",omitzero"`  // in bytes
	Time     *time.Time `json:",omitempty"` // an Entry.Time; when the object was added to the docs

	// DiskPath is the absolute path on disk of the ObjectID corresponding
	// a "get" request's ActionID (on cache hit) or a "put" request's
	// provided ObjectID.
	DiskPath string `json:",omitzero"`
}
