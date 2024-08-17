// Package cachedir implements callbacks for [gocache.Server] that store
// cache data in a local filesystem directory.
//
// # Cache Layout
//
// Actions are cached in a subdirectory named "action", and objects are cached
// in a subdirectory named "object". Within each directory, IDs are partitioned
// by a prefix of their hex representation, e.g. "01234567" is stored as:
//
//	01/01234567
//
// Each action file contains a single line of text giving the current object ID
// for that action, and the size of the object in bytes, separated by a space:
//
//	0123abcd 25
//
// The modification timestamp of the action file is updated whenever the action
// is written, i.e., when a new object ID is sent for that action.
//
// Object files contain only the literal contents of the object.
package cachedir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/gocache"
	"github.com/creachadair/mds/mapset"
)

// Dir implements a file cache using a local directory.
type Dir struct {
	path string
}

// New constructs a new file cache using the specified directory.  If path does
// not exist, it is created.
func New(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	return &Dir{path: path}, nil
}

// Get implements the corresponding method of the gocache service interface.
func (d *Dir) Get(ctx context.Context, actionID string) (objectID, diskPath string, _ error) {
	objectID, _, err := d.readAction(actionID)
	if errors.Is(err, os.ErrNotExist) {
		return "", "", nil // cache miss
	} else if err != nil {
		return "", "", err
	}
	return objectID, d.objectPath(objectID), nil
}

// Put implements the corresponding method of the gocache service interface.
func (d *Dir) Put(ctx context.Context, obj gocache.Object) (diskPath string, _ error) {
	path, size, err := d.writeObject(obj.ObjectID, obj.Size, obj.Body)
	if err != nil {
		return "", err
	}
	return path, d.writeAction(obj.ActionID, obj.ObjectID, size)
}

// Cleanup returns a function implementing the Close method of the gocache
// service interface.  The function prunes from the cache any actions that have
// not been modified within the specified age before present.
// If age ≤ 0, Cleanup returns nil.
func (d *Dir) Cleanup(age time.Duration) func(context.Context) error {
	if age <= 0 {
		return nil
	}
	return func(ctx context.Context) error {
		gocache.Logf(ctx, "begin cache cleanup (%v)", age)
		stats, err := d.PruneEntries(ctx, age)
		if err != nil {
			return err
		}
		gocache.Logf(ctx, "cache cleanup done: %+v", stats)
		return nil
	}
}

// Stats report statistics about the contents of a Dir after pruning.
type Stats struct {
	Actions       int           // the number of actions cached
	ActionsPruned int           // the number of actions pruned
	Objects       int           // the number of objects cached
	ObjectsPruned int           // the number of objects pruned
	BytesPruned   int64         // the nuber of object bytes pruned
	Elapsed       time.Duration // how long pruning took
}

// PruneEntries prunes the contents of the cache to remove actions that have
// not been modified in longer than the specified age, along with any objects
// that are not referenced by any action after pruning is complete.
func (d *Dir) PruneEntries(ctx context.Context, age time.Duration) (s Stats, _ error) {
	start := time.Now()
	defer func() { s.Elapsed = time.Since(start) }()

	// Keep track of the objects that are being retained.
	var keepObject mapset.Set[string] // objects referenced by kept actions

	// Mark: Delete expired actions and collect object IDs.
	root := filepath.Join(d.path, "action")
	if err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		} else if !de.Type().IsRegular() {
			return nil // skip directories and other stuff
		}
		id := d.idFromPath("action", path)
		if id == "" {
			return nil // not ours
		}

		objID, _, err := d.readActionFile(id, path)
		if err != nil {
			return err
		}
		s.Actions++

		fi, _ := de.Info()
		if start.Sub(fi.ModTime()) > age {
			s.ActionsPruned++
			gocache.Logf(ctx, "expire action %v", id)
			return os.Remove(path)
		}
		keepObject.Add(objID)
		return nil
	}); err != nil {
		return s, err
	}

	// Sweep: Delete objects not referenced by unexpired actions.
	root = filepath.Join(d.path, "object")
	if err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		} else if !de.Type().IsRegular() {
			return nil // skip directories and other stuff
		}
		s.Objects++

		if id := d.idFromPath("object", path); id != "" && !keepObject.Has(id) {
			s.ObjectsPruned++
			fi, _ := de.Info()
			s.BytesPruned += fi.Size()
			gocache.Logf(ctx, "remove object %v (%d bytes)", id, fi.Size())
			if err := os.Remove(path); err != nil {
				gocache.Logf(ctx, "remove object %v: %v (ignored)", id, err)
			}
		}
		return nil
	}); err != nil {
		return s, err
	}
	return s, nil
}

func (d *Dir) idFromPath(kind, path string) string {
	// Expected path format: <dir>/<kind>/<xx>/<id>
	tail, _ := filepath.Rel(d.path, path)         // remove <dir>/
	tail, ok := strings.CutPrefix(tail, kind+"/") // remove <kind>/
	if !ok {
		return ""
	}
	return filepath.Base(tail)
}

func (d *Dir) actionPath(id string) string {
	return filepath.Join(d.path, "action", id[:2], id)
}

func (d *Dir) objectPath(id string) string {
	return filepath.Join(d.path, "object", id[:2], id)
}

func (d *Dir) readAction(id string) (objectID string, size int64, _ error) {
	return d.readActionFile(id, d.actionPath(id))
}

func (d *Dir) readActionFile(id, path string) (objectID string, size int64, _ error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	fs := strings.Fields(string(data))
	if len(fs) != 2 {
		return "", 0, fmt.Errorf("invalid action file for %s", id)
	}
	size, err = strconv.ParseInt(fs[1], 10, 64)
	return fs[0], size, err
}

func (d *Dir) writeAction(id, objectID string, size int64) error {
	path, err := makePath(id, d.actionPath)
	if err != nil {
		return err
	}
	return atomicfile.Tx(path, 0644, func(f *atomicfile.File) error {
		_, err := fmt.Fprintf(f, "%s %d\n", objectID, size)
		return err
	})
}

func (d *Dir) writeObject(id string, expSize int64, data io.Reader) (string, int64, error) {
	path, err := makePath(id, d.objectPath)
	if err != nil {
		return "", 0, err
	}

	// If the specified object is already present and has the expected size,
	// skip writing the object.
	fi, err := os.Stat(path)
	if err == nil && fi.Mode().IsRegular() && fi.Size() == expSize {
		return path, fi.Size(), nil
	}

	sz, err := atomicfile.WriteAll(path, data, 0600)
	return path, sz, err
}

func makePath(id string, f func(string) string) (string, error) {
	path := f(id)
	return path, os.MkdirAll(filepath.Dir(path), 0700)
}
