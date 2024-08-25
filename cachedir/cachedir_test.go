package cachedir_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creachadair/gocache"
	"github.com/creachadair/gocache/cachedir"
)

func TestDir(t *testing.T) {
	dir := t.TempDir()

	d, err := cachedir.New(dir)
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	ctx := context.Background()
	checkMiss := func(actionID string) {
		t.Helper()
		if obj, path, err := d.Get(ctx, actionID); obj != "" || path != "" || err != nil {
			t.Errorf(`Get(%q): got %q, %q, %v; want "", "", nil`, actionID, obj, path, err)
		}
	}

	// A cache miss reports empty paths and no error.
	checkMiss("nonesuch")

	// Create a directory in place of an action file.  The cache should fail to
	// read it as an action.
	if err := os.MkdirAll(filepath.Join(dir, "action", "bo", "bogus-action"), 0755); err != nil {
		t.Fatalf("Create bogus action: %v", err)
	}

	// Other errors report empty paths and the error.
	if obj, path, err := d.Get(ctx, "bogus-action"); obj != "" || path != "" || err == nil {
		t.Errorf(`Get(bogus-action): got %q, %q, nil; want "", "", <error>`, obj, path)
	}

	// Put a real object successfully.
	testTime := time.Date(2024, 8, 25, 12, 46, 50, 0, time.Local)
	diskPath, err := d.Put(ctx, gocache.Object{
		ActionID: "good-action",
		ObjectID: "some-object",
		Size:     5,
		Body:     strings.NewReader("xyzzy"),
		ModTime:  testTime,
	})
	if err != nil {
		t.Errorf("Put(good-action): unexpected error: %v", err)
	}

	// Verify that the object exists and looks compos.
	fi, err := os.Stat(diskPath)
	if err != nil {
		t.Errorf("Check object: %v", err)
	} else if fi.Size() != 5 {
		t.Errorf("Object size is %d, want 5", fi.Size())
	} else if !fi.ModTime().Equal(testTime) {
		t.Errorf("Object modtime is %v, want %v", fi.ModTime(), testTime)
	}

	// Remove the object file and verify that the action reports a miss.
	if err := os.Remove(diskPath); err != nil {
		t.Fatalf("Remove object file: %v", err)
	}

	checkMiss("good-action")
}
