package webdav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

type deleteErrorFS struct {
	FileSystem
	err error
}

func (f deleteErrorFS) RemoveAll(context.Context, string) error { return f.err }

func TestDeletePermissionDenied(t *testing.T) {
	for _, err := range []error{os.ErrPermission, fmt.Errorf("guard: %w", os.ErrPermission)} {
		h := &webdavMethods{fs: deleteErrorFS{err: err}, prefix: "/webdav/"}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/webdav/complete/tv/release", nil))
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", w.Code)
		}
	}
}
