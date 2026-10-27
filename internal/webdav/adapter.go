package webdav

import (
	"context"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-pkgz/auth/v2/token"
	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/kipsilabs/altmount/internal/webdav/propfind"
)

// Handler provides WebDAV functionality as an HTTP handler
type Handler struct {
	handler      http.Handler
	authCreds    *AuthCredentials
	configGetter config.ConfigGetter
}

// propfindFS adapts FileSystem to propfind.FS.
// This is needed because propfind.FS.OpenFile returns propfind.FSFile, while
// FileSystem.OpenFile returns File. Since File is a superset of propfind.FSFile,
// the adapter simply forwards the call.
type propfindFS struct {
	fs FileSystem
}

func (p propfindFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	return p.fs.Stat(ctx, name)
}

func (p propfindFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (propfind.FSFile, error) {
	return p.fs.OpenFile(ctx, name, flag, perm)
}

// webdavMethods handles the individual WebDAV HTTP methods.
type webdavMethods struct {
	fs     FileSystem
	prefix string
}

// headerTracker wraps http.ResponseWriter to track whether headers have been committed.
type headerTracker struct {
	http.ResponseWriter
	written bool
}

func (h *headerTracker) WriteHeader(status int) {
	h.written = true
	h.ResponseWriter.WriteHeader(status)
}

func (h *headerTracker) Write(b []byte) (int, error) {
	h.written = true
	return h.ResponseWriter.Write(b)
}

func (h *webdavMethods) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "OPTIONS":
		h.handleOptions(w, r)
	case http.MethodHead, http.MethodGet:
		h.handleGet(w, r)
	case "PROPFIND":
		tracker := &headerTracker{ResponseWriter: w}
		status, err := propfind.HandlePropfind(propfindFS{h.fs}, tracker, r, h.prefix)
		if status != 0 {
			if tracker.written {
				// Headers already committed (207 sent); log the underlying error.
				slog.ErrorContext(r.Context(), "PROPFIND error after headers sent", "status", status, "err", err)
				return
			}
			w.WriteHeader(status)
			if status != http.StatusNoContent {
				_, _ = w.Write([]byte(http.StatusText(status)))
			}
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "Error handling PROPFIND", "err", err)
		}
	case "DELETE":
		h.handleDelete(w, r)
	case "MOVE":
		h.handleMove(w, r)
	case "MKCOL":
		h.handleMkcol(w, r)
	case "COPY":
		// NzbFilesystem explicitly forbids COPY (IsCopy context flag)
		http.Error(w, "Forbidden", http.StatusForbidden)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (h *webdavMethods) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("DAV", "1")
	w.Header().Set("Allow", "OPTIONS, HEAD, GET, PROPFIND, DELETE, MOVE, MKCOL")
	w.WriteHeader(http.StatusOK)
}

func (h *webdavMethods) handleGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reqPath, status, err := propfind.StripPrefix(r.URL.Path, h.prefix)
	if err != nil {
		http.Error(w, "Not Found", status)
		return
	}

	slog.DebugContext(ctx, "WebDAV GET", "path", reqPath)
	fi, err := h.fs.Stat(ctx, reqPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	slog.DebugContext(ctx, "WebDAV GET stat", "path", reqPath, "is_dir", fi.IsDir(), "size", fi.Size())
	if fi.IsDir() {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	f, err := h.fs.OpenFile(ctx, reqPath, os.O_RDONLY, 0)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			http.Error(w, httpErr.Message, httpErr.StatusCode)
		} else if os.IsNotExist(err) {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}
	defer f.Close()

	tracked := &readTracker{File: f}
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), tracked)
	// A client that hangs up cancels the request context and the reader
	// surfaces that cancellation; only a body cut short while the client was
	// still listening is a server-side failure worth a warning.
	if tracked.err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "WebDAV stream ended on read error",
			"path", reqPath,
			"range", r.Header.Get("Range"),
			"bytes_served", tracked.bytesRead,
			"error", tracked.err)
	}
}

func (h *webdavMethods) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reqPath, status, err := propfind.StripPrefix(r.URL.Path, h.prefix)
	if err != nil {
		http.Error(w, "Not Found", status)
		return
	}

	slog.DebugContext(ctx, "WebDAV DELETE", "path", reqPath)
	if err := h.fs.RemoveAll(ctx, reqPath); err != nil {
		if errors.Is(err, os.ErrPermission) {
			http.Error(w, "Forbidden", http.StatusForbidden)
		} else if os.IsNotExist(err) {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *webdavMethods) handleMove(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	src, status, err := propfind.StripPrefix(r.URL.Path, h.prefix)
	if err != nil {
		http.Error(w, "Not Found", status)
		return
	}

	destHeader := r.Header.Get("Destination")
	if destHeader == "" {
		http.Error(w, "Bad Request: missing Destination header", http.StatusBadRequest)
		return
	}

	destURL, err := url.Parse(destHeader)
	if err != nil {
		http.Error(w, "Bad Request: invalid Destination header", http.StatusBadRequest)
		return
	}

	dst, _, err := propfind.StripPrefix(destURL.Path, h.prefix)
	if err != nil {
		// Destination is outside our root — treat as conflict
		http.Error(w, "Conflict: destination outside WebDAV root", http.StatusConflict)
		return
	}

	// Check if destination already exists to determine response code
	_, statErr := h.fs.Stat(ctx, dst)
	slog.DebugContext(ctx, "WebDAV MOVE parsed", "src", src, "dst", dst, "dest_exists", !os.IsNotExist(statErr))

	if err := h.fs.Rename(ctx, src, dst); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	if os.IsNotExist(statErr) {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *webdavMethods) handleMkcol(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reqPath, status, err := propfind.StripPrefix(r.URL.Path, h.prefix)
	if err != nil {
		http.Error(w, "Not Found", status)
		return
	}

	slog.DebugContext(ctx, "WebDAV MKCOL", "path", reqPath)
	if r.ContentLength > 0 {
		// RFC 4918: MKCOL request with a body must return 415 Unsupported Media Type
		http.Error(w, "Unsupported Media Type", http.StatusUnsupportedMediaType)
		return
	}

	if err := h.fs.Mkdir(ctx, reqPath, 0755); err != nil {
		if os.IsExist(err) {
			http.Error(w, "Method Not Allowed: collection already exists", http.StatusMethodNotAllowed)
		} else if os.IsNotExist(err) {
			http.Error(w, "Conflict: parent collection does not exist", http.StatusConflict)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// NewHandler creates a new WebDAV handler that can be used with Fiber adaptor
func NewHandler(
	config *Config,
	fs *nzbfilesystem.NzbFilesystem,
	tokenService *token.Service, // Optional token service for JWT auth
	userRepo *database.UserRepository, // Optional user repository for JWT auth
	configGetter config.ConfigGetter, // Dynamic config access
	streamTracker *api.StreamTracker, // Optional stream tracker
) (*Handler, error) {
	slog.DebugContext(context.Background(), "Creating WebDAV handler",
		"prefix", config.Prefix,
		"stream_tracking", streamTracker != nil)

	// Create dynamic auth credentials with initial values
	authCreds := NewAuthCredentials(config.User, config.Pass)

	// Create custom error handler that maps our errors to proper HTTP status codes
	webdavFS := nzbToWebdavFS(fs)
	errorHandler := &customErrorHandler{
		FileSystem: webdavFS,
	}

	var finalFS FileSystem = errorHandler
	if streamTracker != nil {
		finalFS = &monitoredFileSystem{fs: errorHandler}
	}

	methods := &webdavMethods{
		fs:     finalFS,
		prefix: config.Prefix,
	}

	// Create the main handler with authentication
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginRequired := true
		if configGetter != nil {
			if cfg := configGetter(); cfg != nil && cfg.Auth.LoginRequired != nil {
				loginRequired = *cfg.Auth.LoginRequired
			}
		}

		// Fallback to basic authentication if JWT failed
		username, password, hasBasicAuth := r.BasicAuth()

		var authenticated bool
		var effectiveUser string

		if !hasBasicAuth {
			// Try JWT token authentication first (if services are available)
			if tokenService != nil && userRepo != nil {
				claims, _, err := tokenService.Get(r)
				if err == nil && claims.User != nil {
					// Valid token found, check user exists in database
					userID := claims.User.ID
					if userID == "" {
						userID = claims.Subject
					}

					if userID != "" {
						user, err := userRepo.GetUserByID(r.Context(), userID)
						if err == nil && user != nil {
							authenticated = true
							if user.Name != nil && *user.Name != "" {
								effectiveUser = *user.Name
							} else {
								effectiveUser = user.UserID
							}
							slog.DebugContext(r.Context(), "WebDAV JWT auth succeeded", "user", effectiveUser)
						}
					}
				}
			}
		} else {
			// Check against dynamic credentials
			currentUser, currentPass := authCreds.GetCredentials()
			if username == currentUser && password == currentPass {
				authenticated = true
				effectiveUser = username
				slog.DebugContext(r.Context(), "WebDAV basic auth succeeded", "user", effectiveUser)
			}
		}

		// An explicit Basic credential is always verified: mounts configured with
		// webdav.user/password must fail loudly rather than fall back to anonymous.
		if !authenticated && (loginRequired || hasBasicAuth) {
			slog.DebugContext(r.Context(), "WebDAV auth failed", "method", r.Method, "path", r.URL.Path, "has_basic", hasBasicAuth)
			w.Header().Set("WWW-Authenticate", `Basic realm="BASIC WebDAV REALM"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, err := w.Write([]byte("401 Unauthorized"))
			if err != nil {
				slog.ErrorContext(r.Context(), "Error writing the response to the client", "err", err)
			}
			return
		}

		if effectiveUser == "" {
			effectiveUser = "WebDAV"
		}

		// Pre-set Content-Type to prevent http.ServeContent from sniffing (which
		// reads 512 bytes then seeks back to 0 — not supported by usenet reader).
		ext := filepath.Ext(r.URL.Path)
		if ext != "" {
			mimeType := mime.TypeByExtension(ext)
			if mimeType != "" {
				w.Header().Set("Content-Type", mimeType)
			} else {
				w.Header().Set("Content-Type", "application/octet-stream")
			}
		}

		w.Header().Set("Accept-Ranges", "bytes")
		ctx := r.Context()
		ctx = context.WithValue(ctx, utils.ContentLengthKey, r.Header.Get("Content-Length"))
		ctx = context.WithValue(ctx, utils.RangeKey, r.Header.Get("Range"))
		ctx = context.WithValue(ctx, utils.IsCopy, r.Method == "COPY")
		ctx = context.WithValue(ctx, utils.Origin, r.RequestURI)
		ctx = context.WithValue(ctx, utils.ShowCorrupted, r.Header.Get("X-Show-Corrupted") == "true")
		ctx = context.WithValue(ctx, utils.ClientIPKey, r.RemoteAddr)
		ctx = context.WithValue(ctx, utils.UserAgentKey, r.UserAgent())
		r = r.WithContext(ctx)

		// Log MOVE operations to understand client behavior
		if r.Method == "MOVE" {
			slog.InfoContext(r.Context(), "WebDAV MOVE operation",
				"source", r.RequestURI,
				"destination", r.Header.Get("Destination"),
				"overwrite", r.Header.Get("Overwrite"),
				"user_agent", r.Header.Get("User-Agent"))
		}

		// Track active streams for GET requests
		if r.Method == http.MethodGet && streamTracker != nil {
			streamCtx, cancel := context.WithCancel(r.Context())
			defer cancel()

			stream := streamTracker.Add(r.URL.Path, "WebDAV", effectiveUser, r.RemoteAddr, r.UserAgent(), 0)
			slog.DebugContext(r.Context(), "WebDAV stream registered", "path", r.URL.Path, "stream_id", stream)
			defer streamTracker.Remove(stream)

			streamTracker.SetCancelFunc(stream, cancel)

			streamCtx = context.WithValue(streamCtx, utils.StreamIDKey, stream)

			if sObj := streamTracker.GetStream(stream); sObj != nil {
				r = r.WithContext(context.WithValue(streamCtx, utils.ActiveStreamKey, sObj))
			} else {
				r = r.WithContext(streamCtx)
			}
		}

		methods.ServeHTTP(w, r)
	})

	// Create a mux to handle the WebDAV routing
	mux := http.NewServeMux()

	// Default to root if not set
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = "/"
	}

	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	// Normalize: "/webdav"
	base := strings.TrimRight(prefix, "/")
	if base == "" {
		base = "/"
	}

	if base == "/" {
		// Mount at root
		mux.Handle("/", h)
	} else {
		// Redirect /webdav -> /webdav/
		mux.Handle(base, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, base+"/", http.StatusMovedPermanently)
		}))
		// Mount handler at /webdav/
		mux.Handle(base+"/", h)
	}

	return &Handler{
		handler:      mux,
		authCreds:    authCreds,
		configGetter: configGetter,
	}, nil
}

// GetHTTPHandler returns the HTTP handler for use with Fiber adaptor
func (h *Handler) GetHTTPHandler() http.Handler {
	return h.handler
}

// SyncAuthCredentials updates auth credentials from current config
func (h *Handler) SyncAuthCredentials() {
	if h.configGetter != nil {
		currentConfig := h.configGetter()
		h.authCreds.UpdateCredentials(currentConfig.WebDAV.User, currentConfig.WebDAV.Password)

		slog.DebugContext(context.Background(), "WebDAV configuration synced from config")
	}
}
