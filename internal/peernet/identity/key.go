package identity

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

const (
	// KeyFileName is the name of the key file inside the identity directory.
	KeyFileName = "node.key"

	keyFileMode   fs.FileMode = 0o600
	keyDirMode    fs.FileMode = 0o700
	maxKeyFileLen             = 4096
	pemKeyType                = "PRIVATE KEY"
	redacted                  = "[redacted node key]"
)

var (
	// ErrInsecurePermissions is returned when the key file or its directory
	// is accessible to anyone but the owner.
	ErrInsecurePermissions = errors.New("identity: key file or directory permissions too wide")
	// ErrMalformedKey is returned for a key file that is not a valid key.
	ErrMalformedKey = errors.New("identity: malformed key file")
)

type keyMaterial struct {
	priv ed25519.PrivateKey
}

// Key is a node's Ed25519 identity. The private half is unreachable through
// any method, and every formatting path prints a fixed placeholder, so the
// key cannot leak through logs, JSON or %v. Copying a Key shares the secret.
type Key struct {
	m *keyMaterial
}

// GenerateKey creates a fresh in-memory key without touching disk.
func GenerateKey() (*Key, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("identity: generate key: %w", err)
	}
	return &Key{m: &keyMaterial{priv: priv}}, nil
}

// PublicKey returns a copy of the public key.
func (k *Key) PublicKey() ed25519.PublicKey {
	pub := k.m.priv.Public().(ed25519.PublicKey)
	return append(ed25519.PublicKey(nil), pub...)
}

// NodeID returns the Node ID of this key.
func (k *Key) NodeID() NodeID { return NodeIDFromPublicKey(k.m.priv.Public().(ed25519.PublicKey)) }

// String, GoString, MarshalJSON, MarshalText, MarshalBinary, Format and
// LogValue all print a placeholder. The private key is never serialised.
func (k *Key) String() string { return redacted }

// GoString implements fmt.GoStringer.
func (k *Key) GoString() string { return redacted }

// Format implements fmt.Formatter so no verb can reach the fields.
func (k *Key) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redacted) }

// MarshalJSON always fails: the key must never be serialised to JSON.
func (k *Key) MarshalJSON() ([]byte, error) {
	return nil, errors.New("identity: key is not serialisable")
}

// MarshalText always fails.
func (k *Key) MarshalText() ([]byte, error) {
	return nil, errors.New("identity: key is not serialisable")
}

// MarshalBinary always fails.
func (k *Key) MarshalBinary() ([]byte, error) {
	return nil, errors.New("identity: key is not serialisable")
}

// LogValue implements slog.LogValuer.
func (k *Key) LogValue() slog.Value { return slog.StringValue(redacted) }

// signer adapts the key to crypto.Signer without exposing the private key.
type signer struct{ k *Key }

func (s signer) Public() crypto.PublicKey { return s.k.m.priv.Public() }
func (s signer) Sign(r io.Reader, msg []byte, opts crypto.SignerOpts) ([]byte, error) {
	return s.k.m.priv.Sign(r, msg, opts)
}
func (s signer) String() string               { return redacted }
func (s signer) GoString() string             { return redacted }
func (s signer) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, redacted) }
func (s signer) MarshalJSON() ([]byte, error) { return nil, errors.New("identity: not serialisable") }
func (s signer) LogValue() slog.Value         { return slog.StringValue(redacted) }

// LoadOrCreate loads the key stored in dir, creating the directory (0700) and
// a new key file (0600) when none exists. Creation is atomic: the key is
// written to a temporary file and linked into place, so a concurrent creator
// can never be overwritten and a crash never leaves a partial key file.
func LoadOrCreate(dir string) (*Key, error) {
	k, err := Load(dir)
	if err == nil {
		return k, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(dir, keyDirMode); err != nil {
		return nil, fmt.Errorf("identity: create dir: %w", err)
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	nk, err := GenerateKey()
	if err != nil {
		return nil, err
	}
	if err := nk.writeAtomic(dir); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return Load(dir) // lost a creation race; use the winner's key
		}
		return nil, err
	}
	return nk, nil
}

func checkDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	st, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("identity: stat dir: %w", err)
	}
	if st.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("%w: key directory is a symbolic link", ErrInsecurePermissions)
	}
	if !st.IsDir() {
		return fmt.Errorf("identity: %s is not a directory", dir)
	}
	if !ownedByCurrentUser(st) {
		return fmt.Errorf("%w: key directory is not owned by the current user", ErrInsecurePermissions)
	}
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: directory mode %v", ErrInsecurePermissions, st.Mode().Perm())
	}
	return nil
}

func (k *Key) writeAtomic(dir string) error {
	der, err := x509.MarshalPKCS8PrivateKey(k.m.priv)
	if err != nil {
		return fmt.Errorf("identity: encode key: %w", err)
	}
	data := pem.EncodeToMemory(&pem.Block{Type: pemKeyType, Bytes: der})

	tmp, err := os.CreateTemp(dir, ".node.key.*")
	if err != nil {
		return fmt.Errorf("identity: temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(keyFileMode); err != nil && runtime.GOOS != "windows" {
		tmp.Close()
		return fmt.Errorf("identity: chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("identity: write key: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("identity: sync key: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("identity: close key: %w", err)
	}
	final := dir + string(os.PathSeparator) + KeyFileName
	// Link (fails if the target exists) rather than rename (silently replaces),
	// so an existing identity is never clobbered.
	if err := os.Link(tmpName, final); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return err
		}
		// Filesystems without hard links: rename after an existence check.
		if _, serr := os.Lstat(final); serr == nil {
			return fs.ErrExist
		}
		if rerr := os.Rename(tmpName, final); rerr != nil {
			return fmt.Errorf("identity: install key: %w", rerr)
		}
	}
	return nil
}

// Load reads the key stored in dir. It returns an error wrapping
// fs.ErrNotExist when there is no key file, ErrInsecurePermissions when the
// file or directory is accessible to others, and ErrMalformedKey for bad
// content.
func Load(dir string) (*Key, error) {
	path := dir + string(os.PathSeparator) + KeyFileName
	lst, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	if !lst.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: not a regular file", ErrMalformedKey)
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("identity: open key: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("identity: stat key: %w", err)
	}
	if !os.SameFile(lst, st) || !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: file changed while opening", ErrMalformedKey)
	}
	if runtime.GOOS != "windows" && st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: file mode %v", ErrInsecurePermissions, st.Mode().Perm())
	}
	data, err := io.ReadAll(io.LimitReader(f, maxKeyFileLen+1))
	if err != nil {
		return nil, fmt.Errorf("identity: read key: %w", err)
	}
	return parseKey(data)
}

func parseKey(data []byte) (*Key, error) {
	if len(data) > maxKeyFileLen {
		return nil, fmt.Errorf("%w: too large", ErrMalformedKey)
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != pemKeyType || len(block.Headers) != 0 ||
		strings.TrimSpace(string(rest)) != "" {
		return nil, fmt.Errorf("%w: bad PEM", ErrMalformedKey)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: bad PKCS#8", ErrMalformedKey)
	}
	priv, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: not an Ed25519 key", ErrMalformedKey)
	}
	// The encoded public half must match the seed.
	if !priv.Public().(ed25519.PublicKey).Equal(ed25519.NewKeyFromSeed(priv.Seed()).Public()) {
		return nil, fmt.Errorf("%w: inconsistent key", ErrMalformedKey)
	}
	return &Key{m: &keyMaterial{priv: priv}}, nil
}
