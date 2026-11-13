package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func newDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "id")
	return dir
}

func TestNodeIDParse(t *testing.T) {
	k, _ := GenerateKey()
	id := k.NodeID()
	got, err := ParseNodeID(id.String())
	if err != nil || !got.Equal(id) {
		t.Fatalf("round trip failed: %v", err)
	}
	for _, bad := range []string{"", "abc", strings.Repeat("g", 64), strings.ToUpper(id.String()),
		id.String() + "0", id.String()[:63], " " + id.String()[1:], "0x" + id.String()[2:]} {
		if _, err := ParseNodeID(bad); !errors.Is(err, ErrInvalidNodeID) {
			t.Errorf("ParseNodeID(%q) should fail, got %v", bad, err)
		}
	}
	var z NodeID
	if !z.IsZero() || id.IsZero() {
		t.Error("IsZero wrong")
	}
	b, _ := json.Marshal(id)
	var back NodeID
	if err := json.Unmarshal(b, &back); err != nil || back != id {
		t.Errorf("json round trip: %v", err)
	}
	if err := json.Unmarshal([]byte(`"zz"`), &back); err == nil {
		t.Error("expected unmarshal error")
	}
	if id.Equal(NodeID{}) {
		t.Error("distinct ids equal")
	}
}

func TestLoadOrCreateAtomicAndPermissions(t *testing.T) {
	dir := newDir(t)
	k1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := LoadOrCreate(dir)
	if err != nil || k1.NodeID() != k2.NodeID() {
		t.Fatalf("second load differs: %v", err)
	}
	if runtime.GOOS != "windows" {
		st, _ := os.Stat(dir)
		if st.Mode().Perm() != 0o700 {
			t.Errorf("dir mode %v", st.Mode().Perm())
		}
		fst, _ := os.Stat(filepath.Join(dir, KeyFileName))
		if fst.Mode().Perm() != 0o600 {
			t.Errorf("file mode %v", fst.Mode().Perm())
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("temp files left behind: %v", entries)
	}
}

func TestLoadRejectsWidePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits not meaningful")
	}
	dir := newDir(t)
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, KeyFileName)
	for _, mode := range []fs.FileMode{0o644, 0o640, 0o604, 0o660} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(dir); !errors.Is(err, ErrInsecurePermissions) {
			t.Errorf("mode %v: got %v", mode, err)
		}
	}
	_ = os.Chmod(path, 0o600)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); !errors.Is(err, ErrInsecurePermissions) {
		t.Errorf("wide dir: got %v", err)
	}
	if _, err := LoadOrCreate(dir); !errors.Is(err, ErrInsecurePermissions) {
		t.Errorf("wide dir LoadOrCreate: got %v", err)
	}
}

func TestLoadRejectsMalformed(t *testing.T) {
	good, _ := GenerateKey()
	der, _ := x509.MarshalPKCS8PrivateKey(good.m.priv)
	pemGood := pemEncode("PRIVATE KEY", der)
	cases := map[string][]byte{
		"empty":      {},
		"garbage":    []byte("not a key"),
		"wrong type": pemEncode("RSA PRIVATE KEY", der),
		"trailing":   append(append([]byte{}, pemGood...), []byte("junk")...),
		"truncated":  pemGood[:len(pemGood)/2],
		"huge":       bytes.Repeat([]byte("A"), maxKeyFileLen+10),
		"bad pkcs8":  pemEncode("PRIVATE KEY", []byte{1, 2, 3}),
		"two blocks": append(append([]byte{}, pemGood...), pemGood...),
	}
	for name, content := range cases {
		dir := newDir(t)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, KeyFileName), content, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(dir); !errors.Is(err, ErrMalformedKey) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestLoadRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	dir := newDir(t)
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other.key")
	_ = os.Rename(filepath.Join(dir, KeyFileName), other)
	if err := os.Symlink(other, filepath.Join(dir, KeyFileName)); err != nil {
		t.Skip(err)
	}
	if _, err := Load(dir); !errors.Is(err, ErrMalformedKey) {
		t.Errorf("symlink accepted: %v", err)
	}
}

func TestConcurrentCreateSingleIdentity(t *testing.T) {
	dir := newDir(t)
	var wg sync.WaitGroup
	ids := make([]NodeID, 8)
	errs := make([]error, 8)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			k, err := LoadOrCreate(dir)
			errs[i] = err
			if err == nil {
				ids[i] = k.NodeID()
			}
		}()
	}
	wg.Wait()
	for i := range ids {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("workers got different identities")
		}
	}
}

type holder struct {
	K  *Key
	KV Key
}

func TestKeyNeverFormatted(t *testing.T) {
	k, _ := GenerateKey()
	seedHex := fmt.Sprintf("%x", k.m.priv.Seed())
	privHex := fmt.Sprintf("%x", []byte(k.m.priv))
	var logBuf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&logBuf, nil))
	lg.Info("key", "k", k, "kv", *k, "h", holder{K: k, KV: *k})
	outs := []string{
		fmt.Sprint(k), fmt.Sprintf("%v %+v %#v %s %d %x", k, k, k, k, k, k),
		fmt.Sprintf("%+v %#v", *k, *k), fmt.Sprintf("%+v", holder{K: k, KV: *k}),
		fmt.Sprintf("%#v", holder{K: k, KV: *k}), logBuf.String(),
		fmt.Sprintf("%+v %v", signer{k: k}, signer{k: k}),
	}
	for _, o := range outs {
		for _, secret := range []string{seedHex, privHex} {
			if strings.Contains(o, secret) {
				t.Fatalf("secret leaked in %q", o)
			}
		}
		// Raw byte dumps of the key like [12 34 ...] must not appear either.
		rawDump := fmt.Sprintf("%d", []byte(k.m.priv)[:8])
		if strings.Contains(o, rawDump[1:len(rawDump)-1]) {
			t.Fatalf("raw key bytes leaked in %q", o)
		}
	}
	if _, err := json.Marshal(k); err == nil {
		t.Error("json.Marshal(key) must fail")
	}
	if _, err := json.Marshal(holder{K: k}); err == nil {
		t.Error("json.Marshal(holder) must fail")
	}
	if _, err := k.MarshalText(); err == nil {
		t.Error("MarshalText must fail")
	}
	if _, err := k.MarshalBinary(); err == nil {
		t.Error("MarshalBinary must fail")
	}
	if k.GoString() != redacted || k.String() != redacted {
		t.Error("placeholders")
	}
}

func TestSignDomainSeparation(t *testing.T) {
	k, _ := GenerateKey()
	msg := []byte("hello")
	sig, err := k.Sign("record", msg)
	if err != nil {
		t.Fatal(err)
	}
	pub := k.PublicKey()
	if err := Verify(pub, "record", msg, sig); err != nil {
		t.Fatal(err)
	}
	if err := Verify(pub, "attestation", msg, sig); !errors.Is(err, ErrBadSignature) {
		t.Errorf("cross-tag verify: %v", err)
	}
	// Boundary ambiguity: moving bytes between tag and message must not verify.
	sig2, _ := k.Sign("ab", []byte("c"))
	if err := Verify(pub, "a", []byte("bc"), sig2); err == nil {
		t.Error("tag/message boundary confusion")
	}
	if err := Verify(pub, "record", []byte("hellp"), sig); err == nil {
		t.Error("tampered message verified")
	}
	if err := Verify(pub, "record", msg, sig[:10]); err == nil {
		t.Error("short sig verified")
	}
	if err := Verify(pub[:5], "record", msg, sig); err == nil {
		t.Error("short key verified")
	}
	other, _ := GenerateKey()
	if err := Verify(other.PublicKey(), "record", msg, sig); err == nil {
		t.Error("wrong key verified")
	}
	if _, err := k.Sign("", msg); !errors.Is(err, ErrInvalidTag) {
		t.Error("empty tag accepted")
	}
	if _, err := k.Sign(strings.Repeat("x", 256), msg); !errors.Is(err, ErrInvalidTag) {
		t.Error("long tag accepted")
	}
	if err := Verify(pub, "", msg, sig); !errors.Is(err, ErrInvalidTag) {
		t.Error("empty tag verify accepted")
	}
	// A raw signature over the message without the prefix must not verify.
	raw := ed25519.Sign(k.m.priv, msg)
	if err := Verify(pub, "record", msg, raw); err == nil {
		t.Error("unprefixed signature verified")
	}
}

func TestCertificateRoundTrip(t *testing.T) {
	k, _ := GenerateKey()
	c, err := k.TLSCertificate()
	if err != nil {
		t.Fatal(err)
	}
	id, err := PeerNodeID(c.Certificate)
	if err != nil || id != k.NodeID() {
		t.Fatalf("PeerNodeID: %v", err)
	}
}

func TestCertificateValidity(t *testing.T) {
	k, _ := GenerateKey()
	now := time.Now()
	expired, _ := k.tlsCertificateBetween(now.Add(-48*time.Hour), now.Add(-time.Hour))
	future, _ := k.tlsCertificateBetween(now.Add(time.Hour), now.Add(48*time.Hour))
	for name, c := range map[string]*[][]byte{"expired": &expired.Certificate, "future": &future.Certificate} {
		if _, err := PeerNodeID(*c); !errors.Is(err, ErrInvalidCertificate) {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
}

func TestCertificateRejections(t *testing.T) {
	k, _ := GenerateKey()
	good, _ := k.TLSCertificate()
	der := good.Certificate[0]

	tampered := append([]byte(nil), der...)
	tampered[len(tampered)-1] ^= 0xff // corrupt signature
	tbs := append([]byte(nil), der...)
	tbs[len(tbs)/3] ^= 0x01

	other, _ := GenerateKey()
	otherC, _ := other.TLSCertificate()

	cases := map[string][][]byte{
		"nil":        nil,
		"empty":      {},
		"empty cert": {{}},
		"garbage":    {[]byte("garbage")},
		"two":        {der, otherC.Certificate[0]},
		"tampered":   {tampered},
		"tbs":        {tbs},
		"huge":       {bytes.Repeat([]byte{0x30}, maxCertDERBytes+1)},
	}
	for name, c := range cases {
		if _, err := PeerNodeID(c); !errors.Is(err, ErrInvalidCertificate) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestCertificateWrongSubjectAndAlgorithm(t *testing.T) {
	k, _ := GenerateKey()
	other, _ := GenerateKey()
	// Subject claims another node's ID.
	tmpl := &x509.Certificate{
		SerialNumber: bigOne(), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		Subject: pkixName(other.NodeID().String()), BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.PublicKey(), signer{k: k})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PeerNodeID([][]byte{der}); !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("spoofed subject accepted: %v", err)
	}
	// CA certificate.
	tmpl.Subject = pkixName(k.NodeID().String())
	tmpl.IsCA = true
	tmpl.KeyUsage = x509.KeyUsageCertSign
	der, _ = x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.PublicKey(), signer{k: k})
	if _, err := PeerNodeID([][]byte{der}); !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("CA accepted: %v", err)
	}
	// Signed by a different key than the one in the certificate.
	tmpl.IsCA = false
	tmpl.KeyUsage = 0
	der, _ = x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.PublicKey(), signer{k: other})
	if _, err := PeerNodeID([][]byte{der}); !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("foreign-signed accepted: %v", err)
	}
}

func FuzzParseNodeID(f *testing.F) {
	k, _ := GenerateKey()
	f.Add(k.NodeID().String())
	f.Add("")
	f.Add(strings.Repeat("F", 64))
	f.Fuzz(func(t *testing.T, s string) {
		id, err := ParseNodeID(s)
		if err == nil && id.String() != s {
			t.Fatalf("non-canonical accepted: %q", s)
		}
	})
}

func FuzzPeerNodeID(f *testing.F) {
	k, _ := GenerateKey()
	c, _ := k.TLSCertificate()
	f.Add(c.Certificate[0])
	f.Add([]byte{})
	f.Add([]byte{0x30, 0x82, 0x01})
	f.Fuzz(func(t *testing.T, der []byte) {
		id, err := PeerNodeID([][]byte{der})
		if err == nil && id.IsZero() {
			t.Fatal("zero id accepted")
		}
	})
}

func TestKeyDirSymlinkRefused(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on windows")
	}
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if _, err := LoadOrCreate(real); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link); !errors.Is(err, ErrInsecurePermissions) {
		t.Fatalf("Load through a symlinked dir: %v", err)
	}
	if _, err := LoadOrCreate(link); !errors.Is(err, ErrInsecurePermissions) {
		t.Fatalf("LoadOrCreate through a symlinked dir: %v", err)
	}
	// A dangling symlink must not be followed into creating keys elsewhere.
	dangling := filepath.Join(base, "dangling")
	if err := os.Symlink(filepath.Join(base, "elsewhere"), dangling); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dangling); err == nil {
		t.Fatal("created a key through a dangling symlink")
	}
	if _, err := os.Stat(filepath.Join(base, "elsewhere")); err == nil {
		t.Fatal("key directory created behind the symlink")
	}
}

func TestOwnedByCurrentUser(t *testing.T) {
	fi, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !ownedByCurrentUser(fi) {
		t.Fatal("a directory we just created must count as ours")
	}
}
