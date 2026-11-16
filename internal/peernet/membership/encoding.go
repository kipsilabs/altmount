package membership

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// Format constants.
const (
	CertVersion       = 1
	RevocationVersion = 1
	messageVersion    = 1

	CertLifetime  = 48 * time.Hour
	ClockSkew     = 2 * time.Minute
	RenewGrace    = 2 * time.Hour
	MaxRevoked    = 2048
	InviteCodeLen = 16
	// InviteCodeTextLen is the length of the base32 text form of a code.
	InviteCodeTextLen = 26
	// MaxNameLen is the cap on a display name carried in responses (bytes).
	MaxNameLen = 64

	certBodySize = 1 + 32 + 32 + 1 + 8 + 8
	// CertSize is the exact size of an encoded certificate.
	CertSize = certBodySize + 1 + ed25519.SignatureSize
	// MaxRevocationListSize bounds an encoded revocation list.
	MaxRevocationListSize = 1 + 32 + 8 + 8 + 2 + 32*MaxRevoked + 1 + ed25519.SignatureSize

	redeemRequestSize = 1 + 32 + InviteCodeLen
	renewRequestSize  = 1 + 32
	// MaxResponseSize bounds a join/renew response.
	MaxResponseSize = 1 + 1 + MaxNameLen + 2 + CertSize + 4 + MaxRevocationListSize

	certTag       = "altmount-peernet-membership-cert-v1"
	revocationTag = "altmount-peernet-membership-revocation-v1"
	inviteDomain  = "altmount-peernet-invite-v1\x00"
)

// Role is a node's role in a private network.
type Role uint8

// Roles.
const (
	RoleHost   Role = 1
	RoleMember Role = 2
)

func (r Role) String() string {
	switch r {
	case RoleHost:
		return "host"
	case RoleMember:
		return "member"
	}
	return "unknown"
}

// Errors. Verification failures are distinct so tests and callers can tell
// them apart, but none carries peer-supplied data.
var (
	ErrMalformed       = errors.New("membership: malformed encoding")
	ErrBadSignature    = errors.New("membership: bad signature")
	ErrWrongNetwork    = errors.New("membership: wrong network")
	ErrExpired         = errors.New("membership: certificate expired")
	ErrNotYetValid     = errors.New("membership: certificate not yet valid")
	ErrPeerMismatch    = errors.New("membership: certificate does not belong to the peer")
	ErrRevoked         = errors.New("membership: node revoked")
	ErrStaleRevocation = errors.New("membership: revocation list sequence went backwards")
	ErrBadLifetime     = errors.New("membership: invalid certificate lifetime")
)

// Certificate is a decoded membership certificate.
type Certificate struct {
	NetworkID netid.NetworkID
	NodeID    identity.NodeID
	Role      Role
	IssuedAt  int64
	ExpiresAt int64
	Signature []byte
}

func (c *Certificate) body() []byte {
	b := make([]byte, 0, certBodySize)
	b = append(b, CertVersion)
	b = append(b, c.NetworkID[:]...)
	b = append(b, c.NodeID[:]...)
	b = append(b, byte(c.Role))
	b = binary.BigEndian.AppendUint64(b, uint64(c.IssuedAt))
	b = binary.BigEndian.AppendUint64(b, uint64(c.ExpiresAt))
	return b
}

// Marshal returns the canonical encoding.
func (c *Certificate) Marshal() []byte {
	b := c.body()
	b = append(b, byte(len(c.Signature)))
	return append(b, c.Signature...)
}

// IssueCertificate signs a certificate with the network root key.
func IssueCertificate(root *identity.Key, network netid.NetworkID, node identity.NodeID, role Role, now time.Time, lifetime time.Duration) (*Certificate, error) {
	if role != RoleHost && role != RoleMember {
		return nil, fmt.Errorf("%w: role", ErrMalformed)
	}
	c := &Certificate{NetworkID: network, NodeID: node, Role: role,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(lifetime).Unix()}
	sig, err := root.Sign(certTag, c.body())
	if err != nil {
		return nil, err
	}
	c.Signature = sig
	return c, nil
}

// ParseCertificate strictly decodes a certificate. It does not verify it.
func ParseCertificate(b []byte) (*Certificate, error) {
	if len(b) != CertSize || b[0] != CertVersion {
		return nil, ErrMalformed
	}
	c := &Certificate{}
	copy(c.NetworkID[:], b[1:33])
	copy(c.NodeID[:], b[33:65])
	c.Role = Role(b[65])
	if c.Role != RoleHost && c.Role != RoleMember {
		return nil, ErrMalformed
	}
	c.IssuedAt = int64(binary.BigEndian.Uint64(b[66:74]))
	c.ExpiresAt = int64(binary.BigEndian.Uint64(b[74:82]))
	if c.IssuedAt < 0 || c.ExpiresAt < 0 || b[certBodySize] != ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	c.Signature = append([]byte(nil), b[certBodySize+1:]...)
	return c, nil
}

// Verify checks the certificate against the network root key at time now.
// peer is the authenticated transport peer; pass the zero NodeID only when
// the certificate is the caller's own (use VerifyFor for that).
func (c *Certificate) Verify(root ed25519.PublicKey, peer identity.NodeID, now time.Time) error {
	if peer.IsZero() || !c.NodeID.Equal(peer) {
		return ErrPeerMismatch
	}
	return c.verifyWindow(root, now)
}

func (c *Certificate) verifyWindow(root ed25519.PublicKey, now time.Time) error {
	if len(root) != ed25519.PublicKeySize || !netid.FromRootKey(root).Equal(c.NetworkID) {
		return ErrWrongNetwork
	}
	if err := identity.Verify(root, certTag, c.body(), c.Signature); err != nil {
		return ErrBadSignature
	}
	if c.ExpiresAt <= c.IssuedAt || c.ExpiresAt-c.IssuedAt > int64((CertLifetime+ClockSkew)/time.Second) {
		return ErrBadLifetime
	}
	skew := int64(ClockSkew / time.Second)
	n := now.Unix()
	if n+skew < c.IssuedAt {
		return ErrNotYetValid
	}
	if n-skew > c.ExpiresAt {
		return ErrExpired
	}
	return nil
}

// RevocationList is a decoded, signed list of revoked nodes.
type RevocationList struct {
	NetworkID netid.NetworkID
	Sequence  uint64
	IssuedAt  int64
	Revoked   []identity.NodeID // strictly ascending
	Signature []byte
}

func (l *RevocationList) body() []byte {
	b := make([]byte, 0, 1+32+8+8+2+32*len(l.Revoked))
	b = append(b, RevocationVersion)
	b = append(b, l.NetworkID[:]...)
	b = binary.BigEndian.AppendUint64(b, l.Sequence)
	b = binary.BigEndian.AppendUint64(b, uint64(l.IssuedAt))
	b = binary.BigEndian.AppendUint16(b, uint16(len(l.Revoked)))
	for _, id := range l.Revoked {
		b = append(b, id[:]...)
	}
	return b
}

// Marshal returns the canonical encoding.
func (l *RevocationList) Marshal() []byte {
	b := l.body()
	b = append(b, byte(len(l.Signature)))
	return append(b, l.Signature...)
}

// Contains reports whether id is revoked.
func (l *RevocationList) Contains(id identity.NodeID) bool {
	for _, r := range l.Revoked {
		if r.Equal(id) {
			return true
		}
	}
	return false
}

// IssueRevocationList signs a list. ids are sorted and de-duplicated.
func IssueRevocationList(root *identity.Key, network netid.NetworkID, seq uint64, now time.Time, ids []identity.NodeID) (*RevocationList, error) {
	sorted := append([]identity.NodeID(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) < 0 })
	uniq := sorted[:0]
	for i, id := range sorted {
		if i == 0 || id != sorted[i-1] {
			uniq = append(uniq, id)
		}
	}
	if len(uniq) > MaxRevoked {
		return nil, fmt.Errorf("%w: too many revoked nodes", ErrMalformed)
	}
	l := &RevocationList{NetworkID: network, Sequence: seq, IssuedAt: now.Unix(), Revoked: uniq}
	sig, err := root.Sign(revocationTag, l.body())
	if err != nil {
		return nil, err
	}
	l.Signature = sig
	return l, nil
}

// ParseRevocationList strictly decodes a list. It does not verify it.
func ParseRevocationList(b []byte) (*RevocationList, error) {
	const head = 1 + 32 + 8 + 8 + 2
	if len(b) < head+1+ed25519.SignatureSize || len(b) > MaxRevocationListSize || b[0] != RevocationVersion {
		return nil, ErrMalformed
	}
	count := int(binary.BigEndian.Uint16(b[49:51]))
	if count > MaxRevoked || len(b) != head+32*count+1+ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	l := &RevocationList{}
	copy(l.NetworkID[:], b[1:33])
	l.Sequence = binary.BigEndian.Uint64(b[33:41])
	l.IssuedAt = int64(binary.BigEndian.Uint64(b[41:49]))
	if l.IssuedAt < 0 {
		return nil, ErrMalformed
	}
	l.Revoked = make([]identity.NodeID, count)
	for i := range l.Revoked {
		copy(l.Revoked[i][:], b[head+32*i:head+32*(i+1)])
		if i > 0 && bytes.Compare(l.Revoked[i-1][:], l.Revoked[i][:]) >= 0 {
			return nil, ErrMalformed
		}
	}
	off := head + 32*count
	if b[off] != ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	l.Signature = append([]byte(nil), b[off+1:]...)
	return l, nil
}

// Verify checks network binding, signature and that it is not issued in the
// future beyond the clock-skew allowance.
func (l *RevocationList) Verify(root ed25519.PublicKey, now time.Time) error {
	if len(root) != ed25519.PublicKeySize || !netid.FromRootKey(root).Equal(l.NetworkID) {
		return ErrWrongNetwork
	}
	if err := identity.Verify(root, revocationTag, l.body(), l.Signature); err != nil {
		return ErrBadSignature
	}
	if l.IssuedAt > now.Add(ClockSkew).Unix() {
		return ErrNotYetValid
	}
	return nil
}

// AcceptableOver reports whether l may replace a held list with sequence
// heldSeq: the sequence must never go backwards.
func (l *RevocationList) AcceptableOver(heldSeq uint64) error {
	if l.Sequence < heldSeq {
		return ErrStaleRevocation
	}
	return nil
}

// --- invite codes ---

var codeEnc = base32.StdEncoding.WithPadding(base32.NoPadding)

// InviteCode is 128 bits of randomness.
type InviteCode [InviteCodeLen]byte

// NewInviteCode draws a code from crypto/rand.
func NewInviteCode() (InviteCode, error) {
	var c InviteCode
	if _, err := rand.Read(c[:]); err != nil {
		return c, fmt.Errorf("membership: random: %w", err)
	}
	return c, nil
}

// Text returns the 26-character lower-case form. Treat it as a secret.
func (c InviteCode) Text() string { return strings.ToLower(codeEnc.EncodeToString(c[:])) }

// String and GoString never reveal the code.
func (c InviteCode) String() string   { return "invite-code(redacted)" }
func (c InviteCode) GoString() string { return c.String() }

// ParseInviteCode strictly parses the text form (canonical, lower case).
func ParseInviteCode(s string) (InviteCode, error) {
	var c InviteCode
	if len(s) != InviteCodeTextLen {
		return c, ErrMalformed
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch < 'a' || ch > 'z') && (ch < '2' || ch > '7') {
			return c, ErrMalformed
		}
	}
	raw, err := codeEnc.DecodeString(strings.ToUpper(s))
	if err != nil || len(raw) != InviteCodeLen {
		return c, ErrMalformed
	}
	copy(c[:], raw)
	if c.Text() != s { // rejects non-zero trailing bits
		return InviteCode{}, ErrMalformed
	}
	return c, nil
}

// Hash returns the hex hash stored at rest.
func (c InviteCode) Hash() string {
	h := sha256.New()
	h.Write([]byte(inviteDomain))
	h.Write(c[:])
	return hex.EncodeToString(h.Sum(nil))
}

// hashEqual compares two hex hashes without leaking timing.
func hashEqual(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// --- request / response messages ---

type redeemRequest struct {
	Network netid.NetworkID
	Code    InviteCode
}

func (r redeemRequest) marshal() []byte {
	b := append([]byte{messageVersion}, r.Network[:]...)
	return append(b, r.Code[:]...)
}

func parseRedeemRequest(b []byte) (redeemRequest, error) {
	var r redeemRequest
	if len(b) != redeemRequestSize || b[0] != messageVersion {
		return r, ErrMalformed
	}
	copy(r.Network[:], b[1:33])
	copy(r.Code[:], b[33:])
	return r, nil
}

func marshalRenewRequest(n netid.NetworkID) []byte {
	return append([]byte{messageVersion}, n[:]...)
}

func parseRenewRequest(b []byte) (netid.NetworkID, error) {
	var n netid.NetworkID
	if len(b) != renewRequestSize || b[0] != messageVersion {
		return n, ErrMalformed
	}
	copy(n[:], b[1:])
	return n, nil
}

// joinResponse is the host's answer to redeem and renew.
type joinResponse struct {
	Name       string
	Cert       []byte
	Revocation []byte
}

func (r joinResponse) marshal() []byte {
	name := r.Name
	for len(name) > MaxNameLen {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	b := []byte{messageVersion, byte(len(name))}
	b = append(b, name...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.Cert)))
	b = append(b, r.Cert...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Revocation)))
	return append(b, r.Revocation...)
}

func parseJoinResponse(b []byte) (joinResponse, error) {
	var r joinResponse
	if len(b) < 2 || len(b) > MaxResponseSize || b[0] != messageVersion {
		return r, ErrMalformed
	}
	nl := int(b[1])
	if nl > MaxNameLen || len(b) < 2+nl+2 {
		return r, ErrMalformed
	}
	r.Name = string(b[2 : 2+nl])
	off := 2 + nl
	cl := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if cl != CertSize || len(b) < off+cl+4 {
		return r, ErrMalformed
	}
	r.Cert = b[off : off+cl]
	off += cl
	rl := int64(binary.BigEndian.Uint32(b[off:]))
	off += 4
	if rl > MaxRevocationListSize || int64(len(b)-off) != rl {
		return r, ErrMalformed
	}
	r.Revocation = b[off:]
	return r, nil
}
