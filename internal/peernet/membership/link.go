package membership

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

const (
	linkPrefix = "altmountnet://join?"
	// MaxLinkLen caps the length of an invite link accepted by ParseLink.
	MaxLinkLen = 512
)

var linkKeys = [...]string{"v", "n", "k", "a", "h", "c"}

// Link is a parsed invite link. Its String and GoString methods redact the
// invite code; use Encode to obtain the shareable text.
type Link struct {
	NetworkID netid.NetworkID
	RootKey   ed25519.PublicKey
	HostAddr  string
	HostNode  identity.NodeID
	Code      InviteCode
}

// String never reveals the code.
func (l Link) String() string   { return "invite-link(redacted)" }
func (l Link) GoString() string { return l.String() }

// Encode renders the link. Treat the result as a secret and do not log it.
func (l Link) Encode() (string, error) {
	if len(l.RootKey) != ed25519.PublicKeySize || !netid.FromRootKey(l.RootKey).Equal(l.NetworkID) {
		return "", ErrWrongNetwork
	}
	pa, err := transport.ValidateAddress(l.HostAddr)
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("%sv=1&n=%s&k=%s&a=%s&h=%s&c=%s", linkPrefix, l.NetworkID,
		base64.RawURLEncoding.EncodeToString(l.RootKey), pa.HostPort(), l.HostNode, l.Code.Text())
	if len(s) > MaxLinkLen {
		return "", ErrMalformed
	}
	return s, nil
}

// ParseLink strictly parses an invite link. Errors never echo the input.
func ParseLink(s string) (Link, error) {
	var l Link
	if len(s) > MaxLinkLen || !strings.HasPrefix(s, linkPrefix) {
		return l, ErrMalformed
	}
	parts := strings.Split(s[len(linkPrefix):], "&")
	if len(parts) != len(linkKeys) {
		return l, ErrMalformed
	}
	vals := make([]string, len(linkKeys))
	for i, p := range parts {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k != linkKeys[i] || v == "" {
			return l, ErrMalformed
		}
		vals[i] = v
	}
	if vals[0] != "1" {
		return l, ErrMalformed
	}
	nid, err := netid.ParseNetworkID(vals[1])
	if err != nil {
		return l, ErrMalformed
	}
	if len(vals[2]) != base64.RawURLEncoding.EncodedLen(ed25519.PublicKeySize) {
		return l, ErrMalformed
	}
	key, err := base64.RawURLEncoding.Strict().DecodeString(vals[2])
	if err != nil || len(key) != ed25519.PublicKeySize || identity.ValidatePublicKey(key) != nil {
		return l, ErrMalformed
	}
	pa, err := transport.ValidateAddress(vals[3])
	if err != nil || pa.HostPort() != vals[3] {
		return l, ErrMalformed
	}
	node, err := identity.ParseNodeID(vals[4])
	if err != nil {
		return l, ErrMalformed
	}
	code, err := ParseInviteCode(vals[5])
	if err != nil {
		return l, ErrMalformed
	}
	if !netid.FromRootKey(key).Equal(nid) {
		return l, ErrWrongNetwork
	}
	return Link{NetworkID: nid, RootKey: ed25519.PublicKey(key), HostAddr: pa.HostPort(), HostNode: node, Code: code}, nil
}
