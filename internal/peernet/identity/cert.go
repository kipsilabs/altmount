package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"time"
)

const (
	certBackdate    = 24 * time.Hour
	certLifetime    = 5 * 365 * 24 * time.Hour
	maxCertDERBytes = 2048
)

// ErrInvalidCertificate is returned for any certificate that is not a
// well-formed, self-consistent, currently valid node certificate.
var ErrInvalidCertificate = errors.New("identity: invalid node certificate")

// TLSCertificate returns a self-signed Ed25519 certificate for this key,
// valid from a day ago for five years. It is regenerated on each call.
func (k *Key) TLSCertificate() (tls.Certificate, error) {
	return k.tlsCertificateAt(time.Now())
}

func (k *Key) tlsCertificateAt(now time.Time) (tls.Certificate, error) {
	return k.tlsCertificateBetween(now.Add(-certBackdate), now.Add(certLifetime))
}

func (k *Key) tlsCertificateBetween(notBefore, notAfter time.Time) (tls.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("identity: serial: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: k.NodeID().String()},
		NotBefore:             notBefore.UTC(),
		NotAfter:              notAfter.UTC(),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	s := signer{k: k}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, s.Public(), s)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("identity: create certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: s}, nil
}

// PeerNodeID validates the certificate chain presented by a peer and returns
// the peer's Node ID. Exactly one certificate is accepted. It must be an
// Ed25519 end-entity certificate that is currently valid, that verifies under
// its own public key, and whose subject names the Node ID of that key.
func PeerNodeID(rawCerts [][]byte) (NodeID, error) {
	return peerNodeIDAt(rawCerts, time.Now())
}

func peerNodeIDAt(rawCerts [][]byte, now time.Time) (NodeID, error) {
	bad := func(why string) (NodeID, error) {
		return NodeID{}, fmt.Errorf("%w: %s", ErrInvalidCertificate, why)
	}
	if len(rawCerts) != 1 {
		return bad("expected exactly one certificate")
	}
	raw := rawCerts[0]
	if len(raw) == 0 || len(raw) > maxCertDERBytes {
		return bad("bad certificate size")
	}
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		return bad("unparsable")
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize || cert.PublicKeyAlgorithm != x509.Ed25519 {
		return bad("not an Ed25519 key")
	}
	if ValidatePublicKey(pub) != nil {
		return bad("weak public key")
	}
	if cert.SignatureAlgorithm != x509.PureEd25519 {
		return bad("unexpected signature algorithm")
	}
	if cert.IsCA {
		return bad("CA certificates are not accepted")
	}
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return bad("not currently valid")
	}
	if err := cert.CheckSignature(x509.PureEd25519, cert.RawTBSCertificate, cert.Signature); err != nil {
		return bad("self-signature invalid")
	}
	id := NodeIDFromPublicKey(pub)
	if cert.Subject.CommonName != id.String() {
		return bad("subject does not match key")
	}
	return id, nil
}
