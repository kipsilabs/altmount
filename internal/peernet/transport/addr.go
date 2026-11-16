package transport

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

const (
	maxAddrLen  = 255
	maxHostLen  = 253
	maxLabelLen = 63
)

// ParsedAddr is a validated peer address.
type ParsedAddr struct {
	// Host is a hostname (lower-cased) or a literal IP without brackets.
	Host string
	// IP is set when Host is a literal IP address (IPv4-mapped forms unmapped).
	IP   netip.Addr
	Port uint16
}

// HostPort returns the canonical "host:port" form.
func (p ParsedAddr) HostPort() string {
	return net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port)))
}

// ValidateAddress checks that s is a plain "host:port" (IPv6 literals in
// brackets): no scheme, userinfo, path, query, fragment, whitespace, zone or
// control characters, a port in 1-65535, and a bounded length.
func ValidateAddress(s string) (ParsedAddr, error) {
	bad := func(why string) (ParsedAddr, error) {
		return ParsedAddr{}, fmt.Errorf("%w: %s", ErrInvalidAddress, why)
	}
	if s == "" || len(s) > maxAddrLen {
		return bad("length")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= 0x20 || c >= 0x7f {
			return bad("illegal character")
		}
		switch c {
		case '/', '\\', '@', '?', '#', '%', '*':
			return bad("illegal character")
		}
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil || host == "" || portStr == "" {
		return bad("not host:port")
	}
	if len(portStr) > 5 {
		return bad("port")
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return bad("port")
	}
	if strings.HasPrefix(s, "[") != strings.Contains(host, ":") {
		return bad("ipv6 literals must be bracketed")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" {
			return bad("zone")
		}
		ip = ip.Unmap()
		return ParsedAddr{Host: ip.String(), IP: ip, Port: uint16(port)}, nil
	}
	if strings.Contains(host, ":") {
		return bad("host")
	}
	if !validHostname(host) {
		return bad("hostname")
	}
	return ParsedAddr{Host: strings.ToLower(host), Port: uint16(port)}, nil
}

func validHostname(h string) bool {
	if len(h) > maxHostLen || h == "" {
		return false
	}
	labels := strings.Split(h, ".")
	// A final label starting with a digit looks like a legacy numeric IP form
	// (e.g. "0x7f.1" or "1.2.3"); resolvers may read it as an address.
	if last := labels[len(labels)-1]; last == "" || last[0] >= '0' && last[0] <= '9' {
		return false
	}
	for _, label := range labels {
		if label == "" || len(label) > maxLabelLen || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '-':
			default:
				return false
			}
		}
	}
	return true
}

// isLocalName reports names that always mean this machine.
func isLocalName(h string) bool {
	h = strings.ToLower(h)
	return h == "localhost" || strings.HasSuffix(h, ".localhost") || strings.HasSuffix(h, ".local") ||
		strings.HasSuffix(h, ".internal") || strings.HasSuffix(h, ".localdomain")
}

// IsLoopbackAddress reports whether a is a loopback IP or a name that always
// means this machine (localhost, *.localhost). Loopback is never a usable
// peer address.
func IsLoopbackAddress(a ParsedAddr) bool {
	if a.IP.IsValid() {
		return a.IP.IsLoopback()
	}
	h := strings.ToLower(a.Host)
	return h == "localhost" || strings.HasSuffix(h, ".localhost")
}

type ipPolicy struct {
	allowPrivate  bool
	allowLoopback bool
	// trusted marks an address from local configuration (the user's own list
	// or a local discovery source). Untrusted (peer-learned) addresses may not
	// use denied service ports and, through a proxy, may not be hostnames.
	trusted bool
}

var (
	cgnat       = netip.MustParsePrefix("100.64.0.0/10")
	alwaysBlock = mustPrefixes(
		"0.0.0.0/8", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24",
		"203.0.113.0/24", "240.0.0.0/4", "255.255.255.255/32",
		"::/96",        // IPv4-compatible and unspecified/loopback range
		"64:ff9b::/96", // NAT64: embeds an IPv4 address
		"64:ff9b:1::/48",
		"2002::/16",     // 6to4: embeds an IPv4 address
		"2001::/32",     // Teredo
		"2001:db8::/32", // documentation
		"100::/64",      // discard-only
		"fec0::/10",     // deprecated site-local
	)
)

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// check returns nil if connecting to ip is allowed by the policy.
func (p ipPolicy) check(ip netip.Addr) error {
	ip = ip.Unmap()
	refuse := func(why string) error {
		return fmt.Errorf("%w: %s", ErrAddressRefused, why)
	}
	if !ip.IsValid() || ip.Zone() != "" {
		return refuse("invalid address")
	}
	if ip.IsLoopback() {
		if p.allowLoopback {
			return nil
		}
		return ErrLoopbackRefused
	}
	if ip.IsUnspecified() {
		return refuse("unspecified")
	}
	if ip.IsMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsLinkLocalMulticast() {
		return refuse("multicast")
	}
	if ip.IsLinkLocalUnicast() {
		return refuse("link-local")
	}
	for _, pre := range alwaysBlock {
		if pre.Contains(ip) {
			return refuse("reserved range")
		}
	}
	if ip.IsPrivate() || cgnat.Contains(ip) {
		if p.allowPrivate {
			return nil
		}
		return refuse("private")
	}
	if !ip.IsGlobalUnicast() {
		return refuse("not global unicast")
	}
	return nil
}

// deniedPorts are well-known service ports that are never dialled for an
// address learned from a peer (hello, peer exchange): an attacker could
// otherwise use this node to knock on mail, database or admin services of a
// third party. Addresses from the user's own configuration or a local
// discovery source are trusted and exempt. The TLS handshake that would follow
// a connect can only ever send a ClientHello, but a connect itself is a probe.
var deniedPorts = map[uint16]struct{}{
	22: {}, 23: {}, 25: {}, 53: {}, 110: {}, 111: {}, 135: {}, 139: {}, 143: {}, 389: {}, 445: {},
	465: {}, 587: {}, 636: {}, 993: {}, 995: {}, 1433: {}, 1521: {}, 2049: {}, 2375: {}, 2376: {},
	3306: {}, 3389: {}, 5432: {}, 5900: {}, 6379: {}, 9200: {}, 11211: {}, 27017: {},
}

// PortDenied reports whether port is on the deny-list for peer-learned
// addresses.
func PortDenied(port uint16) bool {
	_, ok := deniedPorts[port]
	return ok
}

// CheckLearnedAddress validates an address learned from a peer (an exchange
// entry): it must be a literal IP, a public one (private, CGNAT, loopback and
// the like are refused whatever allowPrivate says, so a peer can never point
// this node at the local network), and not use a denied service port.
// allowPrivate is accepted only to keep call sites explicit; it is ignored.
// allowLoopback is for tests only.
func CheckLearnedAddress(addr string, _ bool, allowLoopback bool) (ParsedAddr, error) {
	pa, err := CheckPeerAddress(addr, false, allowLoopback)
	if err != nil {
		return ParsedAddr{}, err
	}
	if !pa.IP.IsValid() {
		return ParsedAddr{}, fmt.Errorf("%w: hostname not accepted from peers", ErrAddressRefused)
	}
	if PortDenied(pa.Port) {
		return ParsedAddr{}, fmt.Errorf("%w: port", ErrAddressRefused)
	}
	return pa, nil
}
