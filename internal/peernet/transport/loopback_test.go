package transport

import (
	"errors"
	"testing"
)

func TestLoopbackHasItsOwnRefusal(t *testing.T) {
	for _, a := range []string{"127.0.0.1:80", "[::1]:80", "localhost:80", "a.localhost:80"} {
		// Even with private addresses allowed, loopback is refused, and says so.
		_, err := CheckPeerAddress(a, true, false)
		if !errors.Is(err, ErrLoopbackRefused) || !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: err = %v, want ErrLoopbackRefused (also an ErrAddressRefused)", a, err)
		}
		pa, perr := ValidateAddress(a)
		if perr != nil || !IsLoopbackAddress(pa) {
			t.Errorf("%s: IsLoopbackAddress = false", a)
		}
	}
	for _, a := range []string{"10.0.0.1:80", "192.168.1.2:80", "example.org:80", "nas.local:80"} {
		pa, _ := ValidateAddress(a)
		if IsLoopbackAddress(pa) {
			t.Errorf("%s: wrongly loopback", a)
		}
		if _, err := CheckPeerAddress(a, false, false); errors.Is(err, ErrLoopbackRefused) {
			t.Errorf("%s: wrongly ErrLoopbackRefused", a)
		}
	}
}
