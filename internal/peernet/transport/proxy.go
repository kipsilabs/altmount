package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"golang.org/x/net/http/httpproxy"
)

// ProxyFunc returns the proxy to use for a target URL, or nil for a direct
// connection. It has the same shape as http.Transport.Proxy.
type ProxyFunc func(*url.URL) (*url.URL, error)

// ProxyFromSettings builds a ProxyFunc from the application's outbound proxy
// settings (the same http/https/no-proxy values internal/httpclient uses).
// Empty http and https values mean no proxy. Environment variables are not
// consulted.
func ProxyFromSettings(httpProxy, httpsProxy, noProxy string) ProxyFunc {
	if httpProxy == "" && httpsProxy == "" {
		return nil
	}
	cfg := &httpproxy.Config{HTTPProxy: httpProxy, HTTPSProxy: httpsProxy, NoProxy: noProxy}
	return cfg.ProxyFunc()
}

const maxProxyResponseBytes = 8 << 10

// dialViaProxy opens a raw TCP tunnel to target through an HTTP or HTTPS
// proxy using CONNECT. The TLS session with the peer is then run over the
// tunnel by the caller, so the proxy only ever sees ciphertext. SOCKS proxies
// are not supported.
func dialViaProxy(ctx context.Context, d *net.Dialer, proxy *url.URL, target string) (net.Conn, error) {
	var tunnelHost string
	switch proxy.Scheme {
	case "http", "https":
		tunnelHost = proxy.Host
		if proxy.Port() == "" {
			port := "80"
			if proxy.Scheme == "https" {
				port = "443"
			}
			tunnelHost = net.JoinHostPort(proxy.Hostname(), port)
		}
	default:
		return nil, fmt.Errorf("%w: %q", ErrProxyUnsupported, proxy.Scheme)
	}
	conn, err := d.DialContext(ctx, "tcp", tunnelHost)
	if err != nil {
		return nil, fmt.Errorf("%w: dial proxy: %v", ErrProxyConnect, err)
	}
	fail := func(why string, err error) (net.Conn, error) {
		_ = conn.Close()
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrProxyConnect, why, err)
		}
		return nil, fmt.Errorf("%w: %s", ErrProxyConnect, why)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if proxy.Scheme == "https" {
		tc := tls.Client(conn, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: proxy.Hostname()})
		if err := tc.HandshakeContext(ctx); err != nil {
			return fail("proxy tls", err)
		}
		conn = tc
	}
	req := &http.Request{
		Method: http.MethodConnect, URL: &url.URL{Opaque: target}, Host: target,
		Header: http.Header{},
	}
	if u := proxy.User; u != nil {
		pw, _ := u.Password()
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.Username()+":"+pw)))
	}
	if err := req.Write(conn); err != nil {
		return fail("write connect", err)
	}
	br := bufio.NewReaderSize(&limitedReader{r: conn, n: maxProxyResponseBytes}, 1024)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return fail("read connect response", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fail(fmt.Sprintf("proxy returned status %d", resp.StatusCode), nil)
	}
	if br.Buffered() != 0 {
		return fail("unexpected data after connect", nil)
	}
	_ = conn.SetDeadline(zeroTime)
	return conn, nil
}

type limitedReader struct {
	r interface{ Read([]byte) (int, error) }
	n int
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, ErrBodyTooLarge
	}
	if len(p) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= n
	return n, err
}
