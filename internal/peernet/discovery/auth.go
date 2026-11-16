package discovery

import (
	"context"
	"net/http"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// ResponseDecorator adds headers to an outgoing response. It is called before
// the response is written and must not retain the map.
type ResponseDecorator func(h http.Header)

// Authorizer lets another component enforce network membership. All methods
// are called with the network the request is scoped to and the peer's
// authenticated Node ID.
//
// Server side: AuthorizeInbound decides whether peer may use the network and
// returns a decorator applied to the response (for example to attach a
// certificate). A non-nil error denies the request; the peer only sees a fixed
// 403 and the error text is never sent to it.
//
// Client side: DecorateOutbound adds credentials to an outgoing request's
// headers, and VerifyResponse validates the response status and headers
// before its body is read or trusted; a non-nil error makes the call fail.
type Authorizer interface {
	AuthorizeInbound(ctx context.Context, network netid.NetworkID, peer identity.NodeID, reqHeader http.Header) (ResponseDecorator, error)
	DecorateOutbound(ctx context.Context, network netid.NetworkID, peer identity.NodeID, reqHeader http.Header) error
	VerifyResponse(ctx context.Context, network netid.NetworkID, peer identity.NodeID, status int, respHeader http.Header) error
}

// AllowAll admits everyone and adds and checks nothing. It is the
// authoriser for the open network and for tests.
type AllowAll struct{}

// AuthorizeInbound admits the request.
func (AllowAll) AuthorizeInbound(context.Context, netid.NetworkID, identity.NodeID, http.Header) (ResponseDecorator, error) {
	return func(http.Header) {}, nil
}

// DecorateOutbound adds nothing.
func (AllowAll) DecorateOutbound(context.Context, netid.NetworkID, identity.NodeID, http.Header) error {
	return nil
}

// VerifyResponse accepts any response.
func (AllowAll) VerifyResponse(context.Context, netid.NetworkID, identity.NodeID, int, http.Header) error {
	return nil
}
