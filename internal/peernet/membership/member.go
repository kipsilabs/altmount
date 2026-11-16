package membership

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// JoinOptions are per-join settings.
type JoinOptions struct {
	// AllowPrivateAddresses lets this network's host address be a private
	// (RFC 1918 and similar) address. It is stored as the network's setting.
	AllowPrivateAddresses bool
}

func (m *Manager) call(ctx context.Context, n *Network, req transport.Request, maxResp int64) ([]byte, error) {
	if m.cfg.Caller == nil {
		return nil, errors.New("membership: no caller configured")
	}
	host, err := identity.ParseNodeID(n.HostNodeID)
	if err != nil {
		return nil, err
	}
	resp, err := m.cfg.Caller.Do(ctx, n.HostAddress, host, req,
		transport.CallOptions{AllowPrivate: n.AllowPrivateAddresses, MaxResponseBytes: maxResp,
			// The host address comes from the invite link / stored network row:
			// local configuration, not something a peer told us.
			TrustedAddress: true})
	if err != nil {
		return nil, fmt.Errorf("membership: call host: %w", err)
	}
	body, err := resp.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("membership: read host response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, ErrDenied
	}
	return body, nil
}

// verifyJoinResponse checks the certificate and list a host returned and
// returns them parsed. heldSeq is the revocation sequence already held.
func (m *Manager) verifyJoinResponse(r joinResponse, root ed25519.PublicKey, heldSeq uint64) (*Certificate, *RevocationList, error) {
	cert, err := ParseCertificate(r.Cert)
	if err != nil {
		return nil, nil, err
	}
	if cert.Role != RoleMember || cert.Verify(root, m.cfg.NodeID, m.now()) != nil {
		return nil, nil, fmt.Errorf("%w: certificate", ErrBadSignature)
	}
	list, err := m.verifyList(r.Revocation, root, heldSeq)
	if err != nil {
		return nil, nil, err
	}
	if list.Contains(m.cfg.NodeID) {
		return nil, nil, ErrRevoked
	}
	return cert, list, nil
}

func (m *Manager) verifyList(b []byte, root ed25519.PublicKey, heldSeq uint64) (*RevocationList, error) {
	list, err := ParseRevocationList(b)
	if err != nil {
		return nil, err
	}
	if err := list.Verify(root, m.now()); err != nil {
		return nil, err
	}
	if err := list.AcceptableOver(heldSeq); err != nil {
		return nil, err
	}
	return list, nil
}

// Join redeems an invite link: it dials the host pinned in the link, verifies
// the returned certificate and revocation list against the root key in the
// link, and stores the network. The link is never logged.
func (m *Manager) Join(ctx context.Context, link string, opts JoinOptions) (*Network, error) {
	l, err := ParseLink(link)
	if err != nil {
		return nil, err
	}
	switch _, err := m.cfg.Store.GetNetwork(ctx, l.NetworkID.String()); {
	case err == nil:
		return nil, ErrAlreadyJoined
	case !errors.Is(err, database.ErrPeernetNotFound):
		return nil, err
	}
	n := &Network{NetworkID: l.NetworkID.String(), HostAddress: l.HostAddr, HostNodeID: l.HostNode.String(),
		AllowPrivateAddresses: opts.AllowPrivateAddresses}
	body, err := m.call(ctx, n, transport.Request{Method: http.MethodPost, Path: PathRedeem,
		Body: redeemRequest{Network: l.NetworkID, Code: l.Code}.marshal()}, MaxResponseSize)
	if errors.Is(err, ErrDenied) {
		// The invite may have been redeemed by an earlier attempt of this very
		// join that failed locally (after the host already registered this
		// node): the host knows the node and renews its certificate. A host
		// that does not know it denies this too, with the same error.
		if rbody, rerr := m.call(ctx, n, transport.Request{Method: http.MethodPost, Path: PathRenew,
			Body: marshalRenewRequest(l.NetworkID)}, MaxResponseSize); rerr == nil {
			body, err = rbody, nil
		}
	}
	if err != nil {
		return nil, err
	}
	resp, err := parseJoinResponse(body)
	if err != nil {
		return nil, err
	}
	cert, list, err := m.verifyJoinResponse(resp, l.RootKey, 0)
	if err != nil {
		return nil, err
	}
	now := m.unix()
	row := database.PeernetNetwork{
		NetworkID: n.NetworkID, DisplayName: resp.Name, Role: database.PeernetRoleMember,
		AllowPrivateAddresses: opts.AllowPrivateAddresses, HostAddress: n.HostAddress, HostNodeID: n.HostNodeID,
		RootPublicKey: []byte(l.RootKey), OwnCertificate: cert.Marshal(), RevocationList: list.Marshal(),
		RevocationSeq: list.Sequence, CreatedAt: now, UpdatedAt: now,
	}
	defer m.invalidate()
	if err := m.cfg.Store.CreateNetwork(ctx, row); err != nil {
		if errors.Is(err, database.ErrPeernetExists) {
			return nil, ErrAlreadyJoined
		}
		return nil, err
	}
	// The join response carried the current list: count it as a refresh.
	m.recordMaint(row.NetworkID, m.now(), nil)
	return m.GetNetwork(ctx, l.NetworkID)
}

// Leave forgets a joined network. Hosted networks use DeleteNetwork.
func (m *Manager) Leave(ctx context.Context, id netid.NetworkID) error {
	n, err := m.GetNetwork(ctx, id)
	if err != nil {
		return err
	}
	if n.Role != database.PeernetRoleMember {
		return ErrNotMember
	}
	defer m.invalidate()
	m.forgetMaint(n.NetworkID)
	return m.cfg.Store.DeleteNetwork(ctx, n.NetworkID)
}

// Maintenance tuning.
const (
	// DefaultCallTimeout bounds one maintenance call to a host.
	DefaultCallTimeout = 15 * time.Second
	// RevocationRefreshEvery is the longest a member goes without fetching a
	// network's revocation list from its host while the certificate is not due.
	RevocationRefreshEvery = 15 * time.Minute
	maintBackoffBase       = 30 * time.Second
	maintBackoffMax        = 15 * time.Minute
	// refusalCheckEvery limits how often a refusal by the host may trigger an
	// immediate revocation-list check for one network.
	refusalCheckEvery = time.Minute
)

// maintState is the per-network maintenance bookkeeping of a member.
type maintState struct {
	lastRefresh time.Time // last successful renewal or list fetch
	failures    int
	next        time.Time // no attempt before this (backoff)
	lastForced  time.Time // last list check triggered by a refusal
}

func (m *Manager) maintFor(id string) maintState {
	m.maintMu.Lock()
	defer m.maintMu.Unlock()
	if st := m.maint[id]; st != nil {
		return *st
	}
	return maintState{}
}

// recordMaint notes the outcome of a host call. A nil err resets the backoff.
func (m *Manager) recordMaint(id string, at time.Time, err error) {
	m.maintMu.Lock()
	defer m.maintMu.Unlock()
	st := m.maint[id]
	if st == nil {
		st = &maintState{}
		m.maint[id] = st
	}
	if err == nil {
		st.lastRefresh, st.failures, st.next = at, 0, time.Time{}
		return
	}
	st.failures++
	back := maintBackoffBase
	for i := 1; i < st.failures && back < maintBackoffMax; i++ {
		back *= 2
	}
	st.next = at.Add(min(back, maintBackoffMax))
}

func (m *Manager) forgetMaint(id string) {
	m.maintMu.Lock()
	delete(m.maint, id)
	m.maintMu.Unlock()
}

// RenewDue does the periodic membership upkeep. Hosted networks re-issue the
// host's own certificate and repair a revocation list that misses a revoked
// member. For each joined network it renews a certificate that reached
// half-life, and otherwise fetches the revocation list at most every
// RevocationRefreshEvery (and once after the first call on a new Manager).
// Each host call has its own short timeout, networks run concurrently so one
// unreachable host delays no other, and repeated failures back off per
// network. It starts no goroutines that outlive the call: run it periodically.
// Errors of individual networks are joined.
func (m *Manager) RenewDue(ctx context.Context) error {
	nets, err := m.cfg.Store.ListNetworks(ctx)
	if err != nil {
		return err
	}
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	add := func(n *Network, err error) {
		if err == nil {
			return
		}
		mu.Lock()
		errs = append(errs, fmt.Errorf("network %.8s: %w", n.NetworkID, err))
		mu.Unlock()
	}
	for i := range nets {
		n := &nets[i]
		switch n.Role {
		case database.PeernetRoleHost:
			if _, err := m.ensureHostCert(ctx, n); err != nil {
				add(n, err)
			}
			add(n, m.repairRevocations(ctx, n))
		case database.PeernetRoleMember:
			wg.Add(1)
			go func() {
				defer wg.Done()
				add(n, m.maintainMember(ctx, n))
			}()
		}
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (m *Manager) maintainMember(ctx context.Context, n *Network) error {
	if n.RevokedAt != 0 {
		return nil // removed by the host: nothing to renew, nothing to refresh
	}
	now := m.now()
	st := m.maintFor(n.NetworkID)
	if now.Before(st.next) {
		return nil // backing off after repeated failures
	}
	cert, perr := ParseCertificate(n.OwnCertificate)
	renew := perr != nil || certDue(cert, now)
	if !renew && !st.lastRefresh.IsZero() && now.Sub(st.lastRefresh) < RevocationRefreshEvery {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, m.callTimeout)
	defer cancel()
	var err error
	if renew {
		err = m.renewMember(cctx, n)
		if errors.Is(err, ErrDenied) {
			// A denied renewal may mean the host removed this node. The signed
			// list says so; check it now rather than at the next refresh.
			if lerr := m.refreshRevocations(cctx, n); errors.Is(lerr, ErrRevoked) {
				err = lerr
			}
		}
	} else {
		err = m.refreshRevocations(cctx, n)
	}
	if ctx.Err() == nil { // a shutdown is not the host's fault
		m.recordMaint(n.NetworkID, now, err)
	}
	return err
}

// renewMember asks the host for a fresh certificate.
func (m *Manager) renewMember(ctx context.Context, n *Network) error {
	root := ed25519.PublicKey(n.RootPublicKey)
	id, err := netid.ParseNetworkID(n.NetworkID)
	if err != nil {
		return err
	}
	body, err := m.call(ctx, n, transport.Request{Method: http.MethodPost, Path: PathRenew,
		Body: marshalRenewRequest(id)}, MaxResponseSize)
	if err != nil {
		return err
	}
	resp, err := parseJoinResponse(body)
	if err != nil {
		return err
	}
	newCert, list, err := m.verifyJoinResponse(resp, root, n.RevocationSeq)
	if err != nil {
		m.noteRevoked(ctx, n, err)
		return err
	}
	defer m.invalidate()
	if err := m.cfg.Store.SetRevocationList(ctx, n.NetworkID, list.Sequence, list.Marshal(), m.unix()); err != nil {
		return err
	}
	m.invalidate() // the list changed: never serve a row older than it
	return m.cfg.Store.SetOwnCertificate(ctx, n.NetworkID, newCert.Marshal(), m.unix())
}

// refreshRevocations fetches and stores the host's current revocation list.
func (m *Manager) refreshRevocations(ctx context.Context, n *Network) error {
	body, err := m.call(ctx, n, transport.Request{Method: http.MethodGet, Path: PathRevocations,
		Query: url.Values{"n": {n.NetworkID}}}, MaxRevocationListSize)
	if err != nil {
		return err
	}
	list, err := m.verifyList(body, ed25519.PublicKey(n.RootPublicKey), n.RevocationSeq)
	if err != nil {
		return err
	}
	if list.Contains(m.cfg.NodeID) {
		m.noteRevoked(ctx, n, ErrRevoked)
		return ErrRevoked
	}
	defer m.invalidate()
	return m.cfg.Store.SetRevocationList(ctx, n.NetworkID, list.Sequence, list.Marshal(), m.unix())
}

// RefreshRevocations fetches the current revocation list of a joined network.
func (m *Manager) RefreshRevocations(ctx context.Context, id netid.NetworkID) error {
	n, err := m.GetNetwork(ctx, id)
	if err != nil {
		return err
	}
	if n.Role != database.PeernetRoleMember {
		return ErrNotMember
	}
	err = m.refreshRevocations(ctx, n)
	if ctx.Err() == nil {
		m.recordMaint(n.NetworkID, m.now(), err)
	}
	return err
}

// noteRevoked persists that the host removed this node when err says so. The
// state is kept until the user leaves the network.
func (m *Manager) noteRevoked(ctx context.Context, n *Network, err error) {
	if !errors.Is(err, ErrRevoked) || n.RevokedAt != 0 {
		return
	}
	defer m.invalidate()
	if serr := m.cfg.Store.SetRevoked(context.WithoutCancel(ctx), n.NetworkID, m.unix(), m.unix()); serr != nil {
		slog.WarnContext(ctx, "Could not record removal from a network", "error", serr)
		return
	}
	slog.WarnContext(ctx, "This node was removed from a private network by its host", "network", n.NetworkID[:8])
}

// NoteRefused is called when the host of a joined network refused a request
// without any membership proof. That may mean the host removed this node, so
// the signed revocation list is fetched right away (at most once per
// refusalCheckEvery per network) instead of at the next scheduled refresh.
func (m *Manager) NoteRefused(ctx context.Context, id netid.NetworkID) {
	n, err := m.GetNetwork(ctx, id)
	if err != nil || n.Role != database.PeernetRoleMember || n.RevokedAt != 0 {
		return
	}
	now := m.now()
	m.maintMu.Lock()
	st := m.maint[n.NetworkID]
	if st == nil {
		st = &maintState{}
		m.maint[n.NetworkID] = st
	}
	if !st.lastForced.IsZero() && now.Sub(st.lastForced) < refusalCheckEvery {
		m.maintMu.Unlock()
		return
	}
	st.lastForced = now
	m.maintMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, m.callTimeout)
	defer cancel()
	_ = m.refreshRevocations(cctx, n) // a failure leaves the scheduled refresh in charge
}
