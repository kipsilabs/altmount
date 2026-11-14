package hints

import (
	"context"
	"fmt"
	"time"
)

// knownCache answers per-call "may this publisher count" questions: always
// yes in a private network, and in the open network only for nodes known for
// MinKnown, with a bounded number of lookups per call. On the open network it
// also tells which network prefix a publisher was verified at, so identities
// sharing a prefix can be counted once.
type knownCache struct {
	a       *Advisor
	ctx     context.Context
	n       Network
	now     time.Time
	res     map[string]bool
	prefix  map[string]string
	lookups int
}

func newKnownCache(a *Advisor, ctx context.Context, n Network, now time.Time) *knownCache {
	return &knownCache{a: a, ctx: ctx, n: n, now: now, res: map[string]bool{}, prefix: map[string]string{}}
}

// groupOf is the key distinct publishers are counted by: the publisher itself
// in a private network, its verified network prefix on the open one.
func (k *knownCache) groupOf(node string) string {
	if p, ok := k.prefix[node]; ok {
		return p
	}
	return node
}

// prefetch resolves the standing of many publishers with one batch lookup
// per chunk when the peer lookup supports it. Results are exact, so the
// per-call lookup bound (which only guards one-by-one lookups) cannot let
// fresh identities push known publishers out.
func (k *knownCache) prefetch(nodes []string) error {
	if !k.n.ID.IsOpen() || len(nodes) == 0 {
		return nil
	}
	pre, hasPrefixes := k.a.cfg.Peers.(PeerPrefixes)
	batch, hasBatch := k.a.cfg.Peers.(PeerSeenBatch)
	if !hasPrefixes && !hasBatch {
		return nil
	}
	for start := 0; start < len(nodes); start += prefetchChunk {
		chunk := nodes[start:min(start+prefetchChunk, len(nodes))]
		if hasPrefixes {
			got, err := pre.KnownPrefixes(k.ctx, k.n.ID.String(), chunk, k.a.cfg.MinKnown, k.now)
			if err != nil {
				return fmt.Errorf("hints: peer first-seen: %w", err)
			}
			for _, n := range chunk {
				p, ok := got[n]
				k.res[n] = ok && p != ""
				if k.res[n] {
					k.prefix[n] = p
				}
			}
			continue
		}
		got, err := batch.KnownAmong(k.ctx, k.n.ID.String(), chunk, k.a.cfg.MinKnown, k.now)
		if err != nil {
			return fmt.Errorf("hints: peer first-seen: %w", err)
		}
		for _, n := range chunk {
			k.res[n] = got[n]
		}
	}
	return nil
}

func (k *knownCache) eligible(node string) (bool, error) {
	if !k.n.ID.IsOpen() {
		return true, nil
	}
	if v, ok := k.res[node]; ok {
		return v, nil
	}
	if k.lookups >= maxPublisherLookups {
		return false, nil
	}
	k.lookups++
	ok, err := k.a.cfg.Peers.KnownFor(k.ctx, k.n.ID.String(), node, k.a.cfg.MinKnown, k.now)
	if err != nil {
		return false, fmt.Errorf("hints: peer first-seen: %w", err)
	}
	k.res[node] = ok
	return ok, nil
}
