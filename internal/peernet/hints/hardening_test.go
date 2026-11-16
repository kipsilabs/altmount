package hints

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
)

func (e *env) inPrefix(prefix string, ids ...int) {
	if e.peers.prefix == nil {
		e.peers.prefix = map[string]string{}
	}
	for _, i := range ids {
		e.peers.prefix[node(i)] = prefix
	}
}

func (e *env) label(t *testing.T) NetworkHealth {
	t.Helper()
	got, err := e.a.NetworkHealthy(context.Background(), release)
	if err != nil {
		t.Fatal(err)
	}
	return got[release]
}

func TestManyIdentitiesInOnePrefixCountAsOnePublisher(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 10; i++ {
		e.seed(openNet, i, nil)
	}
	e.inPrefix("198.51.100.0/24", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("ten identities in one /24 corroborated a floor check: %+v", got)
	}
	if h := e.label(t); h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("label %+v: ten identities in one /24 are one publisher", h)
	}
	e.seed(openNet, 11, nil) // a second prefix
	if got := e.advise(t); got.Kind != AdviceReduceSampleToFloor {
		t.Fatalf("two distinct prefixes must corroborate: %+v", got)
	}
}

func TestPublisherWithoutVerifiedAddressDoesNotCount(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	e.inPrefix("-", 2)
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("a publisher with no verified address counted: %+v", got)
	}
}

func TestOneUnhealthyIdentityCannotFlipACorroboratedLabel(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 3
	for i := 1; i <= 3; i++ {
		e.seed(openNet, i, nil)
	}
	e.seed(openNet, 4, unhealthy)
	if h := e.label(t); !h.Healthy {
		t.Fatalf("label %+v: one unhealthy identity removed the label of three healthy prefixes", h)
	}
	if got := e.advise(t); got.Kind != AdviceReduceSampleToFloor {
		t.Fatalf("one unhealthy identity blocked advice backed by three healthy prefixes: %+v", got)
	}
	// Unhealthy evidence that meets the threshold itself resolves conservatively.
	e.seed(openNet, 5, unhealthy)
	e.seed(openNet, 6, unhealthy)
	if h := e.label(t); h.Healthy {
		t.Fatalf("label %+v: three unhealthy prefixes must resolve conservatively", h)
	}
	if got := e.advise(t); got.Kind != AdviceRecheckSooner {
		t.Fatalf("advice %+v: three unhealthy prefixes must move the check forward", got)
	}
}

func TestUnhealthyEvidenceFromOnePrefixDoesNotCountTwice(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 5; i++ {
		e.seed(openNet, i, unhealthy)
	}
	e.inPrefix("198.51.100.0/24", 1, 2, 3, 4, 5)
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("five unhealthy identities in one /24 moved a check forward: %+v", got)
	}
}

func TestPrivateNetworkBehaviourIsUnchangedByPrefixRules(t *testing.T) {
	e := newEnv(t, privRecv)
	e.seed(privNet, 1, nil)
	e.inPrefix("198.51.100.0/24", 1)
	if got := e.advise(t); got.Kind != AdviceMaySkipOneCheck {
		t.Fatalf("one private member must still allow a skip: %+v", got)
	}
	e.seed(privNet, 2, unhealthy)
	if got := e.advise(t); got.Kind != AdviceRecheckSooner {
		t.Fatalf("one unhealthy private member must still resolve conservatively: %+v", got)
	}
}

var _ = database.PeernetAttestation{}
