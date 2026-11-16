package recsync

// Policy says what record exchange may do on one network at one exposure
// level. It is the only place that maps levels to behaviour.
//
//	level  Exists  Pull  AcceptPush  Publish  Push  Serve
//	  0      no     no      no         no      no    no     routes answer as if the network were unknown
//	  1      yes    yes     yes        no      no    no     receive only: serves an empty page
//	  2      yes    yes     yes        yes     yes   yes    own and relayed records served
//	  3      yes    yes     yes        yes     yes   yes    same as 2 for records, and serves its bundles
//
// A level 1 node accepts pushes because "receives" does not name a channel
// and an outbound-only publisher can reach a peer only by pushing; what it
// stores it never relays.
type Policy struct {
	// Exists: record routes answer for the network at all.
	Exists bool
	// Pull: this node pulls records from peers.
	Pull bool
	// AcceptPush: this node ingests records peers push to it.
	AcceptPush bool
	// Publish: this node may create and store its own records.
	Publish bool
	// Push: this node pushes its own records to peers.
	Push bool
	// Serve: this node serves its record list (own and relayed) to pullers.
	Serve bool
	// ServeBundles: this node serves finished-import bundles it announced.
	ServeBundles bool
}

// PolicyFor returns the policy of an exposure level. Out-of-range levels fail
// closed (nothing allowed).
func PolicyFor(level int) Policy {
	switch level {
	case 1:
		return Policy{Exists: true, Pull: true, AcceptPush: true}
	case 2:
		return Policy{Exists: true, Pull: true, AcceptPush: true, Publish: true, Push: true, Serve: true}
	case 3:
		return Policy{Exists: true, Pull: true, AcceptPush: true, Publish: true, Push: true, Serve: true, ServeBundles: true}
	}
	return Policy{}
}
