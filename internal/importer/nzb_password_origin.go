package importer

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/kipsilabs/altmount/internal/importer/parser"
)

// passwordInjectedKey marks, in a queue item's metadata JSON, that the API
// embedded a user-supplied password into the NZB it enqueued (the "&p=" of a
// link or the password field of a form). From then on that password is
// indistinguishable from one the NZB came with, so the mark is how the
// importer tells them apart. The password itself is never stored here.
const passwordInjectedKey = "_altmount_pw_injected"

// MarkPasswordInjected returns queue metadata JSON that records an injected
// password, keeping every key the metadata already had. Unparsable existing
// metadata is replaced (it carried no readable key anyway).
func MarkPasswordInjected(metadata *string) *string {
	fields := map[string]json.RawMessage{}
	if metadata != nil && strings.TrimSpace(*metadata) != "" {
		if err := json.Unmarshal([]byte(*metadata), &fields); err != nil || fields == nil {
			fields = map[string]json.RawMessage{}
		}
	}
	fields[passwordInjectedKey] = json.RawMessage("true")
	out, err := json.Marshal(fields)
	if err != nil {
		return metadata
	}
	s := string(out)
	return &s
}

// passwordInjected reports whether the queue metadata carries the mark.
func passwordInjected(metadata *string) bool {
	if metadata == nil || *metadata == "" {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*metadata), &fields); err != nil {
		return false
	}
	raw, ok := fields[passwordInjectedKey]
	return ok && string(raw) == "true"
}

// NZBPasswordRecorder is told which written files were imported with a
// password that came with the NZB's own content (a meta password present in
// the NZB as received). It is installed only while the peer network is on and
// never receives the password.
type NZBPasswordRecorder interface {
	RecordNZBPasswordFiles(ctx context.Context, virtualPaths []string)
}

type nzbPasswordHolder struct{ r NZBPasswordRecorder }

// SetNZBPasswordRecorder installs (or, with nil, removes) the recorder.
func (proc *Processor) SetNZBPasswordRecorder(r NZBPasswordRecorder) {
	if r == nil {
		proc.pwRecorder.Store(nil)
		return
	}
	proc.pwRecorder.Store(&nzbPasswordHolder{r: r})
}

// SetNZBPasswordRecorder installs (or, with nil, removes) the recorder on the
// service's processor.
func (s *Service) SetNZBPasswordRecorder(r NZBPasswordRecorder) {
	if s.processor != nil {
		s.processor.SetNZBPasswordRecorder(r)
	}
}

// recordNZBPassword tells the recorder about the written files when the
// password in effect is a meta password of the NZB as received: not one of the
// name or the subjects, not one the API injected, and not one of an NZB found
// inside another release's archive.
func (proc *Processor) recordNZBPassword(ctx context.Context, parsed *parser.ParsedNzb, metadata *string, written []string) {
	holder := proc.pwRecorder.Load()
	if holder == nil || parsed == nil || parsed.PasswordOrigin() != parser.PasswordOriginMeta || passwordInjected(metadata) {
		return
	}
	if chain, err := readNestedNzbChain(metadata); err != nil || chain.Depth > 0 {
		return
	}
	paths := make([]string, 0, len(written))
	for _, p := range written {
		if !strings.HasPrefix(p, "DIR:") {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			proc.log.ErrorContext(ctx, "NZB password recorder panicked", "panic", r)
		}
	}()
	holder.r.RecordNZBPasswordFiles(ctx, paths)
}
