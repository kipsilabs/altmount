package availability

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
	"sync"

	"github.com/javi11/altmount/internal/config"
)

// Scope identifies one process generation of the active aggregate provider pool.
type Scope struct {
	Digest     string
	Generation string
}

// BuildPoolScopeDigest returns a non-secret SHA-256 identity for the active pool.
func BuildPoolScopeDigest(cfg *config.Config) string {
	if cfg == nil {
		return sha256Digest(nil)
	}
	entries := make([]string, 0, len(cfg.Providers))
	for _, p := range cfg.Providers {
		if p.Enabled == nil || !*p.Enabled {
			continue
		}
		backup := false
		if p.IsBackupProvider != nil {
			backup = *p.IsBackupProvider
		}
		entries = append(entries, canonicalFields(
			p.ID,
			normalizeHost(p.Host),
			itoa(p.Port),
			boolString(p.TLS),
			boolString(p.InsecureTLS),
			boolString(backup),
			p.StorageGroup,
			boolString(p.SkipPing),
			itoa(p.MaxConnections),
			itoa(p.MinConnectionsAlive),
			itoa(p.InflightRequests),
			itoa(p.StatInflightRequests),
			itoa(p.KeepaliveIntervalSeconds),
			p.KeepaliveCommand,
			p.UserAgent,
			itoa64(p.QuotaBytes),
			itoa(p.QuotaPeriodHours),
			digestText(p.Username),
		))
	}
	sort.Strings(entries)
	return sha256Digest([]byte(canonicalFields(entries...)))
}

// ScopeTracker starts a fresh generation for each process and rotates it when
// provider configuration changes are observed.
type ScopeTracker struct {
	mu    sync.RWMutex
	prior *config.Config
	scope Scope
}

func NewScopeTracker(cfg *config.Config) *ScopeTracker {
	return &ScopeTracker{prior: cfg, scope: Scope{Digest: BuildPoolScopeDigest(cfg), Generation: newGeneration()}}
}

func (t *ScopeTracker) Current() Scope {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.scope
}

func (t *ScopeTracker) OnConfigChange(oldConfig, newConfig *config.Config) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if oldConfig == nil {
		oldConfig = t.prior
	}
	if oldConfig == nil || newConfig == nil {
		t.scope = Scope{Digest: BuildPoolScopeDigest(newConfig), Generation: newGeneration()}
		t.prior = newConfig
		return
	}
	if oldConfig.ProvidersDiff(newConfig) != nil || oldConfig.ProvidersOrderChanged(newConfig) {
		t.scope = Scope{Digest: BuildPoolScopeDigest(newConfig), Generation: newGeneration()}
	}
	t.prior = newConfig
}

// BuildArticleDigest returns the SHA-256 digest of a normalized message ID.
func BuildArticleDigest(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if len(messageID) >= 2 && messageID[0] == '<' && messageID[len(messageID)-1] == '>' {
		messageID = strings.TrimSpace(messageID[1 : len(messageID)-1])
	}
	return digestText(messageID)
}

func canonicalFields(fields ...string) string {
	var b strings.Builder
	var buf [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(buf[:], uint64(len(field)))
		b.Write(buf[:])
		b.WriteString(field)
	}
	return b.String()
}

func digestText(value string) string { return sha256Digest([]byte(value)) }

func sha256Digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func newGeneration() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		sum := sha256.Sum256([]byte("availability-scope-generation"))
		return hex.EncodeToString(sum[:16])
	}
	return hex.EncodeToString(value[:])
}

func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
}
func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
func itoa(value int) string     { return formatInt(int64(value)) }
func itoa64(value int64) string { return formatInt(value) }
func formatInt(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
