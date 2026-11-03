package importer

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const maxNestedNzbJobs = 64
const nestedNzbChainKey = "_altmount_nzb_chain"

type nestedNzbJob struct {
	Name string
	Data []byte
}

type nestedNzbChain struct {
	Depth int        `json:"depth"`
	Seen  [][32]byte `json:"seen"`
}

type nestedNzbJobsError struct {
	Jobs  []nestedNzbJob
	Chain nestedNzbChain
}

func (e *nestedNzbJobsError) Error() string { return fmt.Sprintf("queue %d nested NZBs", len(e.Jobs)) }

func readNestedNzbChain(metadata *string) (nestedNzbChain, error) {
	var chain nestedNzbChain
	if metadata == nil || *metadata == "" {
		return chain, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*metadata), &fields); err != nil {
		return chain, nil
	}
	if raw, ok := fields[nestedNzbChainKey]; ok {
		if err := json.Unmarshal(raw, &chain); err != nil {
			return chain, NewNonRetryableError("invalid NZB chain metadata", err)
		}
	}
	if chain.Depth < 0 || chain.Depth > maxNestedNzbDepth || len(chain.Seen) > maxNestedNzbDepth {
		return chain, NewNonRetryableError("invalid NZB chain metadata", nil)
	}
	return chain, nil
}

func (chain nestedNzbChain) follow(data []byte) (nestedNzbChain, error) {
	if chain.Depth >= maxNestedNzbDepth {
		return chain, NewNonRetryableError("embedded NZB nesting limit exceeded", nil)
	}
	hash := sha256.Sum256(data)
	for _, seen := range chain.Seen {
		if hash == seen {
			return chain, NewNonRetryableError("embedded NZB cycle detected", nil)
		}
	}
	return nestedNzbChain{Depth: chain.Depth + 1, Seen: append(append([][32]byte{}, chain.Seen...), hash)}, nil
}
