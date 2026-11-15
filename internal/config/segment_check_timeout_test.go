package config

import (
	"testing"
	"time"
)

func TestImportSegmentCheckTimeout(t *testing.T) {
	intp := func(v int) *int { return &v }
	tests := []struct {
		name    string
		value   *int
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses the validator default", value: nil, want: 0},
		{name: "zero uses the validator default", value: intp(0), want: 0},
		{name: "lower bound", value: intp(5), want: 5 * time.Second},
		{name: "upper bound", value: intp(300), want: 300 * time.Second},
		{name: "below range", value: intp(4), wantErr: true},
		{name: "above range", value: intp(301), wantErr: true},
		{name: "negative", value: intp(-1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Import.SegmentCheckTimeoutSeconds = tt.value
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Validate() accepted segment_check_timeout_seconds=%d, want an error", *tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if got := cfg.GetImportSegmentCheckTimeout(); got != tt.want {
				t.Fatalf("GetImportSegmentCheckTimeout() = %s, want %s", got, tt.want)
			}
		})
	}
}
