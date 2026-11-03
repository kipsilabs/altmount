package stremio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseReleaseTitle(t *testing.T) {
	tests := []struct {
		name          string
		releaseTitle  string
		expectedTitle string
		expectedClean string
		expectedS     int
		expectedE     int
		expectedPack  bool
	}{
		{
			name:          "Standard Scene The Ark",
			releaseTitle:  "The.Ark.S01E01.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb",
			expectedTitle: "The.Ark",
			expectedClean: "ark",
			expectedS:     1,
			expectedE:     1,
		},
		{
			name:          "The Ark with Year in Title",
			releaseTitle:  "The.Ark.2023.S01E01.2160p.WEB-DL.DDP5.1.HDR.H.265-FLUX",
			expectedTitle: "The.Ark",
			expectedClean: "ark",
			expectedS:     1,
			expectedE:     1,
		},
		{
			name:          "ARK The Animated Series",
			releaseTitle:  "ARK.The.Animated.Series.S01E01.Element.1.2160p.AMZN.WEB-DL.DDP5.1.HEVC-NTb",
			expectedTitle: "ARK.The.Animated.Series",
			expectedClean: "arktheanimatedseries",
			expectedS:     1,
			expectedE:     1,
		},
		{
			name:          "ARK Animated Series with Year and Spaces",
			releaseTitle:  "ARK - The Animated Series (2024) S01E01 (2160p AMZN WEB-DL H265 SDR DDP 5.1 English - HONE)",
			expectedTitle: "ARK - The Animated Series",
			expectedClean: "arktheanimatedseries",
			expectedS:     1,
			expectedE:     1,
		},
		{
			name:          "Norway The Dark Horse (unrelated)",
			releaseTitle:  "Norway.The.Dark.Horse.S01E01.HDR.2160p.WEB.h265-EDITH",
			expectedTitle: "Norway.The.Dark.Horse",
			expectedClean: "norwaythedarkhorse",
			expectedS:     1,
			expectedE:     1,
		},
		{
			name:          "Multi-Episode Release S01E01-E02",
			releaseTitle:  "Peppa.Pig.S06E06E08.Parking.Ticket.1080p.AMZN.WEB-DL.DDP5.1.H.264-tobias",
			expectedTitle: "Peppa.Pig",
			expectedClean: "peppapig",
			expectedS:     6,
			expectedE:     6,
		},
		{
			name:          "Season Pack S01",
			releaseTitle:  "The.Ark.S01.1080p.WEB-DL.DDP5.1.H.264-NTb",
			expectedTitle: "The.Ark",
			expectedClean: "ark",
			expectedS:     1,
			expectedE:     0,
			expectedPack:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := ParseReleaseTitle(tt.releaseTitle)
			assert.Equal(t, tt.expectedClean, parsed.CleanTitle)
			assert.Equal(t, tt.expectedS, parsed.Season)
			if !tt.expectedPack {
				assert.Equal(t, tt.expectedE, parsed.EpisodeStart)
			} else {
				assert.True(t, parsed.IsSeasonPack)
			}
		})
	}
}

func TestMatchesSeries(t *testing.T) {
	t.Run("Accepts genuine The Ark releases for The Ark search", func(t *testing.T) {
		assert.True(t, MatchesSeries("The.Ark.S01E01.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb", "The Ark", 1, 1, 2023))
		assert.True(t, MatchesSeries("The.Ark.2023.S01E01.2160p.WEB-DL.DDP5.1.HDR.H.265-FLUX", "The Ark", 1, 1, 2023))
		assert.True(t, MatchesSeries("The Ark S01E01 1080p", "The Ark", 1, 1, 2023))
	})

	t.Run("Rejects ARK The Animated Series when searching for The Ark", func(t *testing.T) {
		assert.False(t, MatchesSeries("ARK.The.Animated.Series.S01E01.Element.1.2160p.AMZN.WEB-DL.DDP5.1.HEVC-NTb", "The Ark", 1, 1, 2023))
		assert.False(t, MatchesSeries("ARK - The Animated Series (2024) S01E01 (2160p AMZN WEB-DL H265 SDR DDP 5.1 English - HONE)", "The Ark", 1, 1, 2023))
	})

	t.Run("Rejects Norway The Dark Horse when searching for The Ark", func(t *testing.T) {
		assert.False(t, MatchesSeries("Norway.The.Dark.Horse.S01E01.HDR.2160p.WEB.h265-EDITH", "The Ark", 1, 1, 2023))
	})

	t.Run("Rejects wrong episode", func(t *testing.T) {
		assert.False(t, MatchesSeries("The.Ark.S01E02.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb", "The Ark", 1, 1, 2023))
	})

	t.Run("Accepts I Will Find You releases", func(t *testing.T) {
		assert.True(t, MatchesSeries("I.Will.Find.You.S01E01.1080p.NF.WEB-DL.DDP5.1.Atmos.H.264-FLUX", "I Will Find You", 1, 1, 2026))
		assert.True(t, MatchesSeries("I.Will.Find.You.S01E01.1080p.NF.WEB-DL.DD+5.1.Atmos.H.264-playWEB", "I Will Find You", 1, 1, 2026))
	})
	t.Run("Requires explicit aliases for creator-prefixed titles", func(t *testing.T) {
		assert.False(t, MatchesSeries("I.Will.Find.You.S01E01.1080p.NF.WEB-DL.DDP5.1.Atmos.H.264-FLUX", "Harlan Coben's I Will Find You", 1, 1, 2026))
		assert.False(t, MatchesSeries("Harlan.Cobens.I.Will.Find.You.S01E01.1080p.NF.WEB-DL-FLUX", "I Will Find You", 1, 1, 2026))
	})
}

func TestMatchesMovie(t *testing.T) {
	t.Run("Matches Mortal Kombat II 2026", func(t *testing.T) {
		assert.True(t, MatchesMovie("Mortal.Kombat.II.2026.2160p.UHD.BluRay.x265-SURCODE", "Mortal Kombat II", 2026))
	})

	t.Run("Rejects Mortal Kombat II when searching for Mortal Kombat 1", func(t *testing.T) {
		assert.False(t, MatchesMovie("Mortal.Kombat.II.2026.2160p.UHD.BluRay.x265-SURCODE", "Mortal Kombat", 2021))
	})
}
