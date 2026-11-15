package bundleimport

import (
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/importer/rarname"
)

// nameOrder judges the order of the parts of one file by the volume names the
// posted files carry. When every name parses as a volume of one set (old style
// .rar/.r00, new style .partNN.rar, or a numeric split .001), the volume numbers
// must strictly increase in the order of the parts. A set that does not parse at
// all (obfuscated names) says nothing here and "" is returned; a set that parses
// only in part, or into different sets, is refused because names that look like
// a set cannot be told apart from a rearranged one. It returns "" when nothing
// contradicts the order.
func nameOrder(names []string) string {
	type vol struct {
		scheme rarname.Scheme
		num    int
		key    string
	}
	vols := make([]vol, len(names))
	parsed := 0
	for i, n := range names {
		if n == "" {
			continue
		}
		scheme, num, ok := rarname.VolumeNumber(n)
		if !ok {
			continue
		}
		key, _ := rarname.SetKey(n)
		vols[i] = vol{scheme, num, key}
		parsed++
	}
	switch {
	case parsed == 0:
		return ""
	case parsed != len(names):
		return fmt.Sprintf("only %d of %d parts have volume names (%s): the order of the parts cannot be trusted", parsed, len(names), quoteNames(names))
	}
	for i := 1; i < len(vols); i++ {
		a, b := vols[i-1], vols[i]
		switch {
		case a.key != b.key || a.scheme != b.scheme:
			return fmt.Sprintf("parts %d and %d belong to different volume sets (%q, %q)", i, i+1, names[i-1], names[i])
		case b.num <= a.num:
			return fmt.Sprintf("part %d is volume %q and part %d is volume %q: the parts are not in volume order", i, names[i-1], i+1, names[i])
		}
	}
	return ""
}

func quoteNames(names []string) string {
	q := make([]string, 0, 3)
	for _, n := range names {
		if len(q) == 3 {
			q = append(q, "...")
			break
		}
		q = append(q, fmt.Sprintf("%q", n))
	}
	return strings.Join(q, ", ")
}
