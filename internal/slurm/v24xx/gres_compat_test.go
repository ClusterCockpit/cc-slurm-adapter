package slurm_v24xx

import (
	"reflect"
	"testing"
)

func TestParseGRESWithAndWithoutModel(t *testing.T) {
	for _, tc := range []struct {
		text, model string
		count       uint64
		ids         []int
	}{
		{"gpu:1(IDX:0)", "", 1, []int{0}},
		{"gpu:v100:1(IDX:0)", "v100", 1, []int{0}},
		{"gpu:h100:4(IDX:0-3)", "h100", 4, []int{0, 1, 2, 3}},
		{"gpu:2(IDX:1,3)", "", 2, []int{1, 3}},
	} {
		got, err := ParseGRES(tc.text)
		if err != nil {
			t.Fatal(err)
		}
		if got.Variant != "gpu" || got.Id != tc.model || got.Count != tc.count || !reflect.DeepEqual(got.DomainIndices, tc.ids) {
			t.Fatalf("%s: %#v", tc.text, got)
		}
	}
	for _, bad := range []string{"", "gpu:bogus(IDX:0)", "gpu:1(IDX:x)"} {
		if _, err := ParseGRES(bad); err == nil {
			t.Fatalf("accepted malformed GRES %q", bad)
		}
	}
}
