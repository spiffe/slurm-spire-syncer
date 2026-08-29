package slurm

import (
	"strings"
	"testing"
)

func TestExpandHostList(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"single host", "node01", []string{"node01"}},
		{"plain comma list", "a,b,c", []string{"a", "b", "c"}},
		{"simple range", "node[1-4]", []string{"node1", "node2", "node3", "node4"}},
		{"single element range", "node[3-3]", []string{"node3"}},
		{"zero padded range", "node[01-04]", []string{"node01", "node02", "node03", "node04"}},
		{
			"padding widens across boundary",
			"node[08-12]",
			[]string{"node08", "node09", "node10", "node11", "node12"},
		},
		{
			"unpadded low bound leaves wider values alone",
			"node[8-12]",
			[]string{"node8", "node9", "node10", "node11", "node12"},
		},
		{"singleton inside brackets", "node[7]", []string{"node7"}},
		{
			"mixed ranges and singletons",
			"node[01-04,07,10-12]",
			[]string{"node01", "node02", "node03", "node04", "node07", "node10", "node11", "node12"},
		},
		{
			"commas inside brackets are not top level",
			"a,b[1-3],c",
			[]string{"a", "b1", "b2", "b3", "c"},
		},
		{"suffix after bracket", "node[1-2]-ib", []string{"node1-ib", "node2-ib"}},
		{"empty prefix", "[1-2]", []string{"1", "2"}},
		{"surrounding whitespace", "  node[1-2] ", []string{"node1", "node2"}},
		{"empty elements are skipped", "a,,b", []string{"a", "b"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExpandHostList(tc.in)
			if err != nil {
				t.Fatalf("ExpandHostList(%q) returned error: %v", tc.in, err)
			}
			assertStrings(t, got, tc.want)
		})
	}
}

func TestExpandHostListErrors(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantSub string
	}{
		{"unmatched open bracket", "node[1-2", "unmatched '['"},
		{"unmatched close bracket", "node1-2]", "unmatched ']'"},
		{"two bracket groups", "rack[1-2]node[1-4]", "more than one bracket group"},
		{"empty bracket group", "node[]", "empty bracket group"},
		{"empty range component", "node[1,,2]", "empty range component"},
		{"non numeric singleton", "node[abc]", "is not a number"},
		{"non numeric range low", "node[a-3]", "is not a number"},
		{"non numeric range high", "node[1-z]", "is not a number"},
		{"inverted range", "node[9-2]", "inverted"},
		{"range too large", "node[1-99999999]", "more than"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExpandHostList(tc.in)
			if err == nil {
				t.Fatalf("ExpandHostList(%q) = %v, want an error", tc.in, got)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("ExpandHostList(%q) error = %q, want it to contain %q", tc.in, err, tc.wantSub)
			}
		})
	}
}

// A hostlist that is individually small but collectively enormous must still be
// rejected, otherwise the per-element check alone could be walked around.
func TestExpandHostListTotalLimit(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 64; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("node[1-100000]")
	}
	if _, err := ExpandHostList(b.String()); err == nil {
		t.Fatal("expected an error for a hostlist exceeding the total limit")
	}
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries %v, want %d entries %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q (full result %v)", i, got[i], want[i], got)
		}
	}
}
