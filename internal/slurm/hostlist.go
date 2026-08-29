package slurm

import (
	"fmt"
	"strconv"
	"strings"
)

// maxExpandedHosts bounds the result of a single ExpandHostList call. A
// malformed or hostile range such as node[1-99999999] would otherwise allocate
// without limit; the syncer parses squeue output on a timer, so an unbounded
// allocation here would be a durable failure rather than a transient one.
const maxExpandedHosts = 1 << 20

// ExpandHostList expands a Slurm hostlist into individual node names.
//
// Supported forms:
//
//	node1                  -> node1
//	node[1-4]              -> node1 node2 node3 node4
//	node[01-04]            -> node01 node02 node03 node04   (padding from the low bound)
//	node[1-2,7]            -> node1 node2 node7
//	node[1-2]-ib           -> node1-ib node2-ib
//	a,b[1-3],c             -> a b1 b2 b3 c
//
// Slurm also permits several bracket groups in one element (rack[1-2]node[1-4]).
// That form is rejected explicitly rather than guessed at: this function is only
// a fallback for when squeue does not report an already-expanded allocation
// list, and silently producing the wrong node names would create registration
// entries whose parent IDs point at agents that do not exist.
func ExpandHostList(list string) ([]string, error) {
	list = strings.TrimSpace(list)
	if list == "" {
		return nil, nil
	}

	elements, err := splitTopLevel(list)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, el := range elements {
		hosts, err := expandElement(el)
		if err != nil {
			return nil, err
		}
		if len(out)+len(hosts) > maxExpandedHosts {
			return nil, fmt.Errorf("hostlist %q expands to more than %d hosts", list, maxExpandedHosts)
		}
		out = append(out, hosts...)
	}
	return out, nil
}

// splitTopLevel splits on commas that are not inside a bracket group, so that
// "a,b[1,3],c" yields three elements rather than four.
func splitTopLevel(list string) ([]string, error) {
	var (
		elements []string
		current  strings.Builder
		depth    int
	)
	for _, r := range list {
		switch r {
		case '[':
			depth++
			current.WriteRune(r)
		case ']':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("hostlist %q has an unmatched ']'", list)
			}
			current.WriteRune(r)
		case ',':
			if depth == 0 {
				elements = append(elements, current.String())
				current.Reset()
				continue
			}
			current.WriteRune(r)
		default:
			current.WriteRune(r)
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("hostlist %q has an unmatched '['", list)
	}
	elements = append(elements, current.String())

	out := make([]string, 0, len(elements))
	for _, el := range elements {
		if el = strings.TrimSpace(el); el != "" {
			out = append(out, el)
		}
	}
	return out, nil
}

func expandElement(el string) ([]string, error) {
	open := strings.IndexByte(el, '[')
	if open < 0 {
		if strings.IndexByte(el, ']') >= 0 {
			return nil, fmt.Errorf("hostlist element %q has an unmatched ']'", el)
		}
		return []string{el}, nil
	}

	closeIdx := strings.IndexByte(el, ']')
	if closeIdx < open {
		return nil, fmt.Errorf("hostlist element %q has an unmatched '['", el)
	}

	prefix, inner, suffix := el[:open], el[open+1:closeIdx], el[closeIdx+1:]
	if strings.ContainsAny(suffix, "[]") {
		return nil, fmt.Errorf("hostlist element %q has more than one bracket group, which is not supported", el)
	}
	if strings.TrimSpace(inner) == "" {
		return nil, fmt.Errorf("hostlist element %q has an empty bracket group", el)
	}

	var out []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("hostlist element %q has an empty range component", el)
		}

		lowStr, highStr, isRange := strings.Cut(part, "-")
		if !isRange {
			if _, err := strconv.ParseUint(part, 10, 64); err != nil {
				return nil, fmt.Errorf("hostlist element %q: %q is not a number", el, part)
			}
			out = append(out, prefix+part+suffix)
			continue
		}

		low, err := strconv.ParseUint(lowStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("hostlist element %q: %q is not a number", el, lowStr)
		}
		high, err := strconv.ParseUint(highStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("hostlist element %q: %q is not a number", el, highStr)
		}
		if high < low {
			return nil, fmt.Errorf("hostlist element %q: range %s-%s is inverted", el, lowStr, highStr)
		}
		if high-low+1 > maxExpandedHosts {
			return nil, fmt.Errorf("hostlist element %q expands to more than %d hosts", el, maxExpandedHosts)
		}

		// Slurm pads to the width of the low bound, so node[01-10] yields
		// node01..node10 while node[1-10] yields node1..node10. Using the low
		// bound's width unconditionally gives both, since %0*d never truncates.
		width := len(lowStr)
		for n := low; n <= high; n++ {
			out = append(out, fmt.Sprintf("%s%0*d%s", prefix, width, n, suffix))
		}
	}
	return out, nil
}
