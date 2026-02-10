package profiler

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/trace"
)

type measurement struct {
	name  string
	count int

	begin time.Time
	total time.Duration

	children map[string]*measurement
	parent   *measurement
}

var (
	root       measurement
	curSection *measurement
)

func Begin() {
	root.name = "Total"

	root.begin = time.Now()
	root.total = time.Duration(0)

	root.children = make(map[string]*measurement)
	root.parent = nil
	curSection = &root
}

func End() {
	root.total = time.Now().Sub(root.begin)
}

func SectionBegin(name string) {
	if curSection == nil {
		return
	}

	if curSection.children[name] == nil {
		curSection.children[name] = &measurement{
			name:     name,
			total:    time.Duration(0),
			children: make(map[string]*measurement),
			parent:   curSection,
		}
	}

	curSection.children[name].begin = time.Now()
	curSection = curSection.children[name]
}

func SectionEnd(name string) {
	if curSection == nil {
		return
	}

	if curSection.name != name {
		trace.Fatal("Profiler section ended with '%s', but started with '%s'", name, curSection.name)
	}

	curSection.total += time.Now().Sub(curSection.begin)
	curSection.count += 1

	if curSection.parent == nil {
		trace.Fatal("Forcing SectionEnd on root section 'Total' is not allowed")
	}

	curSection = curSection.parent
}

func reportMeasurement(m *measurement, indent string) string {
	childrenSorted := make([]*measurement, 0)
	childrenTotal := time.Duration(0)

	for _, child := range m.children {
		childrenTotal += child.total
		childrenSorted = append(childrenSorted, child)
	}

	if len(m.children) > 0 {
		childrenSorted = append(childrenSorted, &measurement{
			name:   "<other>",
			total:  m.total - childrenTotal,
			parent: m,
		})
	}

	slices.SortFunc(childrenSorted, func(a, b *measurement) int {
		return -cmp.Compare(a.total, b.total)
	})

	callCountString := ""
	if m.count > 0 {
		callCountString = fmt.Sprintf("%d calls, ", m.count)
	}

	result := fmt.Sprintf("\n%s- %s: %s%.5f sec", indent, m.name, callCountString, m.total.Seconds())
	if len(childrenSorted) > 0 {
		for _, child := range childrenSorted {
			result += reportMeasurement(child, indent+"  ")
		}
	}
	return result
}

func Report() string {
	return reportMeasurement(&root, "")
}
