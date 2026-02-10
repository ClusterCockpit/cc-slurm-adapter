package profiler

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

type measurement struct {
	begin    time.Time
	duration time.Duration
}

type namedMeasurement struct {
	name string
	measurement
}

var (
	total measurement

	sections map[string]*measurement
)

func Begin() {
	total.begin = time.Now()
	total.duration = time.Duration(0)

	sections = make(map[string]*measurement)
}

func End() {
	total.duration = time.Now().Sub(total.begin)
}

func SectionBegin(name string) {
	if sections == nil {
		return
	}

	sections[name] = &measurement{
		begin: time.Now(),
	}
}

func SectionEnd(name string) {
	if sections == nil {
		return
	}

	sections[name].duration += time.Now().Sub(sections[name].begin)
}

func Report() string {
	sectionsSorted := make([]*namedMeasurement, 0)

	timeSections := time.Duration(0)

	for name, section := range sections {
		timeSections += section.duration

		sectionsSorted = append(sectionsSorted, &namedMeasurement{
			name:        name,
			measurement: *section,
		})
	}

	sectionsSorted = append(sectionsSorted, &namedMeasurement{
		name:        "<other>",
		measurement: measurement{duration: total.duration - timeSections},
	})

	slices.SortFunc(sectionsSorted, func(a, b *namedMeasurement) int {
		return -cmp.Compare(a.duration, b.duration)
	})

	result := fmt.Sprintf("Total time: %.5f sec", total.duration.Seconds())
	for _, v := range sectionsSorted {
		result += fmt.Sprintf("\n- %s: %.5f sec", v.name, v.duration.Seconds())
	}
	return result
}
