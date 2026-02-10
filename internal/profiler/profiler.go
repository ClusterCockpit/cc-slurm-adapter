package profiler

import (
	"fmt"
	"time"
)

type measurement struct {
	begin    time.Time
	duration time.Duration
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
	result := fmt.Sprintf("Total time: %.5f sec", total.duration.Seconds())

	timeSections := time.Duration(0)

	for name, section := range sections {
		timeSections += section.duration
		result += fmt.Sprintf("\n- %s: %.5f sec", name, section.duration.Seconds())
	}

	result += fmt.Sprintf("\n- <other>: %.5f sec", (total.duration - timeSections).Seconds())
	return result
}
