package trace

import (
	"fmt"
	"log"
	"runtime"
)

const (
	FATAL int = 0
	ERROR     = 1
	WARN      = 2
	INFO      = 3
	DEBUG     = 4
)

var (
	debugLevel int = 2
)

// It's important this function is called at the exact call depth
func format(logLevelPrefix string, fmtStr string, v ...any) string {
	_, fn, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s [%s:%d] %s", logLevelPrefix, fn, line, fmt.Sprintf(fmtStr, v...))
}

func SetLevel(level int) {
	debugLevel = level
}

func Fatal(fmtStr string, v ...any) {
	log.Fatal(format("FATAL", fmtStr, v...))
}

func Error(fmtStr string, v ...any) {
	if debugLevel >= ERROR {
		log.Print(format("ERROR", fmtStr, v...))
	}
}

func Warn(fmtStr string, v ...any) {
	if debugLevel >= WARN {
		log.Print(format("WARN ", fmtStr, v...))
	}
}

func Info(fmtStr string, v ...any) {
	if debugLevel >= INFO {
		log.Print(format("INFO ", fmtStr, v...))
	}
}

func Debug(fmtStr string, v ...any) {
	if debugLevel >= DEBUG {
		log.Print(format("DEBUG", fmtStr, v...))
	}
}
