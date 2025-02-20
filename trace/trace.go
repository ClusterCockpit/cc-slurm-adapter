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
func format(fmtStr string, v ...any) string {
	_, fn, line, _ := runtime.Caller(2)
	return fmt.Sprintf("[%s:%d] %s", fn, line, fmt.Sprintf(fmtStr, v...))
}

func SetLevel(level int) {
	debugLevel = level
}

func Fatal(msg string) {
	log.Fatal(format("%s", msg))
}

func Fatalf(fmtStr string, v ...any) {
	log.Fatal(format(fmtStr, v...))
}

func Error(msg string) {
	if debugLevel >= ERROR {
		log.Print(format("%s", msg))
	}
}

func Errorf(fmtStr string, v ...any) {
	if debugLevel >= ERROR {
		log.Print(format(fmtStr, v...))
	}
}

func Warn(msg string) {
	if debugLevel >= WARN {
		log.Print(format("%s", msg))
	}
}

func Warnf(fmtStr string, v ...any) {
	if debugLevel >= WARN {
		log.Print(format(fmtStr, v...))
	}
}

func Info(msg string) {
	if debugLevel >= INFO {
		log.Print(format("%s", msg))
	}
}

func Infof(fmtStr string, v ...any) {
	if debugLevel >= INFO {
		log.Print(format(fmtStr, v...))
	}
}

func Debug(msg string) {
	if debugLevel >= DEBUG {
		log.Print(format("%s", msg))
	}
}

func Debugf(fmtStr string, v ...any) {
	if debugLevel >= DEBUG {
		log.Print(format(fmtStr, v...))
	}
}
