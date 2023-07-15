package logging

import (
	"log"
	"time"
)

const (
	YYYYMMDD  = "2006:01:02"
	HHMMSS24h = "15:04:05.0000"
)

func Trace(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][TRACE]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}

func Debug(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][DEBUG]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}

func Info(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][INFO]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}

func Warning(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][WARNING]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}

func Error(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][ERROR]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}

func Fatal(a ...interface{}) {
	datetime := "[" + time.Now().UTC().Format(YYYYMMDD+"]["+HHMMSS24h) + "][FATAL]:"
	log.SetFlags(0)
	log.SetPrefix(datetime)
	log.Println(a...)
}
