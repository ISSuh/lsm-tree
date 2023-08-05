/*
MIT License

Copyright (c) 2023 ISSuh

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

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
