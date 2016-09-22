package main

import (
	"fmt"
	"math/rand"
	"testing"
)

var result string
var resultB []byte

func BenchmarkKey_StringInterpolation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := rand.Int63n(9000000000000000000)
		b := fmt.Sprintf("%v", r)
		result = fmt.Sprintf("%s_%s", "user", b)
	}
}

func BenchmarkKey_StringConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := rand.Int63n(9000000000000000000)
		b := fmt.Sprintf("%v", r)
		result = concatStr("user", "_", b)
	}
}

func BenchmarkKey_ByteAppending(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := rand.Int63n(9000000000000000000)
		b := fmt.Sprintf("%v", r)
		resultB = concat([]byte("user"), []byte("_"), []byte(b))
	}
}

func concat(args ...[]byte) (buff []byte) {
	for _, arg := range args {
		buff = append(buff, arg...)
	}
	return
}

func concatStr(args ...string) (res string) {
	for _, arg := range args {
		res += arg
	}
	return res
}
