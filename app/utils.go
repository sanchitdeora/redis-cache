package main

import (
	"math/rand"
	"time"
)

const alphaNumericCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func GenerateAlphaNumericString(length int) string {
	b := make([]byte, length)

	for i := range b {
	b[i] = alphaNumericCharset[seededRand.Intn(len(alphaNumericCharset))]
	}
  return string(b)
}