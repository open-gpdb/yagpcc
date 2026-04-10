package gp

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "internal/gp")
}
