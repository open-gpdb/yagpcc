package uds_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUds(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "internal/uds")
}
