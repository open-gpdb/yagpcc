package master_sentinel_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMasterSentinel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "internal/gp/master_sentinel")
}
