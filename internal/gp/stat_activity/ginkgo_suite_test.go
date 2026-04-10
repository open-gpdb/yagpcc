package stat_activity_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStatActivity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "internal/gp/stat_activity")
}
