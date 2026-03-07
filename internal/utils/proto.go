package utils

import (
	"testing"

	protoequal "github.com/afritzler/protoequal"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func AssertProtoMessagesEqual(t *testing.T, expected proto.Message, actual proto.Message) bool {
	g := gomega.NewWithT(t)
	return g.Expect(actual).To(protoequal.ProtoEqual(expected))
}
