package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigStorage(t *testing.T) {
	SetHostnameForSegindex(1, "hostname1")
	fqdn1 := GetHostnameForSegindex(1)
	assert.Equal(t, fqdn1, "hostname1")
	fqdn2 := GetHostnameForSegindex(2)
	assert.Equal(t, fqdn2, "2")
}
