package gp

import (
	"context"
	"strings"
)

func IsCloudberry(ctx context.Context) (bool, error) {
	version, err := GetVersion(ctx)
	if err != nil {
		return false, err
	}
	if strings.Contains(version.Version, "loudberry") {
		return true, nil
	}
	return false, nil
}
