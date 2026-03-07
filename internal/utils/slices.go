package utils

func NumberOfUniqueSlices(slices []int64) int64 {
	if slices == nil {
		return 0
	}
	slicesMap := make(map[int64]bool, 0)
	for _, slice := range slices {
		slicesMap[slice] = true
	}
	return int64(len(slicesMap))
}
