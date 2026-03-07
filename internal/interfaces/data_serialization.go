package interfaces

type (
	DataSerialization interface {
		ToJSON() ([]byte, error)
	}
)
