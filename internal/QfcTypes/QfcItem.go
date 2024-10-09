package QfcTypes

import "time"

type QfcItem struct {
	LocalId        int32
	Description    string
	QfcUrl         string
	SoldByWeight   bool
	AddedTimestamp time.Time
}
