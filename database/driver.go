package database

type Driver int

const (
	BboltDriver Driver = iota
	LeveldbDriver
)

var driverMapping = map[string]Driver{
	"bbolt": BboltDriver,
	"level": LeveldbDriver,
}

func GetDriver(driverName string) (Driver, bool) {
	driver, ok := driverMapping[driverName]
	return driver, ok
}
