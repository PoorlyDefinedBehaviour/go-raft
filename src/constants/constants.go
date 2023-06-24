package constants

import "os"

var (
	Debug = os.Getenv("DEBUG") != ""
)
