package assert

func True(value bool, message string) {
	if !value {
		panic(message)
	}
}
