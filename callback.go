package rust_bridge

//go:generate go tool cgo callback/callback.go

type Callback struct {
	// Send should return true on failure, false otherwise
	Send func(header, body []byte) bool
}
