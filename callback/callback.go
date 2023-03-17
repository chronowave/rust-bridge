package main

import "C"

import (
	"runtime/cgo"
	"unsafe"

	"github.com/chronowave/rust-bridge"
)

// note: go generate uses this file to generate _obj_export.h header file. This function is required to be
// presented in every main function to satisfy go linker
//
//export send_to_go
func send_to_go(f unsafe.Pointer, hsz C.int, header *C.uchar, bsz C.int, body *C.uchar) bool {
	h := *(*cgo.Handle)(f)
	return h.Value().(rust_bridge.Callback).Send(unsafe.Slice((*byte)(header), int(hsz)), unsafe.Slice((*byte)(body), int(bsz)))
}

func main() {}
