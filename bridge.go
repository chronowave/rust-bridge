package rust_bridge

// #cgo CFLAGS: -I/usr/share/chronowave
// #cgo LDFLAGS: -static -L/usr/share/chronowave -lssd -lm
// #include <stdlib.h>
// #include <rust.h>
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow/flight"
	nats "github.com/nats-io/nats-server/v2/server"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec"
	"github.com/chronowave/ssql/go"
)

var (
	ErrFlattenColumnedEntity = errors.New("empty column entity input")
	ErrIndexEntity           = errors.New("empty entity index")
)

func init() {
	// init Rust module
	C.init_env()
}

// / returns quick filter and index
func SplitIndexQuickFilter(data []byte) ([]byte, []byte) {
	sz := binary.LittleEndian.Uint32(data[:4]) + 4
	return data[4:sz], data[sz:]
}

func AddQuickFilter(flightName string, id uint64, data []byte) error {
	var err error
	c_ptr := C.CString(flightName)
	defer C.free(unsafe.Pointer(c_ptr))
	if e := C.add_quick_filter(c_ptr, C.ulong(id), (*C.uchar)(&data[0]), C.uint(len(data))); e != nil {
		err = errors.New(C.GoString(e))
		C.free(unsafe.Pointer(e))
	}

	return err
}

func StatementToArrowSchema(stmt *ssql.Statement, logger nats.Logger) ([]byte, error) {
	data, err := proto.Marshal(stmt)
	if err != nil {
		logger.Errorf("failed marshall SSQL statement: %v", err)
		return nil, err
	}

	// NOTE: data []byte is still owned
	ba := C.ssql_arrow_schema((*C.uchar)(&data[0]), C.uint(len(data)))
	if ba.ptr != nil {
		data = unsafe.Slice((*byte)(ba.ptr), int(ba.len))
	} else {
		logger.Noticef("converting to arrow schema statement %s returns empty data", stmt.String())
		data = nil
	}

	if ba.err != nil {
		err = errors.New(C.GoString(ba.err))
		C.free(unsafe.Pointer(ba.err))
	}

	return data, err
}

func QueryLocal(dir string, stmt *ssql.Statement, transientID uint64, transientBuf []byte, logger nats.Logger) ([]byte, error) {
	return QueryCluster(dir, stmt, nil, 0, transientID, transientBuf, logger)
}

func QueryCluster(dir string, stmt *ssql.Statement, hosts []string, local int,
	transientID uint64, transientBuf []byte, logger nats.Logger) ([]byte, error) {
	ptr := C.CString(dir)
	defer C.free(unsafe.Pointer(ptr))

	qry, err := proto.Marshal(stmt)
	if err != nil {
		logger.Errorf("failed marshall SSQL statement: %v", err)
		return nil, err
	}

	data := codec.FlattenClusterQuery(qry, hosts, local)

	bufPtr := &data[0]
	if len(transientBuf) > 0 {
		bufPtr = &transientBuf[0]
	}

	// NOTE: data []byte is still owned
	ba := C.query_cluster(ptr, (*C.uchar)(&data[0]), C.uint(len(data)), C.ulong(transientID), (*C.uchar)(bufPtr), C.uint(len(transientBuf)))
	if ba.ptr != nil {
		logger.Noticef("start to deref query cluster result buffer addr=%d, len=%d", ba.ptr, ba.len)
		data = unsafe.Slice((*byte)(ba.ptr), int(ba.len))
		logger.Noticef("done to deref query cluster result buffer addr=%d, len=%d", unsafe.Pointer(&data[0]), len(data))
	} else {
		logger.Noticef("statement %s returns empty data", stmt.String())
		data = []byte{}
	}

	if ba.err != nil {
		err = errors.New(C.GoString(ba.err))
		C.free(unsafe.Pointer(ba.err))
	}

	return data, err
}

// exchange local data
func Exchange(dir string, exchange *flight.FlightData, transientID uint64, data []byte, streaming bool, onFlightData func([]byte, []byte) bool) (*flight.FlightData, error) {
	// exchange FlightData:
	//  AppMetadata: [0] is the partition 0, or 1, ie, primary or backup
	//	DataBody: contains SSQL statement in proto binary format
	ptr := C.CString(dir)
	defer C.free(unsafe.Pointer(ptr))

	if len(exchange.AppMetadata) != 1 {
		return nil, fmt.Errorf("exchange flight AppMetadata should contain one byte, but length is %v", len(exchange.AppMetadata))
	}

	if len(exchange.DataBody) == 0 {
		return nil, fmt.Errorf("exchange flight data is missing SSQL statement")
	}

	h := cgo.NewHandle(Callback{Send: onFlightData})
	defer h.Delete()

	dataPtr := &exchange.DataBody[0]
	if len(data) > 0 {
		dataPtr = &data[0]
	}

	// NOTE: data []byte is still owned and returns ExchangeFlightData
	efd := C.exchange(
		ptr,
		C.uchar(exchange.AppMetadata[0]),
		(*C.uchar)(&exchange.DataBody[0]),
		C.uint(len(exchange.DataBody)),
		C.ulong(transientID),
		(*C.uchar)(dataPtr),
		C.uint(len(data)),
		unsafe.Pointer(&h),
		C.bool(streaming),
	)

	var (
		body   []byte
		header []byte
		err    error
	)

	if efd.header.ptr != nil {
		// use cast to avoid buffer copy as C.GoBytes(unsafe.Pointer(ba.ptr), C.int(ba.len)) does
		//header = (*[math.MaxInt32]byte)(unsafe.Pointer(efd.header.ptr))[:int(efd.header.len):int(efd.header.len)]
		header = unsafe.Slice((*byte)(efd.header.ptr), int(efd.header.len))
	}

	if efd.body.ptr != nil {
		// use cast to avoid buffer copy as C.GoBytes(unsafe.Pointer(ba.ptr), C.int(ba.len)) does
		//body = (*[math.MaxInt32]byte)(unsafe.Pointer(efd.body.ptr))[:int(efd.body.len):int(efd.body.len)]
		body = unsafe.Slice((*byte)(efd.body.ptr), int(efd.body.len))
	}

	if efd.err != nil {
		err = errors.New(C.GoString(efd.err))
		C.free(unsafe.Pointer(efd.err))
	}

	return &flight.FlightData{DataHeader: header, DataBody: body}, err
}

func BuildIndexFromColumnizedEntities(entities []*codec.ColumnedEntity) ([]byte, error) {
	data := codec.FlattenColumnizedEntities(entities)
	if len(data) == 0 {
		return nil, ErrFlattenColumnedEntity
	}

	// build index, note: data []byte is still owned
	ba := C.build_index((*C.uchar)(&data[0]), C.uint(len(data)))
	if ba.err != nil {
		err := errors.New(C.GoString(ba.err))
		C.free(unsafe.Pointer(ba.err))
		return nil, err
	} else if ba.len == C.ulong(0) {
		return nil, ErrIndexEntity
	}

	// use cast to avoid buffer copy as C.GoBytes(unsafe.Pointer(ba.ptr), C.int(ba.len)) does
	//return (*[math.MaxInt32]byte)(unsafe.Pointer(ba.ptr))[:int(ba.len):int(ba.len)], nil
	return unsafe.Slice((*byte)(ba.ptr), int(ba.len)), nil
}
