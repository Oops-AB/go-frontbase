package frontbase

/*
#cgo pkg-config: FBCAccess
#include "clib.h"
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"unsafe"
)

type Driver struct {
}

func (drv *Driver) OpenConnector(name string) (Connector, error) {
	return Connector{
		name:   name,
		driver: drv,
	}, nil
}

func (drv *Driver) Open(name string) (driver.Conn, error) {
	connector, err := drv.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

func (drv *Driver) open(name string) (driver.Conn, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	conn := C.GoFBOpen(cname)
	if conn == nil {
		return nil, fmt.Errorf("drv %p: unable to open connection to '%s'", drv, name)
	}

	var newDrvConn = &Conn{
		conn: conn,
	}

	if err := newDrvConn.setUTC(); err != nil {
		newDrvConn.Close()
		return nil, err
	}

	_, file, line, _ := runtime.Caller(1)
	runtime.SetFinalizer(newDrvConn, func(dc *Conn) {
		panic(fmt.Sprintf("%v: %s:%d: open connection never closed", dc, file, line))
	})

	return newDrvConn, nil
}

//
// Module Initialization
//

func init() {
	sql.Register("frontbase", &Driver{})
}
