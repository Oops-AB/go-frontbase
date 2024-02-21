package frontbase

/*
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

	"github.com/Oops-AB/go-frontbase/prepared"
)

//
// The connection
//

type Conn struct {
	conn *C.FBCDatabaseConnection
	inTx bool
}

func (dc *Conn) setUTC() error {
	md, err := dc.exec("SET TIME ZONE 'UTC';", true, false)
	if err != nil {
		return err
	}
	C.fbcmdRelease(md)
	return nil
}

func (dc *Conn) Begin() (driver.Tx, error) {
	return dc.BeginTx(context.Background(), driver.TxOptions{})
}

func (dc *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var isolation string

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelReadUncommitted:
		isolation = "read uncommitted, locking optimistic"

	case sql.LevelReadCommitted:
		isolation = "read committed, locking optimistic"

	case sql.LevelWriteCommitted:
		isolation = "write committed, locking optimistic"

	case sql.LevelSerializable:
		isolation = "serializable, locking pessimistic"

	case sql.LevelRepeatableRead:
		fallthrough

	case sql.LevelDefault:
		isolation = "repeatable read, locking optimistic"

	case sql.LevelSnapshot:
		return nil, fmt.Errorf("unsupported isolation level LevelSnapshot")

	case sql.LevelLinearizable:
		return nil, fmt.Errorf("unsupported isolation level LevelLinearizable")

	default:
		return nil, fmt.Errorf("unsupported isolation level %d", opts.Isolation)
	}

	readOrWrite := "read write"

	if opts.ReadOnly {
		readOrWrite = "read only"
	}

	query := fmt.Sprintf("set transaction isolation level %s, %s;", isolation, readOrWrite)
	md, err := dc.exec(query, true, false)
	if err != nil {
		return nil, err
	}
	defer C.fbcmdRelease(md)

	dc.inTx = true
	return Tx{
		dc: dc,
	}, nil
}

func (dc *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepped, err := prepared.ParseSQL(query)
	if err != nil {
		return nil, err
	}

	return &stmt{
		dc:     dc,
		closed: false,
		pstmt:  prepped,
	}, nil
}

func (dc *Conn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(context.Background(), query)
}

func (dc *Conn) Close() error {
	C.MyFBClose(dc.conn)
	dc.conn = nil
	runtime.SetFinalizer(dc, nil)
	return nil
}

//
// The transaction
//

type Tx struct {
	dc *Conn
}

func (tx Tx) Commit() error {
	return tx.dc.commit()
}
func (tx Tx) Rollback() error {
	return tx.dc.rollback()
}

func (dc *Conn) commit() error {
	query := fmt.Sprintf("commit;")
	md, err := dc.exec(query, true, false)
	if err != nil {
		return err
	}
	defer C.fbcmdRelease(md)

	dc.inTx = false
	return nil
}

func (dc *Conn) rollback() error {
	query := fmt.Sprintf("rollback;")
	md, err := dc.exec(query, true, false)
	if err != nil {
		return err
	}
	defer C.fbcmdRelease(md)

	dc.inTx = false
	return nil
}

//
// Optional
//

// Pinger
func (dc *Conn) Ping(ctx context.Context) error {
	if C.MyFBPing(dc.conn) == 0 {
		return driver.ErrBadConn
	}
	return nil
}

// SessionResetter
func (dc *Conn) ResetSession(ctx context.Context) error {
	return nil
}

// Validator
func (dc *Conn) IsValid() bool {
	return true
}

//
// Internal
//

func (dc *Conn) exec(sql string, commit bool, warn bool) (*C.FBCMetaData, error) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	clen := uint(len(sql))

	var commitFlags uint = 0
	if commit {
		commitFlags = 2 // FBCDCCommit
	}

	md := C.fbcdcExecuteSQL(dc.conn, csql, C.uint(clen), C.uint(commitFlags))

	if md == nil && C.fbcdcConnected(dc.conn) == 0 {
		C.MyFBClose(dc.conn)
		return nil, fmt.Errorf("conn %p: no database connection", dc)
	}

	if C.fbcmdErrorsFound(md) != 0 {
		defer C.fbcmdRelease(md)

		emd := C.fbcmdErrorMetaData(md)
		defer C.fbcemdRelease(emd)

		all := C.fbcemdAllErrorMessages(emd)
		defer C.fbcemdReleaseMessage(all)

		msg := C.GoString(all)
		return nil, fmt.Errorf("conn %p: execute SQL failed:\n%v", dc, msg)
	}

	return md, nil
}
