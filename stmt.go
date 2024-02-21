package frontbase

/*
#include "clib.h"
*/
import "C"
import (
	"context"
	"database/sql/driver"

	"github.com/Oops-AB/go-frontbase/prepared"
)

type stmt struct {
	dc     *Conn
	pstmt  *prepared.Stmt
	closed bool
}

func (st *stmt) Close() (err error) {
	st.closed = true
	return nil
}

func (st *stmt) Query(args []driver.Value) (driver.Rows, error) {
	sql, err := st.pstmt.Bind(args)
	if err != nil {
		return nil, err
	}

	// Execute the SQL query and return a driver.Rows iterator.
	md, err := st.dc.exec(sql, !st.dc.inTx, false)
	if err != nil {
		return nil, err
	}

	return &Rows{
		md: md,
	}, nil
}

func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	sql, err := st.pstmt.BindNamed(args)
	if err != nil {
		return nil, err
	}

	// Execute the SQL query and return a driver.Rows iterator.
	md, err := st.dc.exec(sql, !st.dc.inTx, false)
	if err != nil {
		return nil, err
	}

	return &Rows{
		md: md,
	}, nil
}

func (st *stmt) Exec(args []driver.Value) (driver.Result, error) {
	sql, err := st.pstmt.Bind(args)
	if err != nil {
		return nil, err
	}

	// Execute the SQL query and return a driver.Result.
	md, err := st.dc.exec(sql, !st.dc.inTx, false)
	if err != nil {
		return nil, err
	}
	defer C.fbcmdRelease(md)

	return driver.ResultNoRows, nil
}

func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sql, err := st.pstmt.BindNamed(args)
	if err != nil {
		return nil, err
	}

	// Execute the SQL query and return a driver.Result.
	md, err := st.dc.exec(sql, !st.dc.inTx, false)
	if err != nil {
		return nil, err
	}
	defer C.fbcmdRelease(md)

	return driver.ResultNoRows, nil
}

func (st *stmt) NumInput() int {
	return -1
}
