package frontbase

/*
#include "clib.h"
*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"
	"unsafe"
)

type Rows struct {
	md *C.FBCMetaData
}

func (rows *Rows) Next(dest []driver.Value) error {
	row := C.fbcmdFetchRow(rows.md)
	if row == nil {
		return io.EOF
	}

	for i := range dest {
		col := C.MyFBColumnAtIndex(row, C.uint(i))
		if col == nil {
			return fmt.Errorf("no col at index %v", i)
		}

		cmd := C.fbcmdColumnMetaDataAtIndex(rows.md, C.uint(i))
		dataType := C.fbccmdDatatype(cmd)
		dtc := C.fbcdmdDatatypeCode(dataType)

		switch dtc {
		case C.FB_Boolean:
			dest[i] = C.MyFBColumnValueBool(col)
		case C.FB_TinyInteger:
			dest[i] = C.MyFBColumnValueTinyInt(col)
		case C.FB_SmallInteger:
			dest[i] = C.MyFBColumnValueSmallInt(col)
		case C.FB_Integer:
			dest[i] = C.MyFBColumnValueInt(col)
		case C.FB_LongInteger:
			dest[i] = C.MyFBColumnValueLongInt(col)
		case C.FB_TimestampTZ:
			fallthrough
		case C.FB_Timestamp:
			tval := C.struct_MyFBTimestampValue{
				secs:  0,
				nsecs: 0,
			}
			C.MyFBColumnValueTimestamp(col, &tval)
			dest[i] = time.Unix(int64(tval.secs), int64(tval.nsecs))
		case C.FB_Character:
			fallthrough
		case C.FB_VCharacter:
			dest[i] = C.GoString(C.MyFBColumnValueChar(col))
		case C.FB_Bit:
			fallthrough
		case C.FB_VBit:
			dest[i] = C.GoBytes(unsafe.Pointer(C.MyFBColumnValueBit(col)), C.MyFBColumnSizeBit(col))
		default:
			return fmt.Errorf("unsupported dtc %v", dtc)
		}
	}

	return nil
}

func (rows *Rows) Columns() []string {
	numCols := C.fbcmdColumnCount(rows.md)
	cols := make([]string, numCols)

	for i := C.uint(0); i < numCols; i++ {
		cmd := C.fbcmdColumnMetaDataAtIndex(rows.md, i)
		cols[i] = C.GoString(C.fbccmdLabelName(cmd))
	}

	return cols
}

func (rows *Rows) Close() error {
	if rows.md != nil {
		C.fbcmdRelease(rows.md)
		rows.md = nil
		return nil
	} else {
		return fmt.Errorf("Rows iterator already closed")
	}
}
