package frontbase

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
	name   string
	driver *Driver
}

func (cnct Connector) Connect(context.Context) (driver.Conn, error) {
	return cnct.driver.open(cnct.name)
}

func (cnct Connector) Driver() *Driver {
	return cnct.driver
}

func (cnct Connector) Close() error {
	return nil
}
