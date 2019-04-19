package mysql4g

import (
	"database/sql"
	"sync"
)

import (
	 "github.com/xgo11/stdlog"
)

type confRegistry struct {
	sync.Mutex
	registry map[string]*ConnectionParameters
}

type connection struct {
	sync.Mutex
	conf *ConnectionParameters
	db   *sql.DB
}

const (
	driverName = "mysql"
)

var log = stdlog.Std

type connectorManager struct {
	sync.Mutex

	configs     *confRegistry
	connections map[string]*connection
}

var mgr = &connectorManager{configs: &confRegistry{}}

func (r *confRegistry) GetConf(path string) *ConnectionParameters {
	r.Lock()
	defer r.Unlock()

	if r.registry == nil {
		r.registry = make(map[string]*ConnectionParameters)
	}

	path = fulfillPath(path)
	if c, ok := r.registry[path]; ok {
		return c
	}

	if c, err := NewConnectionParameters(path); err == nil {
		log.Debugf("Load connection config ok, %v", c.String())
		r.registry[c.Path()] = &c
		return &c
	} else {
		log.Errorf("Load connection config fail, path=%s, err=%v", path, err)
		return nil
	}
}

func (m *connectorManager) Connect(path string) *sql.DB {

	conf := m.configs.GetConf(path)
	if conf == nil {
		return nil
	}

	m.Lock()
	if m.connections == nil {
		m.connections = make(map[string]*connection)
	}
	var c = m.connections[conf.Path()]
	if c == nil {
		c = &connection{conf: conf}
		m.connections[conf.Path()] = c
	}
	m.Unlock()

	return c.Connect()
}

func (c *connection) Connect() *sql.DB {
	c.Lock()
	defer c.Unlock()

	var reOpen bool
	if c.db == nil {
		reOpen = true
	} else if err := c.db.Ping(); err != nil {
		reOpen = true
	}

	if reOpen {
		if db, err := sql.Open(driverName, c.conf.BuildConnectionString()); err != nil {
			log.Errorf("connect %v fail, conf=%v, err=%v", driverName, c.conf, err)
			c.db = nil
		} else {
			c.db = db
			log.Debugf("mysql connect, %v", c.conf)
		}
	}

	return c.db
}
