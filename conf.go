package mysql4g

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

import (
	"github.com/xgo11/configuration"
	"github.com/xgo11/datetime"
	"github.com/xgo11/env"
)

const (
	prefixPath = "db/mysql"
)

type ConnectionParameters struct {
	file   string // config file
	path   string // config path
	lstmod int64  // last modify time

	Host     string            `json:"host" yaml:"host"`
	Port     int               `json:"port" yaml:"port"`
	User     string            `json:"user" yaml:"user"`
	Password string            `json:"password" yaml:"password"`
	Db       string            `json:"db" yaml:"db"`
	Params   map[string]string `json:"params" yaml:"params"`
}

func fulfillPath(path string) string {
	path = strings.Trim(path, "/")
	if strings.HasPrefix(path, prefixPath) {
		return path
	}
	return prefixPath + "/" + path
}

func NewConnectionParameters(path string) (cp ConnectionParameters, err error) {
	path = fulfillPath(path)
	file := filepath.Join(env.ConfDir(), path+".yaml")
	if err = configuration.LoadYaml(file, &cp); err != nil {
		return
	}
	if err = cp.checkValid(); err != nil {
		return
	}
	var info os.FileInfo
	if info, err = os.Stat(file); err != nil {
		return
	}
	cp.lstmod = info.ModTime().In(datetime.LocalTZ()).Unix()
	cp.file = file
	cp.path = path

	/*
		auto fill default params

		charset: utf8
		parseTime: true
		loc: Asia%2FShanghai
	*/

	if cp.Params == nil {
		cp.Params = make(map[string]string)
	}

	if _, ok := cp.Params["charset"]; !ok {
		cp.Params["charset"] = "utf8"
	}
	if _, ok := cp.Params["parseTime"]; !ok {
		cp.Params["parseTime"] = "true"
	}
	if _, ok := cp.Params["loc"]; !ok {
		cp.Params["loc"] = url.QueryEscape(datetime.TzZHName)
	}

	return

}

func (c *ConnectionParameters) checkValid() error {
	if c.Host == "" {
		return errors.New("host empty")
	}
	if c.Port == 0 {
		c.Port = 3306
	}

	if c.User == "" || c.Password == "" {
		return errors.New("authorize information missing")
	}

	if c.Db == "" {
		return errors.New("db name missing")
	}
	return nil
}

func (c *ConnectionParameters) JSON(indent ...int) string {
	var tab int
	if len(indent) > 0 {
		tab = indent[0]
	}
	if tab < 0 {
		tab = 0
	}
	if tab == 0 {
		bs, _ := json.Marshal(c)
		return string(bs)
	} else {
		bs, _ := json.MarshalIndent(c, "", strings.Repeat(" ", tab))
		return string(bs)
	}

}

func (c *ConnectionParameters) File() string {
	return c.file
}

func (c *ConnectionParameters) Path() string {
	return c.path
}

func (c *ConnectionParameters) LstMod() int64 {
	return c.lstmod
}

func (c ConnectionParameters) String() string {
	return fmt.Sprintf("<%v> %v:%v/%v@%d", c.Path(), c.Host, c.Port, c.Db, c.LstMod())
}

func (c *ConnectionParameters) BuildConnectionString() string {
	var s = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.Db)
	if len(c.Params) > 0 {
		var p string
		for k, v := range c.Params {
			p += fmt.Sprintf("&%s=%s", k, v)
		}
		s += "?" + p[1:]
	}
	return s
}
