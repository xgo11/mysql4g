package mysql4g

import (
	"database/sql"
)

import (
	"github.com/go-sql-driver/mysql"
)

func Connect(path string) *sql.DB {
	return mgr.Connect(path)
}

func GetConf(path string) (cp ConnectionParameters) {
	if c := mgr.configs.GetConf(path); c != nil {
		cp = *c
	}
	return
}

func Close(db *sql.DB) {
	if db != nil {
		var st = db.Stats()
		if st.InUse > 0 {
			return
		}
		_ = db.Close()
	}

}

func ParseMySQLError(err error) (code uint16, msg string) {
	if err != nil {
		var v interface{} = err
		if e, ok := v.(*mysql.MySQLError); ok && e != nil {
			code = e.Number
			msg = e.Message
		}
	}
	return
}
