package tinysql

import (
	"database/sql"
	"errors"
)

// 数据库链接
type DB struct {
	db         *sql.DB
	tx         *sql.Tx
	autoCommit bool
}

func (this *DB) NewBuilder() *builder {
	var b = new(builder)
	b.reset()
	b.db = this
	return b
}

// Call 调用存储过程
func (this *DB) Call(procedure string, params ...interface{}) *Rows {
	var sql = "call " + procedure + "("
	for i := 0; i < len(params); i++ {
		sql += "?,"
	}
	if len(params) != 0 {
		sql = sql[:len(sql)-1]
	}
	sql += ")"
	var rows, err = this.db.Query(sql, params...)
	return &Rows{rows, err, nil}
}

// Query 查询sql
func (this *DB) Query(sql string, params ...interface{}) *Rows {
	var rows, err = this.db.Query(sql, params...)
	return &Rows{rows, err, nil}
}

// Exec 执行sql
func (this *DB) Exec(sql string, params ...interface{}) (sql.Result, error) {
	if this.autoCommit {
		return this.db.Exec(sql, params...)
	} else {
		return this.tx.Exec(sql, params...)
	}
}

// Begin 开始事务
func (this *DB) begin() bool {
	var err error
	this.tx, err = this.db.Begin()
	this.autoCommit = false
	if err != nil {
		return false
	}
	return true
}

// Commit 提交事务
func (this *DB) commit() error {
	if this.autoCommit == true {
		return errors.New("there's no transaction begun")
	}
	this.autoCommit = true
	return this.tx.Commit()
}

// Rollback 回滚事务
func (this *DB) rollback() error {
	if this.autoCommit == true {
		return errors.New("this's no transaction begun")
	}
	this.autoCommit = true
	return this.tx.Rollback()
}
