package tinysql

import (
	"database/sql"
	"reflect"
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
func (this *DB) Begin() bool {
	var err error
	this.tx, err = this.db.Begin()
	this.autoCommit = false
	if err != nil {
		return false
	}
	return true
}

// Commit 提交事务
func (this *DB) Commit() error {
	this.autoCommit = true
	return this.tx.Commit()
}

// Rollback 回滚失误
func (this *DB) Rollback() error {
	this.autoCommit = true
	return this.tx.Rollback()
}

//向指定table插入数据
func (this *DB) Insert(table string, model interface{}) int64 {
	query := "insert into " + table
	value := reflect.ValueOf(model).Elem()
	data := make(map[string]interface{})
	mapStructToMap(value, data)
	keys := " ("
	values := " ("
	params := make([]interface{}, 0, 0)
	for k, v := range data {
		keys += k + ","
		values += "?,"
		params = append(params, v)
	}
	query += keys[:len(keys)-1] + ") values"
	query += values[:len(values)-1] + ")"
	var result sql.Result
	var err error
	result, err = this.Exec(query, params...)
	if err != nil {
		return -1
	}
	var id int64
	id, err = result.LastInsertId()
	if err != nil {
		return -1
	}
	return id
}
