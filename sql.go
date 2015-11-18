// Package tinysql 实现了一个基本的sql工具
package tinysql

import "database/sql"

// 数据库链接
var connections = map[string]*sql.DB{}

// Register 注册数据库链接
//  name:链接名称
//  driver:驱动名称
//  conn:链接字符串
//  idle:最大空闲连接数,可以使用tinysql.DefaultMaxIdleConns
func RegisterDB(name, driver, conn string, idle int) error {
	var db, err = sql.Open(driver, conn)
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(idle)
	connections[name] = db
	return nil
}

// OpenDefault 获取链接名称为default的链接
func OpenDefault() *DB {
	return Open("default")
}

// Open 获取指定名称的链接
func Open(name string) *DB {
	var db, ok = connections[name]
	if ok {
		return &DB{db, nil, true}
	}
	return nil
}
