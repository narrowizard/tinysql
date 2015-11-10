package tinysql

import (
	"fmt"
	"errors"
)

var DbEntities map[string]*DB

//  注册数据库连接
func RegisterDBConn(connName string,connString string,driver string) bool{
	fmt.Println("register db conn")
	_,ok := DbEntities[connName]
	if ok {
		fmt.Println("[tinysql] db conn already exist")
		return false
	}
	db,err := initDB(driver,connString)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	DbEntities[connName] = db
	fmt.Println(len(DbEntities))
	return true
}

//  获取数据库连接
func GetDBConn(connName string) (*DB,error){
	db,ok := DbEntities[connName]
	if ok {
		return db,nil
	}
	return nil,errors.New("[tinysql] db conn not found!")
}

func init(){
	DbEntities = make(map[string]*DB)
}