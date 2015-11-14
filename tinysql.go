package tinysql

import (
	"database/sql"
	"strconv"
	"fmt"
	"errors"
)

var DbEntities map[string]*sql.DB

type DBConnConfig struct{
	UseConnString bool
	Username string
	Password string
	Host string
	Port int
	ConnString string
	Charset string
	Driver string
	Database string
	ParseTime bool
}

func NewDBConnConfig() DBConnConfig{
	var instance DBConnConfig
	instance.Charset = "utf-8"
	instance.Driver = "mysql"
	instance.Port = 3306
	instance.UseConnString = false
	instance.ParseTime = false
	return instance
}

//  注册数据库连接
func RegisterDBConn(connName string,config DBConnConfig) bool{
	var connectionString string
	if config.UseConnString{
		connectionString = config.ConnString
	}else{
		connectionString = config.Username + ":" + config.Password + "@tcp(" + config.Host + ":" + strconv.Itoa(config.Port) + ")/" + config.Database + "?charset=" + config.Charset + "&parseTime=" + strconv.FormatBool(config.ParseTime)
	}
	_,ok := DbEntities[connName]
	if ok {
		fmt.Println("[tinysql] db conn already exist")
		return false
	}
	db,err := initDB(config.Driver,connectionString)
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
		var entity = new(DB)
		entity.reset()
		entity.DB = db
		return entity,nil
	}
	return nil,errors.New("[tinysql] db conn not found!")
}

func init(){
	DbEntities = make(map[string]*sql.DB)
}

//初始化DB对象，DB是一个支持多线程，线程安全的对象
func initDB(driver string,connString string) (*sql.DB,error){
	db,err := sql.Open(driver,connString)
	if(err != nil){
		fmt.Printf("database open failed.\n")
		return nil,err
	}
	err = db.Ping()
	if err != nil{
		fmt.Printf("database ping failed.\n")
		return nil,err
	}
	return db,nil
}
