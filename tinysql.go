package tinysql

import (
	"os"
	"fmt"
)

var DbEntity *DB

func init(){
	var err error
	loadConfig()
	DbEntity,err = initDB()
	if err != nil{
		fmt.Printf("db init failed")
		os.Exit(2)
	}
}