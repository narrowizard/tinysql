package tinysql

import(
	"fmt"
	"database/sql"
)

type IDbHelper interface{
	//查询
	Query(sql string) *sql.Rows
	//插入
	Insert(sql string) int
	//更新
	Update(sql string) int
	//删除
	Delete(sql string) int
}

type DB struct{
	*sql.DB
}

func initDB() (*DB,error){
	db,err := sql.Open(tinyConfig.DriverName,tinyConfig.ConnectionString)
	res := new(DB)
	if(err != nil){
		fmt.Printf("database open failed.\n")
		return res,err
	}
	err = db.Ping()
	if err != nil{
		fmt.Printf("database ping failed.\n")
		return res,err
	}
	res.DB = db
	return res,nil
}

func (this *DB) Query(sql string,model interface{},param...interface{}) *sql.Rows{
	if this == nil{
		fmt.Printf("db not init")
		return nil
	}
	stmtOut,err := this.Prepare(sql)
	if err != nil{
		fmt.Printf("sql prepare failed")
		return nil
	}
	defer stmtOut.Close()
	rows,err := stmtOut.Query(param...)
	if err != nil{
		fmt.Printf("query abort:%s",err.Error())
		return nil;
	}
	return rows;
}
