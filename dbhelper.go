package tinysql

import (
	"strings"
	"errors"
)

import(
	"fmt"
	"database/sql"
	"reflect"
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

type Count struct{
	Count int
}

type DB struct{
	*sql.DB
	data *[] interface{}
	from []string
	columns []string
	constraint map[string]interface{}
	err []error
}

func (this *DB) From(table string) *DB{
	if strings.Trim(table," ") == "" {
		return this
	}
	this.from = append(this.from,table)
	return this
}

func (this *DB) Select(columns string) *DB{
	s := strings.Split(columns,",")
	if len(s) == 0{
		return this
	}
	this.columns = append(this.columns,s...)
	return this
}

func (this *DB) Get(model interface{}) *DB{
	if len(this.from) == 0{
		this.err = append(this.err,errors.New("table not be set"))
		return this
	}
	var sql string
	var params = make([]interface{},0,0)
	sql = "select "
	if len(this.columns) == 0 {
		sql += " * "
	}else{
		for i := 0;i < len(this.columns);i++{
			sql += (this.columns[i] + ",")
		}
		sql = sql[:len(sql) - 1]
	}
	sql += " from "
	for i := 0;i < len(this.from);i++{
		sql += (this.from[i] + ",")
	}
	sql = sql[:len(sql) - 1]
	if len(this.constraint) != 0{
		sql += " where "
		for k,v := range this.constraint{
			sql += (k + "=? and ")
			params = append(params,v)
		}
		sql = sql[:len(sql) - 4]
	}
	fmt.Println(sql)
	rows,err := this.DB.Query(sql,params...)
	if err != nil{
		this.err = append(this.err,err)
		fmt.Println(err.Error())
		return this
	}
	this.data = ResolveRowsData(model,rows)
	return this
}

func (this *DB) Where(key string,value interface{})(*DB){
	this.constraint[key] = value
	return this
}

func (this *DB) HasError() bool{
	return len(this.err) != 0
}

func (this *DB) GetError() []error{
	return this.err
}

func (this *DB) Reset(){
	this.data = nil
	this.constraint = make(map[string]interface{})
	this.err = make([]error,0,0)
}

//将查询转换成数组
func (this *DB) ToData()(*[] interface{},bool){
	if this.HasError(){
		return nil,false
	}
	return this.data,!this.HasError()
}

//取第一条数据，若不存在，则返回nil
func (this *DB) FirstOrDefault()(*interface{}){
	if(len(*this.data) == 0){
		return nil
	}else{
		return &(*this.data)[0]
	}
}

//初始化DB对象，DB是一个支持多线程，线程安全的对象
func initDB(driver string,connString string) (*DB,error){
	db,err := sql.Open(driver,connString)
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
	res.constraint = make(map[string]interface{})
	res.err = make([]error,0,0)
	return res,nil
}

//执行查询语句，返回DB对象
func (this *DB) Query(query string,model interface{},param...interface{})(*DB){
	//var oldTime = time.Now().UnixNano()
	if this == nil{
		var err error = errors.New("db not init.")
		this.err = append(this.err,err)
		return this
	}
	stmtOut,err := this.Prepare(query)
	if err != nil{
		this.err = append(this.err,err)
		return this
	}
	defer stmtOut.Close()
	rows,err := stmtOut.Query(param...)
	if err != nil{
		this.err = append(this.err,err)
		return this
	}
	defer rows.Close()
	this.data = ResolveRowsData(model,rows)
	return this
}

//向指定table插入数据
func (this *DB) Insert(table string,model interface{})(id int64){
	query := "insert into " + table
	value := reflect.ValueOf(model).Elem()
	data := make(map[string]interface{})
	mapStructToMap(value,data)
	keys := " ("
	values := " ("
	params := make([]interface{},0,0)
	for k,v := range data{
		keys += k + ","
		values += "?,"
		params = append(params,v)
	}
	query += keys[:len(keys) - 1] + ") values"
	query += values[:len(values) - 1] + ")"
	id,_ = this.execute(query,params...)
	return
}

//删除数据
func (this *DB) Delete(table string)(effects int64){
	query := "delete from " + table + " where "
	params := make([]interface{},0,0)
	for k,v := range this.constraint{
		query += k + "=? and "
		params = append(params,v)
	}
	query = query[:len(query) - 4]
	_,effects = this.execute(query,params...)
	this.constraint = make(map[string]interface{})
	return effects
}

//更新数据
func (this *DB) Update(table string)(effects int64){
	return 0
}

// mapStructToMap 将一个结构体所有字段(包括通过组合得来的字段)到一个map中
// value:结构体的反射值
// data:存储字段数据的map
func mapStructToMap(value reflect.Value, data map[string]interface{}) {
	if value.Kind() == reflect.Struct {
		for i := 0; i < value.NumField(); i++ {
			var fieldValue = value.Field(i)
			if fieldValue.CanInterface() {
				var fieldType = value.Type().Field(i)
				if fieldType.Anonymous {
					//匿名组合字段,进行递归解析
					mapStructToMap(fieldValue, data)
				} else {
					//非匿名字段
					var fieldName = fieldType.Tag.Get("db")
					if(fieldName == "null"){
						continue
					}
					if fieldName == "" {
						fieldName = fieldType.Name
					}
					data[fieldName] = fieldValue.Interface()
					//fmt.Println(fieldName + ":" + fieldValue.Interface().(string))
				}
			}
		}
	}
}


func (this *DB) execute(query string,param...interface{}) (id int64,effects int64){
	if this == nil{
		fmt.Printf("db not init.\n")
		return 0,0
	}
	stmt,err := this.Prepare(query)
	if err != nil{
		fmt.Printf("sql prepare failed.\n")
		fmt.Printf(err.Error())
		return 0,0
	}
	res,err := stmt.Exec(param...)
	if err != nil{
		fmt.Printf("sql exec failed.\n")
		fmt.Printf(err.Error())
		return 0,0
	}
	id,err = res.LastInsertId()
	if err != nil{
		id = 0
	}
	effects,err = res.RowsAffected()
	if err != nil{
		effects = 0
	}
	return
}

//解析sql.Rows到interface{}的slice中
func ResolveRowsData(model interface{},rows *sql.Rows) *[]interface{}{
	s := reflect.ValueOf(model).Elem()
	if !s.IsValid() {
		return nil
	}
	t := s.Kind()
	if t == reflect.Struct{
		count := s.NumField()
		result := make([]interface{},0)
		onerow := make([]interface{},count)
		for i := 0; i < count; i++ {
			onerow[i] = s.Field(i).Addr().Interface()
		}
		for rows.Next(){
			err := rows.Scan(onerow...)
		    if err != nil {
		        panic(err)
		    }
		    result = append(result, s.Interface())
		}
		return &result
	}else{
		result := make([]interface{},0)
		temp := s.Addr().Interface()
		for rows.Next(){
			err := rows.Scan(temp)
		    if err != nil {
		        panic(err)
		    }
		    result = append(result, s.Interface())
		}
		return &result
	}
}

