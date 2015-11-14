package tinysql

import (
	"strconv"
)

import(
	"strings"
	"errors"
	"fmt"
	"database/sql"
	"reflect"
)

type whereConstraint struct{
	multiValue bool
	value interface{}
	values []interface{}
	isOr bool
	extCharPosition int	//0 : 无 1 : group_start 2 : group_end
	extChar int
}
// group by order by having
type DB struct{
	*sql.DB
	data *[] interface{}
	from []string
	columns []string
	join map[string]string
	groupby []string
	having[] string
	where map[string]whereConstraint
	distinct bool
	limit int
	offset int
	orderby []string
	err []error
	groupStart int
	groupEnd int
}

//支持逗号分隔的多个表
func (this *DB) From(table string) *DB{
	if strings.Trim(table," ") == "" {
		return this
	}
	t := strings.Split(table,",")
	if len(t) == 0 {
		return this
	}
	this.from = append(this.from,t...)
	return this
}

func (this *DB) SelectMax(col string) *DB{
	return this.max_min_avg_sum(col,"max")
}

func (this *DB) SelectMin(col string) *DB{
	return this.max_min_avg_sum(col,"min")
}

func (this *DB) SelectAvg(col string) *DB{
	return this.max_min_avg_sum(col,"avg")
}

func (this *DB) SelectSum(col string) *DB{
	return this.max_min_avg_sum(col,"sum")
}

func (this *DB) Join(table string,condition string) *DB{
	this.join[table] = condition
	return this
}

//func (this *DB) GroupBy(col string) *DB{
	
//	return this
//}

func (this *DB) GroupStart() *DB{
	this.groupStart++
	return this
}

func (this *DB) GroupEnd() *DB{
	this.groupEnd++
	return this
}

//支持逗号分隔的多个列
func (this *DB) Select(columns string) *DB{
	s := strings.Split(columns,",")
	if len(s) == 0{
		return this
	}
	this.columns = append(this.columns,s...)
	return this
}

func (this *DB) ToSql() string{
	if len(this.from) == 0{
		this.err = append(this.err,errors.New("table not be set"))
		return ""
	}
	var sql string
	var params = make([]interface{},0,0)
	//select 
	sql = "select "
	if this.distinct {
		sql += " distinct "
	}
	if len(this.columns) == 0 {
		sql += " * "
	}else{
		for i := 0;i < len(this.columns);i++{
			sql += (this.columns[i] + ",")
		}
		sql = sql[:len(sql) - 1]
	}
	// from
	sql += " from "
	for i := 0;i < len(this.from);i++{
		sql += (this.from[i] + ",")
	}
	sql = sql[:len(sql) - 1]
	//join 
	if len(this.join) != 0{
		for k,v := range this.join{
			sql += " join "
			sql += k
			sql += " on "
			sql += v
		}
	}
	//group by
	
	// where
	if len(this.where) != 0{
		sql += " where "
		isFirst := true
		for k,v := range this.where{
			//where group
			if v.extCharPosition == 1{
				sql += " "
				sql += strings.Repeat("(",v.extChar)
			}
			if v.isOr {
				if !isFirst{
					sql += " or "
				}else{
					isFirst = false
				}
				
			}else{
				if !isFirst{
					sql += " and "
				}else{
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,",len(v.values))
				sql = sql[:len(sql) - 1]
				sql += (") ")
				params = append(params,v.values...)
			}else{
				sql += (k + "? ")
				params = append(params,v.value)
			}
			// where group
			if v.extCharPosition == 2{
				sql += " "
				sql += strings.Repeat(")",v.extChar)
			}
		}
	}
	if this.groupEnd != 0{
		sql += strings.Repeat(")",this.groupEnd)
	}
	//having 
	
	//limit
	if this.limit != 0{
		sql += " limit " + strconv.Itoa(this.offset) + "," + strconv.Itoa(this.limit)
	}
	return sql
}

func (this *DB) Get(model interface{}) *DB{
	if len(this.from) == 0{
		this.err = append(this.err,errors.New("table not be set"))
		return this
	}
	var sql string
	var params = make([]interface{},0,0)
	//select 
	sql = "select "
	if this.distinct {
		sql += " distinct "
	}
	if len(this.columns) == 0 {
		sql += " * "
	}else{
		for i := 0;i < len(this.columns);i++{
			sql += (this.columns[i] + ",")
		}
		sql = sql[:len(sql) - 1]
	}
	// from
	sql += " from "
	for i := 0;i < len(this.from);i++{
		sql += (this.from[i] + ",")
	}
	sql = sql[:len(sql) - 1]
	//join 
	if len(this.join) != 0{
		for k,v := range this.join{
			sql += " join "
			sql += k
			sql += " on "
			sql += v
		}
	}
	//group by
	
	// where
	if len(this.where) != 0{
		sql += " where "
		isFirst := true
		for k,v := range this.where{
			//where group
			if v.extCharPosition == 1{
				sql += " "
				sql += strings.Repeat("(",v.extChar)
			}
			if v.isOr {
				if !isFirst{
					sql += " or "
				}else{
					isFirst = false
				}
				
			}else{
				if !isFirst{
					sql += " and "
				}else{
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,",len(v.values))
				sql = sql[:len(sql) - 1]
				sql += (") ")
				params = append(params,v.values...)
			}else{
				sql += (k + "? ")
				params = append(params,v.value)
			}
			// where group
			if v.extCharPosition == 2{
				sql += " "
				sql += strings.Repeat(")",v.extChar)
			}
		}
	}
	if this.groupEnd != 0{
		sql += strings.Repeat(")",this.groupEnd)
	}
	rows,err := this.DB.Query(sql,params...)
	if err != nil{
		this.err = append(this.err,err)
		fmt.Println(err.Error())
		return this
	}
	this.data,err = resolveRowsData(model,rows)
	if err != nil {
		this.err = append(this.err,err)
	}
	this.reset()
	return this
}

func (this *DB) Where(key string,val interface{})(*DB){
	return this._where(key,val,"and")
}

func (this *DB) OrWhere(key string,val interface{}) *DB{
	return this._where(key,val,"or")
}

func (this *DB) WhereIn(key string,val []interface{}) *DB{
	return this._where_in(key,val,"and")
}

func (this *DB) OrWhereIn(key string,val []interface{})*DB{
	return this._where_in(key,val,"or")
}

func (this *DB) Limit(limit int,offset int) *DB{
	this.limit = limit
	this.offset = offset
	return this
}

func (this *DB) HasError() bool{
	return len(this.err) != 0
}

func (this *DB) GetError() []error{
	return this.err
}

//将查询转换成数组
func (this *DB) ToData()(*[] interface{},bool){
	if this.HasError(){
		for i := 0;i < len(this.err);i++{
			fmt.Println(this.err[i].Error())
		} 
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
	this.data,err = resolveRowsData(model,rows)
	if err != nil{
		this.err = append(this.err,err)
	}
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
func (this *DB) Delete()(effects int64){
	if len(this.from) != 1{
		return 0
	}
	var sql string
	sql = "delete from " + this.from[0]
	params := make([]interface{},0,0)
	if len(this.where) != 0{
		sql += " where "
		isFirst := true
		for k,v := range this.where{
			//where group
			if v.extCharPosition == 1{
				sql += " "
				sql += strings.Repeat("(",v.extChar)
			}
			if v.isOr {
				if !isFirst{
					sql += " or "
				}else{
					isFirst = false
				}
				
			}else{
				if !isFirst{
					sql += " and "
				}else{
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,",len(v.values))
				sql = sql[:len(sql) - 1]
				sql += (") ")
				params = append(params,v.values...)
			}else{
				sql += (k + "? ")
				params = append(params,v.value)
			}
			// where group
			if v.extCharPosition == 2{
				sql += " "
				sql += strings.Repeat(")",v.extChar)
			}
		}
	}
	if this.groupEnd != 0{
		sql += strings.Repeat(")",this.groupEnd)
	}
	_,effects = this.execute(sql,params...)
	this.reset()
	return effects
}

//更新数据
func (this *DB) Update(table string,model interface{})(effects int64){
	query := "update " + table + " set "
	value := reflect.ValueOf(model).Elem()
	data := make(map[string]interface{})
	mapStructToMap(value,data)
	params := make([]interface{},0,0)
	
	for k,v := range data{
		query += (k + "=?,")
		params = append(params,v)
	}
	query = query[:len(query) - 1]
	_,effects = this.execute(query,params)
	return
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

func (this *DB) reset(){
	this.from = this.from[:0]
	this.columns = this.columns[:0]
	this.groupby = this.groupby[:0]
	this.having = this.having[:0]
	this.orderby = this.orderby[:0]
	this.offset = 0
	this.limit = 0
	this.groupStart = 0
	this.groupEnd = 0
	this.distinct = false
	this.where = make(map[string]whereConstraint)
	this.join = make(map[string]string)
	this.err = make([]error,0,0)
}

func (this *DB) max_min_avg_sum(col string,t string) *DB{
	if strings.Trim(col," ") == "" {
		return this
	}
	this.columns = append(this.columns,t + "(" + col + ")")
	return this
}

func (this *DB) _where(key string,val interface{},t string) *DB{
	ext := strings.ContainsAny(key,"<=>")
	if !ext{
		key = key + "="
	}
	var aa = new(whereConstraint)
	if strings.ToUpper(t) == "OR" {
		aa.isOr = true
	}
	if strings.ToUpper(t) == "AND" {
		aa.isOr = false
	}
	aa.multiValue = false
	aa.value = val
	if this.groupStart != 0{
		aa.extCharPosition = 1
		aa.extChar = this.groupStart
		this.groupStart = 0
	}else if this.groupEnd != 0{
		aa.extCharPosition = 2
		aa.extChar = this.groupEnd
		this.groupEnd = 0
	}else{
		aa.extCharPosition = 0
	}
	this.where[key] = *aa
	return this
}

func (this *DB) _where_in(key string,val []interface{},t string) *DB{
	var aa = new(whereConstraint)
	if strings.ToUpper(t) == "OR" {
		aa.isOr = true
	}
	if strings.ToUpper(t) == "AND" {
		aa.isOr = false
	}
	aa.multiValue = true
	aa.values = val
	if this.groupStart != 0{
		aa.extCharPosition = 1
		aa.extChar = this.groupStart
		this.groupStart = 0
	}else if this.groupEnd != 0{
		aa.extCharPosition = 2
		aa.extChar = this.groupEnd
		this.groupEnd = 0
	}else{
		aa.extCharPosition = 0
	}
	this.where[key] = *aa
	return this
}
