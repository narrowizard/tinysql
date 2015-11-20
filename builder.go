package tinysql

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
)

type whereConstraint struct {
	multiValue      bool
	value           interface{}
	values          []interface{}
	isOr            bool
	extCharPosition int //0 : 无 1 : group_start 2 : group_end
	extChar         int
}

type builder struct {
	from           []string
	columns        []string
	join           map[string]string
	groupby        []string
	having         []string
	whereCondition map[string]whereConstraint
	distinct       bool
	limit          int
	offset         int
	orderby        []string
	groupStart     int
	groupEnd       int
	set            map[string]interface{}
	db             *DB
}

func (this *builder) reset() {
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
	this.whereCondition = make(map[string]whereConstraint)
	this.join = make(map[string]string)
	this.set = make(map[string]interface{})
}

func (this *builder) OrderBy(column string) *builder {
	var cols = strings.Split(column, ",")
	for i := 0; i < len(cols); i++ {
		cols[i] = "`" + cols[i] + "`"
	}
	this.orderby = append(this.orderby, cols...)
	return this
}

// toSql 生成sql语句
func (this *builder) toQuerySql() (string, []interface{}) {
	if len(this.from) == 0 {
		return "", nil
	}
	var sql string
	var params = make([]interface{}, 0, 0)
	//select
	sql = "select "
	if this.distinct {
		sql += " distinct "
	}
	if len(this.columns) == 0 {
		sql += " * "
	} else {
		for i := 0; i < len(this.columns); i++ {
			sql += (this.columns[i] + ",")
		}
		sql = sql[:len(sql)-1]
	}
	// from
	sql += " from "
	for i := 0; i < len(this.from); i++ {
		sql += (this.from[i] + ",")
	}
	sql = sql[:len(sql)-1]
	//join
	if len(this.join) != 0 {
		for k, v := range this.join {
			sql += " join "
			sql += k
			sql += " on "
			sql += v
		}
	}
	//group by

	// where
	if len(this.whereCondition) != 0 {
		sql += " where "
		isFirst := true
		for k, v := range this.whereCondition {
			//where group
			if v.extCharPosition == 1 {
				sql += " "
				sql += strings.Repeat("(", v.extChar)
			}
			if v.isOr {
				if !isFirst {
					sql += " or "
				} else {
					isFirst = false
				}

			} else {
				if !isFirst {
					sql += " and "
				} else {
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (k + "? ")
				params = append(params, v.value)
			}
			// where group
			if v.extCharPosition == 2 {
				sql += " "
				sql += strings.Repeat(")", v.extChar)
			}
		}
	}
	if this.groupEnd != 0 {
		sql += strings.Repeat(")", this.groupEnd)
	}
	//order by
	if len(this.orderby) != 0 {
		sql += " order by "
		for i := 0; i < len(this.orderby); i++ {
			sql += this.orderby[i] + ","
		}
		sql = sql[:len(sql)-1]
	}
	//limit
	if this.limit != 0 {
		sql += " limit "
		sql += strconv.Itoa(this.offset)
		sql += ","
		sql += strconv.Itoa(this.limit)
	}

	return sql, params
}

// Query 执行查询
func (this *builder) Query() *Rows {
	var sql, params = this.toQuerySql()
	this.reset()
	return this.db.Query(sql, params...)
}

// Delete 执行删除方法,返回影响行数
func (this *builder) Delete() int64 {
	var sql, params = this.toDeleteSql()
	this.reset()
	var res, err = this.db.Exec(sql, params)
	if err != nil {
		return -1
	}
	var c int64
	c, err = res.RowsAffected()
	if err != nil {
		return -1
	}
	return c
}

// Update 执行更新方法,返回影响行数
func (this *builder) Update(table string) int64 {
	if len(this.set) == 0 || strings.Trim(table, " ") == "" {
		return -1
	}
	var sql = "update `" + table + "` set "
	var params = make([]interface{}, 0, 0)
	for k, v := range this.set {
		sql += (k + "=?,")
		params = append(params, v)
	}
	sql = sql[:len(sql)-1]
	if len(this.whereCondition) != 0 {
		sql += " where "
		var isFirst = true
		for k, v := range this.whereCondition {
			//where group
			if v.extCharPosition == 1 {
				sql += " "
				sql += strings.Repeat("(", v.extChar)
			}
			if v.isOr {
				if !isFirst {
					sql += " or "
				} else {
					isFirst = false
				}

			} else {
				if !isFirst {
					sql += " and "
				} else {
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (k + "? ")
				params = append(params, v.value)
			}
			// where group
			if v.extCharPosition == 2 {
				sql += " "
				sql += strings.Repeat(")", v.extChar)
			}
		}
		if this.groupEnd != 0 {
			sql += strings.Repeat(")", this.groupEnd)
		}
	}
	this.reset()
	var result, err = this.db.Exec(sql, params...)
	if err != nil {
		return -1
	}
	var c int64
	c, err = result.RowsAffected()
	if err != nil {
		return -1
	}
	return c
}

// InsertModel 插入数据,表名即为model struct的名称
func (this *builder) InsertModel(model interface{}) int64 {
	var v = reflect.TypeOf(model).Elem()
	var table = transFieldName(v.Name())
	return this.Insert(table, model)
}

// Insert 向指定table插入数据
func (this *builder) Insert(table string, model interface{}) int64 {
	query := "insert into `" + table + "`"
	value := reflect.ValueOf(model).Elem()
	data := make(map[string]interface{})
	mapStructToMap(value, data)
	keys := " ("
	values := " ("
	params := make([]interface{}, 0, 0)
	for k, v := range data {
		keys += "`" + k + "`,"
		values += "?,"
		params = append(params, v)
	}
	query += keys[:len(keys)-1] + ") values"
	query += values[:len(values)-1] + ")"
	var result sql.Result
	var err error
	this.reset()
	result, err = this.db.Exec(query, params...)
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

// Set 为Update设置值
func (this *builder) Set(key string, value interface{}) *builder {
	if strings.Trim(key, " ") == "" {
		return this
	}
	key = "`" + key + "`"
	this.set[key] = value
	return this
}

func (this *builder) toDeleteSql() (string, []interface{}) {
	if len(this.from) != 1 {
		return "", nil
	}
	var sql string
	sql = "delete from " + this.from[0]
	params := make([]interface{}, 0, 0)
	if len(this.whereCondition) != 0 {
		sql += " where "
		isFirst := true
		for k, v := range this.whereCondition {
			//where group
			if v.extCharPosition == 1 {
				sql += " "
				sql += strings.Repeat("(", v.extChar)
			}
			if v.isOr {
				if !isFirst {
					sql += " or "
				} else {
					isFirst = false
				}

			} else {
				if !isFirst {
					sql += " and "
				} else {
					isFirst = false
				}
			}
			if v.multiValue {
				sql += (k + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (k + "? ")
				params = append(params, v.value)
			}
			// where group
			if v.extCharPosition == 2 {
				sql += " "
				sql += strings.Repeat(")", v.extChar)
			}
		}
	}
	if this.groupEnd != 0 {
		sql += strings.Repeat(")", this.groupEnd)
	}
	return sql, params
}

// From 设置查询的表,支持逗号分隔的多个表
func (this *builder) From(table string) *builder {
	if strings.Trim(table, " ") == "" {
		return this
	}
	t := strings.Split(table, ",")
	if len(t) == 0 {
		return this
	}
	for i := 0; i < len(t); i++ {
		t[i] = "`" + t[i] + "`"
	}
	this.from = append(this.from, t...)
	return this
}

func (this *builder) SelectMax(col string) *builder {
	return this.maxMinAvgSum(col, "max")
}

func (this *builder) SelectMin(col string) *builder {
	return this.maxMinAvgSum(col, "min")
}

func (this *builder) SelectAvg(col string) *builder {
	return this.maxMinAvgSum(col, "avg")
}

func (this *builder) SelectSum(col string) *builder {
	return this.maxMinAvgSum(col, "sum")
}

func (this *builder) Join(table string, condition string) *builder {
	table = "`" + table + "`"
	this.join[table] = condition
	return this
}

//func (this *Builder) GroupBy(col string) *Builder{

//	return this
//}

func (this *builder) GroupStart() *builder {
	this.groupStart++
	return this
}

func (this *builder) GroupEnd() *builder {
	this.groupEnd++
	return this
}

// Select 支持逗号分隔的多个列
func (this *builder) Select(columns string) *builder {
	s := strings.Split(columns, ",")
	if len(s) == 0 {
		return this
	}
	for i := 0; i < len(s); i++ {
		if s[i] == "*" {
			continue
		}
		s[i] = "`" + s[i] + "`"
	}
	this.columns = append(this.columns, s...)
	return this
}

func (this *builder) Where(key string, val interface{}) *builder {
	return this.where(key, val, "and")
}

func (this *builder) OrWhere(key string, val interface{}) *builder {
	return this.where(key, val, "or")
}

func (this *builder) WhereIn(key string, val []interface{}) *builder {
	return this.whereIn(key, val, "and")
}

func (this *builder) OrWhereIn(key string, val []interface{}) *builder {
	return this.whereIn(key, val, "or")
}

func (this *builder) Limit(limit int, offset int) *builder {
	this.limit = limit
	this.offset = offset
	return this
}

func (this *builder) maxMinAvgSum(col string, t string) *builder {
	if strings.Trim(col, " ") == "" {
		return this
	}
	this.columns = append(this.columns, t+"(`"+col+"`)")
	return this
}

func (this *builder) where(key string, val interface{}, t string) *builder {
	ext := strings.ContainsAny(key, "<=>")
	if ext {
		var p = strings.IndexAny(key, "<=>")
		var keyName = key[:p]
		var symbol = key[p:]
		key = "`" + keyName + "`" + symbol
	} else {
		key = "`" + key + "`="
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
	if this.groupStart != 0 {
		aa.extCharPosition = 1
		aa.extChar = this.groupStart
		this.groupStart = 0
	} else if this.groupEnd != 0 {
		aa.extCharPosition = 2
		aa.extChar = this.groupEnd
		this.groupEnd = 0
	} else {
		aa.extCharPosition = 0
	}
	this.whereCondition[key] = *aa
	return this
}

func (this *builder) whereIn(key string, val []interface{}, t string) *builder {
	var aa = new(whereConstraint)
	if strings.ToUpper(t) == "OR" {
		aa.isOr = true
	}
	if strings.ToUpper(t) == "AND" {
		aa.isOr = false
	}
	aa.multiValue = true
	aa.values = val
	if this.groupStart != 0 {
		aa.extCharPosition = 1
		aa.extChar = this.groupStart
		this.groupStart = 0
	} else if this.groupEnd != 0 {
		aa.extCharPosition = 2
		aa.extChar = this.groupEnd
		this.groupEnd = 0
	} else {
		aa.extCharPosition = 0
	}
	key = "`" + key + "`"
	this.whereCondition[key] = *aa
	return this
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
					if fieldName == "-" {
						continue
					}
					if fieldName == "" {
						fieldName = transFieldName(fieldType.Name)
					}
					data[fieldName] = fieldValue.Interface()
					//fmt.Println(fieldName + ":" + fieldValue.Interface().(string))
				}
			}
		}
	}
}
