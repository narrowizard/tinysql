package tinysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type tableModel struct {
	name  string
	alias string
}

type columnModel struct {
	name  string
	alias string
}

type countModel struct {
	C int
}

type whereConstraint struct {
	column          string
	multiValue      bool
	value           interface{}
	values          []interface{}
	isOr            bool
	extCharPosition int //0 : 无 1 : group_start 2 : group_end
	extChar         int
}

type joinModel struct {
	name      string
	alias     string
	condition string
	joinType  string
}

type setModel struct {
	column string
	value  interface{}
}

type builder struct {
	tables         []tableModel
	columns        []columnModel
	joins          []joinModel
	groupby        []string
	having         []string
	whereCondition []whereConstraint
	distinct       bool
	limit          int
	offset         int
	orderby        []string
	groupStart     int
	groupEnd       int
	set            []setModel
	db             *DB
}

// Begin 开始一个事务,在调用Commit或者Rollback之前的所有sql操作会被绑定在同一个数据库连接
func (this *builder) Begin() bool {
	return this.db.begin()
}

// Commit 提交事务,如果在commit的时候产生错误(通常是由于连接被断开),数据库将自动执行rollback
func (this *builder) Commit() error {
	return this.db.commit()
}

// Rollback 回滚事务
func (this *builder) Rollback() error {
	return this.db.rollback()
}

// Query 执行查询
func (this *builder) Query() *Rows {
	var sql, params = this.toQuerySql()
	this.reset()
	return this.db.Query(sql, params...)
}

// Exec execute the sql query
// return rows affected.
// return -1 when error occurred
func (this *builder) Exec(sql string, params ...interface{}) int64 {
	res, err := this.db.Exec(sql, params...)
	if err != nil {
		fmt.Println(err.Error())
		return -1
	}
	var affects, _ = res.RowsAffected()
	return affects
}

// From 支持以下写法
// From("table")
// From("table alias")
// From("table as alias")
func (this *builder) From(table string) *builder {
	if strings.Trim(table, " ") == "" {
		return this
	}
	var ss = strings.Split(table, " ")
	var temp = new(tableModel)
	if len(ss) == 2 {
		temp.alias = ss[1]
		temp.name = addDelimiter(ss[0], 1)
	} else if len(ss) == 3 {
		if ss[1] == "as" {
			temp.alias = ss[2]
			temp.name = addDelimiter(ss[0], 1)
		} else {
			//格式错误
			fmt.Println("[TinySql]", "From param error")
			return this
		}
	} else if len(ss) == 1 {
		temp.name = addDelimiter(ss[0], 1)
	} else {
		//格式错误
		fmt.Println("[TinySql]", "From param error")
		return this
	}
	this.tables = append(this.tables, *temp)
	return this
}

// Select 支持逗号分隔的多个列
// 复杂查询请调用SelectSpec
func (this *builder) Select(columns string) *builder {
	s := strings.Split(columns, ",")
	if len(s) == 0 {
		return this
	}
	var aa, err = regexp.Compile(`([\w()*.]+)((\s*as)?\s*([\w]+))?`)
	if err != nil {
		fmt.Println("[TinySql]", err.Error())
		return this
	}
	for _, value := range s {
		var c = aa.ReplaceAllString(value, `$1|$4`)
		var temp = new(columnModel)
		var sp = strings.Split(c, "|")
		if sp[0] == "*" {
			temp.name = sp[0]
		} else {
			temp.name = addDelimiter(sp[0], 1)
		}
		temp.alias = sp[1]
		this.columns = append(this.columns, *temp)
	}
	return this
}

// SelectSpec 查询某一个特定的列
// 这里指的是包含函数的查询,如group_concat(concat(c1,c2,c3)) as alias
// 如果列名包含关键词,请自行加关键词限定符
func (this *builder) SelectSpec(column string, alias string) *builder {
	var temp = new(columnModel)
	temp.name = column
	temp.alias = alias
	this.columns = append(this.columns, *temp)
	return this
}

// SelectCount 搜索某个字段的Count值
func (this *builder) SelectCount(col string, alias string) *builder {
	if col == "*" {
		this.columns = append(this.columns, columnModel{name: "count(*)", alias: alias})
	} else if strings.Trim(col, " ") == "" {
		this.columns = append(this.columns, columnModel{name: "count(1)", alias: alias})
	} else {
		if strings.Contains(col, ".") {
			var df = strings.Split(col, ".")
			col = ""
			for j := 0; j < len(df); j++ {
				col += "`" + df[j] + "`" + "."
			}
		}
		this.columns = append(this.columns, columnModel{name: "count(" + col[:len(col)-1] + ")", alias: alias})
	}
	return this
}

func (this *builder) SelectMax(col string, alias string) *builder {
	return this.maxMinAvgSum(col, "max", alias)
}

func (this *builder) SelectMin(col string, alias string) *builder {
	return this.maxMinAvgSum(col, "min", alias)
}

func (this *builder) SelectAvg(col string, alias string) *builder {
	return this.maxMinAvgSum(col, "avg", alias)
}

func (this *builder) SelectSum(col string, alias string) *builder {
	return this.maxMinAvgSum(col, "sum", alias)
}

// Join 连接表
func (this *builder) Join(table string, condition string) *builder {
	return this.join(table, condition, "")
}

func (this *builder) LeftJoin(table string, condition string) *builder {
	return this.join(table, condition, "left ")
}

func (this *builder) RightJoin(table string, condition string) *builder {
	return this.join(table, condition, "right ")
}

func (this *builder) Distinct() *builder {
	this.distinct = true
	return this
}

// reset current builder's state
func (this *builder) reset() {
	this.tables = this.tables[:0]
	this.columns = this.columns[:0]
	this.groupby = this.groupby[:0]
	this.having = this.having[:0]
	this.orderby = this.orderby[:0]
	this.offset = 0
	this.limit = 0
	this.groupStart = 0
	this.groupEnd = 0
	this.distinct = false
	this.whereCondition = make([]whereConstraint, 0, 0)
	this.joins = make([]joinModel, 0, 0)
	this.set = make([]setModel, 0, 0)
}

func (this *builder) OrderBy(column string) *builder {
	if strings.Trim(column, " ") == "" {
		return this
	}
	var cols = strings.Split(column, ",")
	for i := 0; i < len(cols); i++ {
		cols[i] = addDelimiter(cols[i], 3)
	}
	this.orderby = append(this.orderby, cols...)
	return this
}

// toSql 生成sql语句
func (this *builder) toQuerySql() (string, []interface{}) {
	if len(this.tables) == 0 {
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
			sql += (this.columns[i].name + " " + this.columns[i].alias + ",")
		}
		sql = sql[:len(sql)-1]
	}
	// from
	sql += " from "
	for i := 0; i < len(this.tables); i++ {
		sql += (this.tables[i].name + " " + this.tables[i].alias + ",")
	}
	sql = sql[:len(sql)-1]
	sql += " "
	//join
	if len(this.joins) != 0 {
		for i := 0; i < len(this.joins); i++ {
			sql += this.joins[i].joinType
			sql += " join "
			sql += this.joins[i].name
			sql += " "
			sql += this.joins[i].alias
			sql += " on "
			sql += this.joins[i].condition
		}
	}
	// where
	if len(this.whereCondition) != 0 {
		sql += " where "
		isFirst := true
		for i := 0; i < len(this.whereCondition); i++ {
			var v = this.whereCondition[i]
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
				sql += (v.column + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (v.column + "? ")
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
	//group by
	if len(this.groupby) != 0 {
		sql += " group by "
		for i := 0; i < len(this.groupby); i++ {
			sql += this.groupby[i] + ","
		}
		sql = sql[:len(sql)-1]
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
	fmt.Println("[TinySql]", sql)
	return sql, params
}

// Delete 执行删除方法,返回影响行数
func (this *builder) Delete() int {
	var sql, params = this.toDeleteSql()
	this.reset()
	var res, err = this.db.Exec(sql, params...)
	if err != nil {
		return -1
	}
	var c int64
	c, err = res.RowsAffected()
	if err != nil {
		return -1
	}
	return int(c)
}

// Update 执行更新方法,返回影响行数
func (this *builder) Update(table string) int {
	if len(this.set) == 0 || strings.Trim(table, " ") == "" {
		return -1
	}
	var sql = "update `" + table + "` set "
	// set
	var params = make([]interface{}, 0, 0)
	for i := 0; i < len(this.set); i++ {
		if strings.Contains(this.set[i].column, "=") {
			sql += (this.set[i].column + ",")
		} else {
			sql += (this.set[i].column + "=?,")
		}
		params = append(params, this.set[i].value)
	}
	sql = sql[:len(sql)-1]
	// where
	if len(this.whereCondition) != 0 {
		sql += " where "
		var isFirst = true
		for i := 0; i < len(this.whereCondition); i++ {
			var v = this.whereCondition[i]
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
				sql += (v.column + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (v.column + "? ")
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
	fmt.Println("[TinySql]", sql)
	var result, err = this.db.Exec(sql, params...)
	if err != nil {
		fmt.Println("[TinySql]", err.Error())
		return -1
	}
	var c int64
	c, err = result.RowsAffected()
	if err != nil {
		return -1
	}
	return int(c)
}

// InsertModel 插入数据,表名即为model struct的名称
func (this *builder) InsertModel(model interface{}) int {
	var v = reflect.TypeOf(model).Elem()
	var table = transFieldName(v.Name())
	return this.Insert(table, model)
}

// Insert 向指定table插入数据
func (this *builder) Insert(table string, model interface{}) int {
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
	fmt.Println("[TinySql]", query)
	result, err = this.db.Exec(query, params...)
	if err != nil {
		fmt.Println("[TinySql]", err.Error())
		return -1
	}
	var id int64
	id, err = result.LastInsertId()
	if err != nil {
		return -1
	}
	return int(id)
}

// Set 为Update设置值
func (this *builder) Set(key string, value interface{}) *builder {
	if strings.Trim(key, " ") == "" {
		return this
	}
	//key = "`" + key + "`"
	var temp = setModel{column: key, value: value}
	this.set = append(this.set, temp)
	return this
}

func (this *builder) toDeleteSql() (string, []interface{}) {
	if len(this.tables) != 1 {
		return "", nil
	}
	var sql string
	sql = "delete from " + this.tables[0].name + " " + this.tables[0].alias
	params := make([]interface{}, 0, 0)
	if len(this.whereCondition) != 0 {
		sql += " where "
		isFirst := true
		for i := 0; i < len(this.whereCondition); i++ {
			var v = this.whereCondition[i]
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
				sql += (v.column + " in (")
				sql += strings.Repeat("?,", len(v.values))
				sql = sql[:len(sql)-1]
				sql += (") ")
				params = append(params, v.values...)
			} else {
				sql += (v.column + "? ")
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
	fmt.Println("[TinySql]", sql)
	return sql, params
}

// Count 返回符合条件的结果数量
// @param reset 查询完成后是否重置
// 注意:使用group by之后该函数失效
func (this *builder) Count(reset bool) int {
	var temp = this.columns
	this.columns = []columnModel{columnModel{name: "Count(*) as c"}}
	var c countModel
	var sql, params = this.toQuerySql()
	//去掉limit
	var aa, _ = regexp.Compile("limit \\d+,\\d+")
	sql = aa.ReplaceAllString(sql, "")
	var _, err = this.db.Query(sql, params...).Scan(&c)
	if err != nil {
		this.columns = temp
		return -1
	}
	this.columns = temp
	if reset {
		this.reset()
	}
	return c.C
}

func (this *builder) GroupBy(col string) *builder {
	var c = addDelimiter(col, 1)
	this.groupby = append(this.groupby, c)
	return this
}

func (this *builder) GroupConcat(col string, alias string) *builder {
	var c = addDelimiter(col, 1)
	this.columns = append(this.columns, columnModel{name: "group_concat(" + c + ") ", alias: alias})
	return this
}

// Having 暂未实现
func (this *builder) Having(col string, value interface{}) *builder {
	return this
}

func (this *builder) GroupStart() *builder {
	this.groupStart++
	return this
}

func (this *builder) GroupEnd() *builder {
	this.groupEnd++
	return this
}

func (this *builder) Where(key string, val interface{}) *builder {
	return this.where(key, val, "and")
}

// Like
// @param t 1:在前面加% 2:在后面加% 3:前后都加%
func (this *builder) Like(key string, val string) *builder {
	var aa = new(whereConstraint)
	aa.column = addDelimiter(key, 1) + " like "
	aa.isOr = false
	aa.multiValue = false
	aa.value = val
	this.whereCondition = append(this.whereCondition, *aa)
	return this
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

func (this *builder) join(table string, condition string, t string) *builder {
	if strings.Trim(table, " ") == "" {
		return this
	}
	var segment = strings.Split(table, " ")
	var jm = new(joinModel)
	jm.name = addDelimiter(segment[0], 1)
	if len(segment) == 1 {

	} else if len(segment) == 2 {
		jm.alias = segment[1]
	} else if len(segment) == 3 {
		if segment[1] == "as" {
			jm.alias = segment[2]
		}
	}
	jm.condition = condition
	jm.joinType = t
	this.joins = append(this.joins, *jm)
	return this
}

func (this *builder) maxMinAvgSum(col string, t string, alias string) *builder {
	if strings.Trim(col, " ") == "" {
		return this
	}
	if strings.Contains(col, ".") {
		var df = strings.Split(col, ".")
		col = ""
		for j := 0; j < len(df); j++ {
			col += "`" + df[j] + "`" + "."
		}
	} else {
		col += "."
	}
	this.columns = append(this.columns, columnModel{name: t + "(" + col[:len(col)-1] + ")", alias: alias})
	return this
}

func (this *builder) where(key string, val interface{}, t string) *builder {
	ext := strings.ContainsAny(key, "!<=>")
	if ext {
		var p = strings.IndexAny(key, "!<=>")
		var keyName = strings.Trim(key[:p], " ")
		var symbol = key[p:]
		//处理限定,如database.table.column
		key = addDelimiter(keyName, 1) + symbol
	} else {
		//处理限定,如database.table.column
		key = addDelimiter(key, 1) + "="
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
	aa.column = key
	this.whereCondition = append(this.whereCondition, *aa)
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
	//处理限定,如database.table.column

	key = addDelimiter(key, 1)
	aa.column = key
	this.whereCondition = append(this.whereCondition, *aa)
	return this
}

// addDelimiter 添加限定符(表名,列明)
// @param t 添加类型,1 database.table.column 2 table as alias
func addDelimiter(s string, t int) string {
	switch t {
	case 1:
		{
			var segments = strings.Split(s, ".")
			s = ""
			for i := 0; i < len(segments); i++ {
				if segments[i] == "*" {
					s += segments[i] + "."
					continue
				}
				s += "`" + segments[i] + "`" + "."
			}
			return s[:len(s)-1]
		}
	case 2:
		{
			var segments = strings.Split(s, " ")
			s = "`" + segments[0] + "`"
			for i := 1; i < len(segments); i++ {
				s += " " + segments[i]
			}
			return s
		}
	case 3:
		{
			var segments = strings.Split(s, " ")
			s = addDelimiter(segments[0], 1)
			for i := 1; i < len(segments); i++ {
				s += " " + segments[i]
			}
			return s
		}
	default:
		return s
	}
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
