package tinysql

type IQueryBuilder interface{
	//获取数据
	ToData() *[]interface{}
	
	SingleOrDefault() *interface{}
	//增加Where条件
	Where(col string,val interface{}) *DB
	
	OrWhere(key string,val interface{}) *DB
	
	OrWhereIn(key string,val []interface{}) *DB
	
	WhereIn(col string,val []interface{}) *DB
	//查询sql语句
	Query(sql string) *DB
	//
	From(table string) *DB

	Join(table string,condition string) *DB
	
	GroupBy(col string) *DB
	
	OrderBy(col string) *DB
	
	GroupStart() *DB
	
	GroupEnd() *DB

	Limit(limit int,offset int) *DB
	
	ToSql() string
	
	Select(columns string) *DB
	
	SelectMax(col string) *DB
	
	SelectMin(col string) *DB
	
	SelectAvg(col string) *DB
	
	SelectSum(col string) *DB
	
	Distinct() *DB
}