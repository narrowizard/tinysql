package tinysql

import (
	"reflect"
	"errors"
	"database/sql"
)

//解析sql.Rows到interface{}的slice中
func resolveRowsData(model interface{},rows *sql.Rows) (*[]interface{},error){
	s := reflect.ValueOf(model).Elem()
	if !s.IsValid() {
		return nil,errors.New("model access denied.")
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
		        return nil,err
		    }
		    result = append(result, s.Interface())
		}
		return &result,nil
	}else{
		result := make([]interface{},0)
		temp := s.Addr().Interface()
		for rows.Next(){
			err := rows.Scan(temp)
		    if err != nil {
		        return nil,err
		    }
		    result = append(result, s.Interface())
		}
		return &result,nil
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