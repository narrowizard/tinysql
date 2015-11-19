package tinysql

import (
	"errors"
	"fmt"
)

// 配置错误信息
type TinySqlError string

// 错误码
const (
	TinySqlErrorParamInvalidError TinySqlError = "T10010:TinySqlErrorParamInvalidError,无效的输入类型(%s)"
	TinySqlErrorNoRowError        TinySqlError = "T10011:TinySqlErrorNoRowError,没有发现数据(%s)"
)

// Format 格式化错误信息并生成新的错误信息
func (this TinySqlError) Format(data ...interface{}) TinySqlError {
	return TinySqlError(fmt.Sprintf(string(this), data...))
}

// Error 生成error类型
func (this TinySqlError) Error() error {
	return errors.New(string(this))
}
