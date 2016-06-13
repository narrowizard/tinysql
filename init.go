package tinysql

import (
	"os"

	"github.com/kdada/tinygo"
	"github.com/kdada/tinylogger"
)

func init() {
	//初始化文件日志记录器
	fl, err1 := logger.NewLogger(logger.LoggerTypeFile)
	if err1 != nil {
		tinygo.Error(err1)
		os.Exit(110)
	}
	fl.SetAsync(true)
	logger.SetDefaultLogger(fl)
}

// createLog 创建日志记录
//  title:日志标题
//  content:日志内容
func createLog(title, strSql, strError string, params interface{}) {

	logger.DefaultLogger().WriteInfoLog("******* " + title + " *******")
	logger.DefaultLogger().WriteInfoLog("     [tinysql]执行语句:		" + strSql)
	logger.DefaultLogger().WriteInfoLog("     [tinysql]执行参数:		")
	logger.DefaultLogger().WriteInfoLog(params)
	logger.DefaultLogger().WriteInfoLog("     [tinysql]Error:		" + strError)
	logger.DefaultLogger().WriteInfoLog("------------------------------------------------------")
}
