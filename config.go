package tinysql

import(
	"fmt"
	"io/ioutil"
	"encoding/json"
	"github.com/narro/tinysql/info"
)

// Tiny配置
var tinyConfig = struct {
	ConnectionString	string   //连接字符窜
	DriverName			string	 //数据库驱动
}{
	ConnectionString:		"",			//默认为80端口
	DriverName:				"mysql",	//默认为mysql	
}

// loadConfig 加载配置
func loadConfig() {
	var config = info.DefaultConfigPath
	var content, err = ioutil.ReadFile(config)
	if err == nil {
		err = json.Unmarshal(content, &tinyConfig)
	} 
	if err != nil {
		fmt.Println(config,err)
	}
}