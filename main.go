package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "load") {
		// 生成测试数据
		e := mockTask()
		fmt.Println("mockTask excute, err: %+v", e)
		return
	}

	// 以服务运行
	e := serverInit()
	if e != nil {
		fmt.Printf("serverInit failed, err: %+v", e)
		return
	}
	server()
}
