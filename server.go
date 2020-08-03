// 简易gin server, 具体方法在server_handle.go
package main

import "github.com/gin-gonic/gin"

func server() {
	router := gin.Default()
	// nginx通过/proxy 进行端口转发
	proxy := router.Group("/proxy")
	{
		proxy.GET("/people", getPeopleHandle)
		proxy.GET("/people/couter", areaCouterHandle)
		proxy.GET("/people/excelize", areaExcelizeHandle)
	}
	router.Run()
}
