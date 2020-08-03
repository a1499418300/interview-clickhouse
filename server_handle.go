// 请求处理
package main

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

var (
	gv GlobalVar // 全局变量
)

// GlobalVar 全局变量
type GlobalVar struct {
	DB              ClickHouseProxy
	oriCSVName      string // csv源文件名
	excelizeOutName string // 输入统计excel文件名
}

func serverInit() error {
	DBProxy := ClickHouseProxy{}
	e := DBProxy.Conn("tcp://127.0.0.1:9000?debug=true")
	if e != nil {
		return e
	}
	gv.DB = DBProxy
	gv.oriCSVName = "testdata.csv"
	gv.excelizeOutName = "地区成绩分布.xlsx"
	return nil
}

func getPeopleHandle(c *gin.Context) {
	var e error
	var total int
	var items []People
	defer func() {
		c.JSON(200, gin.H{
			"data":  items,
			"ret":   ErrCode(e),
			"msg":   ErrMsg(e),
			"total": total,
		})
	}()

	condition := c.Query("condition")
	pageIdxStr := c.Query("pageidx")
	pageSizeStr := c.Query("pagesize")
	pageIdx, e := strconv.Atoi(pageIdxStr)
	if e != nil {
		return
	}
	pageSize, e := strconv.Atoi(pageSizeStr)
	if e != nil {
		return
	}

	items, total, e = queryPeople(gv.DB, condition, pageIdx, pageSize)
	return
}

func areaCouterHandle(c *gin.Context) {
	var e error
	var items []Counter
	defer func() {
		c.JSON(200, gin.H{
			"data": items,
			"ret":  ErrCode(e),
			"msg":  ErrMsg(e),
		})
	}()

	column := c.Query("column")
	items, e = areaCounter(gv.DB, column)
	return
}

func areaExcelizeHandle(c *gin.Context) {
	var e error
	defer func() {
		c.JSON(200, gin.H{
			"ret": ErrCode(e),
			"msg": ErrMsg(e),
		})
	}()

	e = peopleExcelize(gv.DB)
	return
}
