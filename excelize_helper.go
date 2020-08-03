package main

import (
	"encoding/json"
	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize"
)

type Series struct {
	Name       string `json:"name"`
	Categories string `json:"categories"`
}

func peopleExcelize(p ClickHouseProxy) error {
	rows, e := areaCounter(p, "score")
	if e != nil {
		return e
	}

	// "编号", "姓名", "年龄", "所在地区", "考试成绩"
	categories := map[string]string{"A1": "所在地区", "B1": "总人数", "C1": "最好成绩", "D1": "最差成绩", "E1": "平均成绩"}
	values := map[string]int{"B2": 2, "C2": 3, "D2": 3, "B3": 5, "C3": 2, "D3": 4, "B4": 6, "C4": 7, "D4": 8}
	// series := [{"name":"Sheet1!$A$2","categories":"Sheet1!$B$1:$E$1","values":"Sheet1!$B$2:$D$2"},{"name":"Sheet1!$A$3","categories":"Sheet1!$B$1:$E$1","values":"Sheet1!$B$3:$D$3"},{"name":"Sheet1!$A$4","categories":"Sheet1!$B$1:$E$1","values":"Sheet1!$B$4:$D$4"}]
	series := make([]Series, 0)
	for i := range rows {
		rowIdx := i + 2
		colAkey := fmt.Sprintf("A%d", rowIdx)
		colBkey := fmt.Sprintf("B%d", rowIdx)
		colCkey := fmt.Sprintf("C%d", rowIdx)
		colDkey := fmt.Sprintf("D%d", rowIdx)
		colEkey := fmt.Sprintf("E%d", rowIdx)
		seriesNameKey := fmt.Sprintf("Sheet1!$A$%d", rowIdx)
		seriesValKey := fmt.Sprintf("Sheet1!$B$%d:$E$%d", rowIdx, rowIdx)
		categories[colAkey] = rows[i].Key
		values[colBkey] = rows[i].Num
		values[colCkey] = rows[i].Max
		values[colDkey] = rows[i].Min
		values[colEkey] = int(rows[i].Avg)
		series = append(series, Series{
			Name:       seriesNameKey,
			Categories: seriesValKey,
		})
	}

	f := excelize.NewFile()
	for k, v := range categories {
		f.SetCellValue("Sheet1", k, v)
	}
	for k, v := range values {
		f.SetCellValue("Sheet1", k, v)
	}

	seriesB, e := json.Marshal(series)
	if e != nil {
		return e
	}

	chartOption := fmt.Sprintf(`{"type":"col3DClustered","series": %s,"title":{"name":"Fruit 3D Clustered Column Chart"}}`, string(seriesB))
	fmt.Println(chartOption)
	if err := f.AddChart("Sheet1", "E1", chartOption); err != nil {
		fmt.Println(err)
		return err
	}
	// Save xlsx file by the given path.
	if err := f.SaveAs(gv.excelizeOutName); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
