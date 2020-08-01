package main

import "fmt"

func main() {
	p := CSVParser{}
	e := p.Init("./testdata.csv")
	if e != nil {
		fmt.Printf("csv parse init failed, err: %v\n", e)
		return
	}

	data := getMockData(1000)
	p.Write(data)
	fmt.Println("写入完成")
}