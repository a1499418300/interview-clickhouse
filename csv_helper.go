// 提供csv读写功能
package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// CSVParser csv解析器
type CSVParser struct {
	data [][]string
	fs   *os.File // 文件指针
}

// Init 初始化
func (p *CSVParser) Init(path string) error {
	fs, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return errConn
	}
	p.fs = fs
	return nil
}

// Close 关闭连接
func (p *CSVParser) Close() {
	p.fs.Close()
}

// 写csv
func (p *CSVParser) Write(data [][]string) {
	p.fs.Truncate(0) //清空内容
	w := csv.NewWriter(p.fs)
	w.WriteAll(data)
	w.Flush()
}

// 读csv
func (p *CSVParser) read() [][]string {
	p.fs.Seek(0, 0) // 指向文件开头
	r := csv.NewReader(p.fs)
	data := make([][]string, 0)

	//针对大文件，一行一行的读取文件
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			fmt.Printf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			fmt.Println(row)
			break
		}
		data = append(data, row)
	}
	return data
}
