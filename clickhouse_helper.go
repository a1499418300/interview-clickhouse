// clickhouse简单封装，可以去掉
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

var (
	errQuery = errors.New("query DB failed")
)

// ClickHouseProxy 数据库对象
type ClickHouseProxy struct {
	conn *sql.DB
}

// Counter 聚合类查询结果
type Counter struct {
	Key string
	Num int
	Max int
	Min int
	Sum int
	Avg float32
}

// Conn 连接数据库
// addr tcp://127.0.0.1:9000?debug=true
func (p *ClickHouseProxy) Conn(addr string) error {
	conn, err := sql.Open("clickhouse", addr)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); !ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			return err
		}
	}
	p.conn = conn
	return nil
}

// Exec 执行sql
func (p *ClickHouseProxy) Exec(sql string) (sql.Result, error) {
	return p.conn.Exec(sql)
}

// GetConn 获取连接
func (p *ClickHouseProxy) GetConn() *sql.DB {
	return p.conn
}

// Close 关闭连接
func (p *ClickHouseProxy) Close() {
	p.conn.Close()
}

// ---分割线：业务代码---
// TinyLog 一写多读
func createDB(p ClickHouseProxy) error {
	_, e := p.Exec(`
		CREATE TABLE IF NOT EXISTS people (
			id			UInt64,
			name		String,
			age   		UInt8,
			area   		String,
			score   	UInt8,
			update_time DateTime
		) engine=TinyLog
	`)
	if e != nil {
		return e
	}
	return nil
}

func writeDB(p ClickHouseProxy, rows [][]string) error {
	conn := p.GetConn()
	tx, _ := conn.Begin()
	stmt, _ := tx.Prepare("INSERT INTO people (id, name, age, area, score, update_time) VALUES (?, ?, ?, ?, ?, ?)")
	defer stmt.Close()

	for _, row := range rows {
		o, e := parsePeople(row)
		if e != nil {
			fmt.Printf("parsePeople failed, err: %+v, row: %+v\n", e, row)
			continue
		}

		_, e = stmt.Exec(
			o.ID,
			o.Name,
			o.Age,
			o.Area,
			o.Score,
			time.Now(),
		)
		if e != nil {
			fmt.Printf("insert DB failed, err: %+v, obj: %+v\n", e, o)
			continue
		}
	}
	if e := tx.Commit(); e != nil {
		fmt.Printf("commit to DB failed, err: %+v\n", e)
	}
	return nil
}

func queryPeople(p ClickHouseProxy, condition string, pageIdx, pageSize int) ([]People, int, error) {
	likeStr := fmt.Sprintf("%%%s%%", condition)
	valInt, e := strconv.Atoi(condition)
	if e != nil {
		valInt = -1 // 暂时这样处理
	}

	// 查询总符合条件条数
	rows, e := p.conn.Query(`
		SELECT count(1) FROM people where name like ? or area like ? or id = ? or age = ? or score = ?
	`, likeStr, likeStr, valInt, valInt, valInt)
	if e != nil {
		return []People{}, 0, e
	}
	var total int
	if rows.Next() {
		if err := rows.Scan(&total); err != nil {
			return []People{}, 0, e
		}
	}

	// 查询数据结果
	rows, e = p.conn.Query(`
		SELECT * FROM people where name like ? or area like ? or id = ? or age = ? or score = ? limit ?, ?
	`, likeStr, likeStr, valInt, valInt, valInt, pageIdx*pageSize, pageSize)
	if e != nil {
		return []People{}, 0, e
	}

	defer rows.Close()
	items := make([]People, 0)
	for rows.Next() {
		var o People
		if err := rows.Scan(&o.ID, &o.Name, &o.Age, &o.Area, &o.Score, &o.UpdateTime); err != nil {
			fmt.Printf("scan from *sql.Rows failed, err: %+v\n", err)
			continue
		}
		items = append(items, o)
	}
	return items, total, nil
}

func areaCounter(p ClickHouseProxy, column string) ([]Counter, error) {
	// 只支持 area 对 age 或 score 的统计
	var sql string
	if column == "age" {
		sql = `SELECT area,count(1),avg(age),max(age),min(age),sum(age) FROM people group by area`
	} else {
		sql = `SELECT area,count(1),avg(score),max(score),min(score),sum(score) FROM people group by area`
	}

	rows, e := p.conn.Query(sql)
	if e != nil {
		return []Counter{}, e
	}

	defer rows.Close()
	items := make([]Counter, 0)
	for rows.Next() {
		var o Counter
		if err := rows.Scan(&o.Key, &o.Num, &o.Avg, &o.Max, &o.Min, &o.Sum); err != nil {
			fmt.Printf("scan from *sql.Rows failed, err: %+v\n", err)
			continue
		}
		items = append(items, o)
	}
	return items, nil
}
