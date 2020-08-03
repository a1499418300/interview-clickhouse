// 生成模拟csv，并读csv数据入库
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// People 测试结构
type People struct {
	ID         int
	Name       string
	Age        int
	Area       string
	Score      int
	UpdateTime time.Time
}

var (
	firstName = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田樊胡凌霍虞万支柯昝管卢莫经房裘缪干解应宗丁宣贲邓郁单杭洪包诸左石崔吉钮龚程嵇邢滑裴陆荣翁荀羊於惠甄麹家封芮羿储靳汲邴糜松井段富巫乌焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘景詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲台从鄂索咸籍赖卓蔺屠蒙池乔阴欎胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍郤璩桑桂濮牛寿通边扈燕冀郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎戈廖庾终暨居衡步都耿满弘匡国文寇广禄阙东欧殳沃利蔚越夔隆师巩厍聂晁勾敖融冷訾辛阚那简饶空曾毋沙乜养鞠须丰巢关蒯相查后荆红"
	lastName  = "子赫祺祾朝彦圣鹏新哲鼎明楠明裕昊智棋皓福敬坤渊荣景尧敬洪朝实善玮朝棋朝寒楷林景瑞琪洋捷杰寒柏敬易涛光鼎益朝波新明昌震皓翔乔豪敬轩尚兴皓清裕明杰宇岩乐乔宁乔诚川善东辉皓宁雄杰金锋涛宇楠峻靖轩尚欧琪哲皓景昊辉雨锋智凯捷旭雨逸宜磊川峰智睿尚啸铭晨莱远宝峰涆安腾波星儒玥隆日麒震可远皓宇正铭振蓄景曜为昂康豪嘉晟良逸凌珹耀轩越燎段炎殿淼瀚昌烨黎伟昱名彭奎爵立嘉珂行成曦栋"
	areas     = []string{"宝安区", "南山区", "光明新区", "大鹏新区", "龙华新区", "龙岗区", "罗湖区"}
	couter    = 0
	// CNWidth utf8中汉字所占字节
	CNWidth = 3
)

func (p *People) string() []string {
	return []string{
		strconv.Itoa(p.ID),
		p.Name,
		strconv.Itoa(p.Age),
		p.Area,
		strconv.Itoa(p.Score),
	}
}

func parsePeople(data []string) (People, error) {
	if len(data) < 5 {
		return People{}, errors.New("input data don't match People columns")
	}

	ID, e := strconv.Atoi(data[0])
	if e != nil {
		return People{}, e
	}
	age, e := strconv.Atoi(data[2])
	if e != nil {
		return People{}, e
	}
	score, e := strconv.Atoi(data[4])
	if e != nil {
		return People{}, e
	}

	return People{
		ID:    ID,
		Name:  data[1],
		Age:   age,
		Area:  data[3],
		Score: score,
	}, nil
}

func getMockData(n int) [][]string {
	res := make([][]string, 0)
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	firstNameLen := len(firstName)/CNWidth - 1
	lastNameLen := len(lastName)/CNWidth - 2
	areasLen := len(areas)

	for i := 0; i < n; i++ {
		couter++
		firstNameIdx := CNWidth * seed.Intn(firstNameLen)
		lastNameIdx := CNWidth * seed.Intn(lastNameLen)
		areaIdx := seed.Intn(areasLen)
		age := int(seed.Float64() * 100)
		score := int(seed.Float64() * 100)
		name := firstName[firstNameIdx:firstNameIdx+CNWidth] + lastName[lastNameIdx:lastNameIdx+2*CNWidth]

		p := People{
			ID:    couter,
			Name:  name,
			Age:   age,
			Area:  areas[areaIdx],
			Score: score,
		}
		fmt.Println(p.string())
		res = append(res, p.string())
	}
	return res
}

func mockTask() error {
	// 生成csv
	csvProxy := CSVParser{}
	defer csvProxy.Close()
	e := csvProxy.Init("./testdata.csv")
	if e != nil {
		fmt.Printf("csv parse init failed, err: %v\n", e)
		return e
	}

	title := [][]string{{"编号", "姓名", "年龄", "所在地区", "考试成绩"}}
	data := getMockData(1000)
	data = append(title, data...)
	csvProxy.Write(data)
	fmt.Println("create csv success!")

	// 写DB
	DBProxy := ClickHouseProxy{}
	e = DBProxy.Conn("tcp://127.0.0.1:9000?debug=true")
	if e != nil {
		return e
	}
	defer DBProxy.Close()

	//创建表
	e = createDB(DBProxy)
	if e != nil {
		fmt.Printf("create table failed, err: %+v\n", e)
		return e
	}

	// 写入数据
	rows := csvProxy.read()
	if len(rows) == 0 {
		return errCSVRowLen
	}
	e = writeDB(DBProxy, rows[1:]) // 不写入csv标题
	if e != nil {
		fmt.Printf("write DB failed, err: %+v\n", e)
		return e
	}
	fmt.Println("write DB success!")
	return nil
}
