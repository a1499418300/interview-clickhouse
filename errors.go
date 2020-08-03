package main

// Error 错误
type Error struct {
	Code int
	Msg  string
}

const (
	okMsg   = "success"
	oriMsg  = "original error"
	okCode  = 0
	oriCode = -999
)

// 业务定义的错误码
var (
	errConn       = NewErr(-1001, "connect failed")
	errRead       = NewErr(-1002, "read csv failed")
	errInputFront = NewErr(-1003, "front input not pass check")
	errCSVRowLen = NewErr(-1003, "csv len not pass check")
)

// Error 继承error
func (e *Error) Error() string {
	if e == nil {
		return okMsg
	}
	return e.Msg
}

// NewErr 创建一个错误对象
func NewErr(code int, msg string) error {
	return &Error{
		Code: code,
		Msg:  msg,
	}
}

// ErrCode 从错误中获取code
func ErrCode(e error) int {
	if e == nil {
		return okCode
	}
	err, ok := e.(*Error)
	if !ok {
		return oriCode
	}
	if err == (*Error)(nil) {
		return okCode
	}
	return err.Code
}

// ErrMsg 从错误中获取msg
func ErrMsg(e error) string {
	if e == nil {
		return okMsg
	}
	err, ok := e.(*Error)
	if !ok {
		return e.Error()
	}
	if err == (*Error)(nil) {
		return okMsg
	}
	return err.Msg
}
