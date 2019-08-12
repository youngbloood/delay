package delay

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

type Req struct {
	Topic     string `json:"topic,omitempty"` // Job类型
	Id        string `json:"id,omitempty"`    // Job唯一标识(确保job唯一)
	Delay     int64  `json:"delay,omitempty"` // Job需要延迟的时间, 单位：秒
	TimeToRun int64  `json:"ttr,omitempty"`   // Job执行超时时间, 单位：秒
	Body      string `json:"body,omitempty"`  // Job的内容，供消费者做具体的业务处理，如果是json格式需转义
}

func (r *Req) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Req) Read() io.Reader {
	bts, _ := r.Marshal()
	return bytes.NewBuffer(bts)
}

type Resp struct {
	Code    int         `json:"code"`    //状态码	0: 成功 非0: 失败
	Message string      `json:"message"` // 状态描述信息
	Data    interface{} `json:"data"`    // 附加信息
}

func (r *Resp) Unmarshal(bts []byte) error {
	return json.Unmarshal(bts, &r)
}

func (r *Resp) HasError() bool {
	if r.Code != 0 {
		return true
	}
	return false
}

func (r *Resp) GetError() error {
	if r.HasError() {
		return errors.New(r.Message)
	}
	return nil
}

type PopResp struct {
	Id   string `json:"id"`
	Body string `json:"body"`
}
