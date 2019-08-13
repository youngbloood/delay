package delay

import (
	"io/ioutil"
	"net/http"
)

type Client struct {
	host      string
	htpClient *http.Client

	Handle func(*PopResp) error
}

func NewClient(host string) *Client {
	return &Client{
		host:      host,
		htpClient: &http.Client{},
	}
}

func (d *Client) Push(req *Req) error {
	htpResp, err := d.htpClient.Post(d.host+"/push", "application/json", req.Read())
	if err != nil {
		return err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return err
	}
	resp := new(Resp)
	if err = resp.Unmarshal(bts); err != nil {
		return err
	}
	return resp.GetError()
}

func (d *Client) Pop(topic string) (pr *PopResp, err error) {

	req := &Req{
		Topic: topic,
	}

	htpResp, err := d.htpClient.Post(d.host+"/pop", "application/json", req.Read())
	if err != nil {
		return nil, err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return nil, err
	}
	resp := new(Resp)
	pr = new(PopResp)
	resp.Data = &pr
	if err = resp.Unmarshal(bts); err != nil {
		return nil, err
	}
	return pr, resp.GetError()
}

func (d *Client) Delete(id string) (*Req, error) {
	req := &Req{
		Id: id,
	}

	htpResp, err := d.htpClient.Post(d.host+"/delete", "application/json", req.Read())
	if err != nil {
		return nil, err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return nil, err
	}
	resp := new(Resp)
	resp.Data = &req
	if err = resp.Unmarshal(bts); err != nil {
		return req, err
	}
	return req, resp.GetError()
}

func (d *Client) Finish(id string) error {
	req := &Req{
		Id: id,
	}
	htpResp, err := d.htpClient.Post(d.host+"/finish", "application/json", req.Read())
	if err != nil {
		return err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return err
	}
	resp := new(Resp)
	return resp.Unmarshal(bts)
}

func (d *Client) Get(id string) (*Req, error) {
	req := &Req{
		Id: id,
	}

	htpResp, err := d.htpClient.Post(d.host+"/get", "application/json", req.Read())
	if err != nil {
		return nil, err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return nil, err
	}
	resp := new(Resp)
	resp.Data = &req
	if err = resp.Unmarshal(bts); err != nil {
		return req, err
	}
	return req, resp.GetError()
}

func (d *Client) RoundPop(topic string, queue chan<- *PopResp) error {
RETRY:
	resp, _ := d.Pop(topic)
	if resp!=nil{
		queue <- resp
	}
	goto RETRY
}

func (d *Client) HandleSync(queue <-chan *PopResp) {
	if d.Handle == nil {
		panic("handle func is nil !")
	}
	for pr := range queue {
		if err := d.Handle(pr); err != nil {
			continue
		}
		d.Finish(pr.Id)
	}
}

func (d *Client) HandleAsync(queue <-chan *PopResp) {
	if d.Handle == nil {
		panic("handle func is nil !")
	}
	for pr := range queue {
		go func(pr *PopResp) {
			if err := d.Handle(pr); err != nil {
				return
			}
			d.Finish(pr.Id)
		}(pr)
	}
}
