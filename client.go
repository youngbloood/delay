package delay

import (
	"io/ioutil"
	"net/http"
)

type Client struct {
	Host      string
	htpClient *http.Client
}

func NewClient(host string) *Client {
	return &Client{
		Host:      host,
		htpClient: &http.Client{},
	}
}

func (d *Client) Push(req *Req) error {
	htpResp, err := d.htpClient.Post(d.Host+"/push", "application/json", req.Read())
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

	htpResp, err := d.htpClient.Post(d.Host+"/pop", "application/json", req.Read())
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

	htpResp, err := d.htpClient.Post(d.Host+"/delete", "application/json", req.Read())
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
	htpResp, err := d.htpClient.Post(d.Host+"/finish", "application/json", req.Read())
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

	htpResp, err := d.htpClient.Post(d.Host+"/get", "application/json", req.Read())
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
	req, err := d.Pop(topic)
	if err != nil {
		goto RETRY
	}
	queue <- req
	goto RETRY
}
