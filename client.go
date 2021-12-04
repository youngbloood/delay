package delay

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Client struct {
	host      string
	htpClient *http.Client
	// topic:handle
	HandleMap map[string]func(*PopResp) error
}

func NewClient(host string) *Client {
	return &Client{
		host:      host,
		htpClient: &http.Client{},
		HandleMap: make(map[string]func(*PopResp) error, 0),
	}
}

func (d *Client) SetClient(client *http.Client) {
	d.htpClient = client
}

func (d *Client) Push(req *Req) (*Resp, error) {
	htpResp, err := d.htpClient.Post(d.host+"/push", "application/json", req.Read())
	if err != nil {
		return nil, err
	}
	defer htpResp.Body.Close()
	bts, err := ioutil.ReadAll(htpResp.Body)
	if err != nil {
		return nil, err
	}
	resp := new(Resp)
	if err = resp.Unmarshal(bts); err != nil {
		return nil, err
	}
	return resp, resp.GetError()
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
	resp, err := d.Pop(topic)
	if err != nil {
		return err
	}
	if resp != nil {
		queue <- resp
	}
	goto RETRY
}

func (d *Client) HandleSync(topic string, queue <-chan *PopResp) error {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	handle, ok := d.HandleMap[topic]
	if !ok || handle == nil {
		panic(fmt.Sprintf("handle[%s] func is nil !", topic))
	}

	for pr := range queue {
		if err := handle(pr); err != nil {
			log.Println("sync handle message err = ", err)
			continue
		}
		if err := d.Finish(pr.Id); err != nil {
			return err
		}
	}
	return nil
}

func (d *Client) HandleAsync(topic string, queue <-chan *PopResp) {
	handle, ok := d.HandleMap[topic]
	if !ok || handle == nil {
		panic(fmt.Sprintf("handle[%s] func is nil !", topic))
	}

	for pr := range queue {
		go func(pr *PopResp) {
			defer func() {
				if err := recover(); err != nil {
					log.Println("recover() err = ", err)
				}
			}()

			if err := handle(pr); err != nil {
				log.Println("handle message err = ", err)
				return
			}
			d.Finish(pr.Id)
		}(pr)
	}
}
