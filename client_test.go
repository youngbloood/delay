package delay_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/youngbloood/delay"
)

var client = delay.NewClient("http://localhost:9277")

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func print(v interface{}) {
	bts, _ := json.Marshal(v)
	fmt.Println("resp = ", string(bts))
}

func TestDelayPush(t *testing.T) {
	req := &delay.Req{
		Topic:     "order",
		Id:        "234567",
		Delay:     60,
		TimeToRun: 3600,
	}
	if err := client.Push(req); err != nil {
		panic(err)
	}

	fmt.Println("push success")
}

func TestDelayGet(t *testing.T) {
	resp, err := client.Get("234567")
	if err != nil {
		panic(err)
	}

	print(resp)
}

func TestDelayPop(t *testing.T) {
	resp, err := client.Pop("order")
	if err != nil {
		panic(err)
	}

	print(resp)
}

func TestDelayDelete(t *testing.T) {
	resp, err := client.Delete("234567")
	if err != nil {
		panic(err)
	}

	print(resp)
}

func TestDelayFinish(t *testing.T) {
	err := client.Finish("234567")
	if err != nil {
		panic(err)
	}
	fmt.Println("finish")
}
