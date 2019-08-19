# delay-queue
delayqueue client for [delayqueue](https://github.com/ouqiang/delay-queue)




# Client

```
client = delay.NewClient("delay_host")

```
# Publish Message
```
	req := &delay.Req{
		Topic:     "order",
		Id:        "orderId"
		Delay:     20*60,                     // 20minute
		TimeToRun: 60,                        // ttr:60s
		Body:      "", 
	}
	client.Push(req)

```

# Consume Message

```
import ("github.com/youngbloood/delay")

func cycle(){
	pool := make(chan *delay.PopResp, 1000)

	// 轮巡获取消息
	go client.RoundPop("order", pool)

	// 处理消息的handler
	client.HandleMap["order"] = func(pr *delay.PopResp) error {
		// handle pr
		// ...
		return nil
	}

	// handle with sync
	go client.HandleSync("order", pool)
    // handle with async
	go client.HandleAsync("order", pool)

}

```


