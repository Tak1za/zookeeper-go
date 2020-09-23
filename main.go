package main

import (
	"fmt"
	"log"
	"time"

	"github.com/go-zookeeper/zk"
)

type client struct {
	zkClient *zk.Conn
}

func getPath(node string) string {
	return "/" + node
}

func connectZK(server string) (*client, <- chan zk.Event, error){
	zk, events, err := zk.Connect([]string{server}, time.Second)
	if err != nil {
		return nil, nil, err
	}

	return &client{zk}, events, nil
}

func (client *client) getChildren(path string) ([]string, *zk.Stat, error) {
	data, stat, err := client.zkClient.Children(path)
	if err != nil {
		return nil, nil, err
	}

	return data, stat, nil
}

func (client *client) createChildren(path string, data string) error{
	_, err := client.zkClient.Create(path, []byte(data), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	return nil
}

func main(){
	client, events, err := connectZK("127.0.0.1:2181")
	if err != nil {
		log.Println(err)
	}

	defer client.zkClient.Close()

	go func(){
		for {
			event := <- events
			log.Println("Zookeeper state: ", event.State)
			if event.State == zk.StateHasSession {
				log.Println("Session established")
			}
		}
	}()

	//create a znode
	if err = client.createChildren(getPath("program-test"), ""); err != nil {
		log.Println(err)
	}

	//get children
	children, stat, err := client.getChildren("/")
	if err != nil {
		log.Println(err)
	}

	fmt.Printf("%+v %+v\n", children, stat)
}