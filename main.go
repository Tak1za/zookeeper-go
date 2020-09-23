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

func (client *client) getNodeData(path string) ([]byte, *zk.Stat, error) {
	rawData, stat, err := client.zkClient.Get(path)
	if err != nil {
		return nil, nil, err
	}

	return rawData, stat, nil
}

func (client *client) deleteNode(path string) error {
	if err := client.zkClient.Delete(path, -1); err != nil {
		return err
	}

	return nil
}

func (client *client) setNodeData(path, data string) error {
	_, err := client.zkClient.Set(path, []byte(data), -1)
	if err != nil {
		return err
	}

	return nil
}

func (client *client) checkNodeExists(path string) (bool, error) {
	exists, _, err := client.zkClient.Exists(path)
	if err != nil {
		return exists, err
	}

	return exists, nil
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

	const testPath = "program-test2"

	//create a znode
	if err := client.createChildren(getPath(testPath), "program-test-data-2"); err != nil {
		log.Println(err)
	}
	log.Printf("znode added at path: %s\n", testPath)

	//get children
	children, stat, err := client.getChildren("/")
	if err != nil {
		log.Println(err)
	}

	fmt.Printf("%+v %+v\n", children, stat)

	//set data to a znode
	if err := client.setNodeData(getPath(testPath), "new-program-test-data-2"); err != nil {
		log.Println(err)
	}
	log.Println("set data completed at path: ", testPath)

	//get znode data
	rawData, stat, err := client.getNodeData(getPath(testPath))
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("%+v %+v\n", string(rawData), stat)

	//check node exists before deleting
	exists, err := client.checkNodeExists(getPath(testPath))
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("Node at path: %s exists: %t\n", testPath, exists)

	//delete znode
	if err := client.deleteNode(getPath(testPath)); err != nil {
		log.Println(err)
	}
	fmt.Printf("znode deleted at path: %s\n", testPath)

	//check node exists after deleting
	exists, err = client.checkNodeExists(getPath(testPath))
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("Node at path: %s exists: %t\n", testPath, exists)
}