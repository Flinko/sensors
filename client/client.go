package main

import (
	"bufio"
	"flag"
	"fmt"
	"google.golang.org/protobuf/proto"
	"math/rand"
	"net"
	"sensors/protocol"
	"sync"
	"time"
)

type Cache struct {
	samples map[int64]float32
}

func getTemperature(id uint64) float32 {
	s := rand.NewSource(time.Now().Unix() + int64(id))
	r := rand.New(s)
	value := r.Float32()
	return value
}

func genSensorRequest(id uint64) protocol.Request {
	value := getTemperature(id)
	return protocol.Request{
		SensorId:  id,
		Temp:      value,
		Timestamp: time.Now().Unix(),
	}
}

var wg sync.WaitGroup

func main() {
	n := flag.Uint64("clients", 1000, "amount of clients, default: 100")
	fmt.Println(*n)
	cachePerSensor := make(map[uint64]*Cache)
	var i uint64 = 1
	for ; i <= *n; i++ {
		cache := new(Cache)
		cache.samples = make(map[int64]float32)
		cachePerSensor[i] = cache
		runSensor(i, cache)
		wg.Add(1)
	}
	wg.Wait()
}

func runSensor(id uint64, cache *Cache) {
	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-ticker.C:
				fmt.Printf("sensor: %v\n", id)
				request := genSensorRequest(id)
				cache.samples[request.Timestamp] = request.Temp
				if err := sendRequest(&request, cache); err != nil {
					fmt.Printf("sentRequest failed: %v", err)
				}
			}
		}
	}()
}

func sendRequest(request *protocol.Request, cache *Cache) error {
	p, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request, err: %v", err)
	}
	conn, err := net.Dial("udp", "127.0.0.1:1234")
	if err != nil {
		return fmt.Errorf("dial error occured: %v", err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Printf("connection close failed, err: %v", err)
		}
	}()
	n, err := conn.Write(p)
	if err != nil {
		return fmt.Errorf("failed to write to connection, err: %v", err)
	}
	buffer := make([]byte, 1024)
	n, err = bufio.NewReader(conn).Read(buffer)
	response := &protocol.Response{}
	err = proto.Unmarshal(buffer[:n], response)
	if err != nil {
		fmt.Printf("failed to unmarshal response, err: %v", err)
	} else {
		fmt.Printf("resp: %v\n", response)
		delete(cache.samples, response.Timestamp)
	}
	return nil
}
