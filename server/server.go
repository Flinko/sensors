package main

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"sensors/protocol"
	"sort"
	"sync"
)

type Data struct {
	max      float32
	min      float32
	avg      float32
	nSamples uint64
	sum      float32
	sync.Mutex
}

type SensorsData struct {
	data map[uint64]*Data
	sync.Mutex
}

const Min float32 = -9999999

func sendResponse(conn *net.UDPConn, addr *net.UDPAddr, request *protocol.Request) {
	res := &protocol.Response{
		SensorId:  request.SensorId,
		Timestamp: request.Timestamp,
	}
	payload, err := proto.Marshal(res)
	if err != nil {
		fmt.Printf("failed to marshal response, err: %v", err)
	}
	_, err = conn.WriteToUDP(payload, addr)
	if err != nil {
		fmt.Printf("respose sending is failed %v", err)
	}
}

func storeData(request *protocol.Request, data *Data, sensorsData *SensorsData) {
	value := request.Temp
	data.Lock()
	defer data.Unlock()
	if data.min == Min {
		data.min = value
	}
	if data.min > value {
		data.min = value
	}
	if data.max < value {
		data.max = value
	}
	data.sum += value
	data.nSamples++

	sensorsData.Lock()
	defer sensorsData.Unlock()
	sensorData, ok := sensorsData.data[request.SensorId]
	if !ok {
		sensorsData.data[request.SensorId] = &Data{
			min:      value,
			max:      value,
			nSamples: 1,
			sum:      value,
		}
	} else {
		if sensorData.min > value {
			sensorData.min = value
		}
		if sensorData.max < value {
			sensorData.max = value
		}
		sensorData.sum += value
		sensorData.nSamples++
	}
}

func WriteDataToFile(data *Data, sensorsData *SensorsData) {
	avg := data.sum / float32(data.nSamples)
	bytes := []byte(fmt.Sprintf("min: %v, max: %v, avg: %v\n", data.min, data.max, avg))
	err := ioutil.WriteFile("allData.txt", bytes, 0777)
	if err != nil {
		fmt.Printf("failed to write all data to file, err: %v", err)
	}

	str := ""
	sensorsData.Lock()
	defer sensorsData.Unlock()
	keys := make([]int, 0, len(sensorsData.data))
	for k := range sensorsData.data {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, k := range keys {
		data, _ := sensorsData.data[uint64(k)]
		avg = data.sum / float32(data.nSamples)
		str += fmt.Sprintf("id: %v, min: %v, max: %v, avg: %v\n", k, data.min, data.max, avg)
	}
	bytes = []byte(str)
	err = ioutil.WriteFile("dataPerSensor.txt", bytes, 0777)
	if err != nil {
		fmt.Printf("failed to write all data to file, err: %v", err)
	}
	fmt.Printf("data saved")
}

func main() {
	data := new(Data)
	data.min = Min
	sensorsData := new(SensorsData)
	sensorsData.data = make(map[uint64]*Data)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		osCall := <-c
		log.Printf("system call:%+v\n", osCall)
		// for saving the data to file before the exit
		WriteDataToFile(data, sensorsData)
		cancel()
	}()

	if err := serve(ctx, data, sensorsData); err != nil {
		log.Printf("failed to serve:+%v\n", err)
	}
}

func serve(ctx context.Context, data *Data, sensorsData *SensorsData) error {
	p := make([]byte, 2048)
	addr := net.UDPAddr{
		Port: 1234,
		IP:   net.ParseIP("127.0.0.1"),
	}
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("error occurred: %v\n", err)
		return err
	}
	fmt.Printf("server running")

	go func() {
		for {
			n, remoteAddr, err := ser.ReadFromUDP(p)
			if err != nil {
				fmt.Printf("failed to read from udp, err: %v", err)
			}
			request := &protocol.Request{}
			err = proto.Unmarshal(p[:n], request)
			if err != nil {
				fmt.Printf("failed to unmarshal request, err: %v", err)
			}
			fmt.Printf("Read a message from %v %s \n", remoteAddr, request)
			go storeData(request, data, sensorsData)
			go sendResponse(ser, remoteAddr, request)
		}
	}()
	<-ctx.Done()
	return nil
}
