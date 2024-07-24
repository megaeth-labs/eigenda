package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/eigenda/api/clients"
	"github.com/Layr-Labs/eigenda/core/auth"
	"github.com/Layr-Labs/eigenda/encoding/utils/codec"
	"time"
)

const timeout = 10 * time.Second

const cap = 1000000

type BlobLog struct {
	Time      string `json:"time"`
	Status    string `json:"status"`
	Msg       string `json:"msg"`
	RequestId string `json:"info"`
}

var sendCh = make(chan string, cap)

func NewClient() clients.DisperserClient {

	privateKeyHex := "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff8d"
	signer := auth.NewLocalBlobRequestSigner(privateKeyHex)

	disp := clients.NewDisperserClient(&clients.Config{
		Hostname: "localhost",
		Port:     "32003",
		//Hostname:          "disperser-holesky.eigenda.xyz",
		//Port:              "443",
		//UseSecureGrpcFlag: true,
		Timeout: timeout,
	}, signer)

	return disp
}

func timeNow() string {
	now := time.Now()
	return fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d.%dZ", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond())
}

func printBlobLog(status string, requestId string) {
	var blobLog = BlobLog{
		Time:      timeNow(),
		Status:    status,
		Msg:       "MegaETH store blob with metadataKey",
		RequestId: requestId,
	}
	j, _ := json.Marshal(blobLog)
	fmt.Println(string(j))
}

func SendData() (string, error) {
	fmt.Println("begin send data")
	disp := NewClient()
	data := make([]byte, 192*1024*1024/100)
	//data := make([]byte, 10)
	_, err := rand.Read(data)
	if err != nil {
		return "", err
	}

	paddedData := codec.ConvertByPaddingEmptyByte(data)
	//{
	//	fmt.Println("raw:", hex.EncodeToString(paddedData))
	//	encoded := base64.StdEncoding.EncodeToString(paddedData)
	//	fmt.Println("encodeded:", encoded)
	//}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 10*timeout)
	defer cancel()

	blobStatus1, key1, err := disp.DisperseBlob(ctxTimeout, paddedData, []uint8{})
	if err != nil {
		fmt.Println("send fail, debug000000000")
		return "", err
	}
	if blobStatus1 == nil {
		return "", errors.New("blob status == nil")
	}
	if key1 == nil {
		return "", errors.New("key == nil")
	}
	encoded := base64.StdEncoding.EncodeToString(key1)
	printBlobLog(blobStatus1.String(), encoded)
	//fmt.Println("blobStatus:", blobStatus1.String(), "info", encoded, "time", time.Now().Format("%d-%02d-%02dT%02d:%02d:%02d-00:00"))
	return encoded, nil
}

func RetrieveData(ctx context.Context) {

	for {
		fmt.Println("begin retrieve data")
		select {
		case <-ctx.Done():
			break
		case requestId := <-sendCh:
			fmt.Println("receive requestId:", requestId)
			go func() {
				i := 0
				for {
					i += 1
					disp := NewClient()
					key1, err := base64.StdEncoding.DecodeString(requestId)
					if err != nil {
						fmt.Println("decode err:", err.Error())
					}

					ctxTimeout, cancel := context.WithTimeout(context.Background(), timeout)
					defer cancel()

					ret, err := disp.GetBlobStatus(ctxTimeout, key1)
					if err != nil {
						fmt.Println("get status err:", err.Error())
						continue
					}
					if ret.Status.String() == "CONFIRMED" {
						printBlobLog(ret.Status.String(), requestId)
						return
					} else if ret.Status.String() == "FAILED" {
						return
					}
					fmt.Println("i:", i, ", ret:", ret.Status.String(), ",requestId:", requestId)
					time.Sleep(60 * time.Second)
				}
			}()
		}
	}
	fmt.Println("end unexpected, retrieve")
}

func main() {

	interval := int64(1000)
	blobNumber := 5

	interval = interval * 1 * int64(time.Millisecond)
	ticker := time.NewTicker(time.Duration(interval))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	go RetrieveData(ctx)
	for {
		select {
		case <-ctx2.Done():
			break
		case <-ticker.C:
			for i := 0; i < blobNumber; i++ {
				go func() {
					requestId, err := SendData()
					if err != nil {
						fmt.Println("send fail", err.Error())
						return
					}
					sendCh <- requestId
				}()
			}
		}
	}

}
