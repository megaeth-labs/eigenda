package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/eigenda/encoding/utils/codec"
	"os/exec"
	"time"
)

func SendData() (status string, requestId string, err error) {
	//生成data
	data := make([]byte, 700*1024)
	_, err = rand.Read(data)
	if err != nil {
		return "", "", err
	}

	bz := data
	padded := codec.ConvertByPaddingEmptyByte(bz)
	encoded := base64.StdEncoding.EncodeToString(padded)
	fmt.Println("len(padded):", len(padded))

	//命令行发送
	{
		cmdData := `{"data": "` + encoded + `"}`
		cmd := exec.Command("grpcurl",
			"-import-path", "../../api/proto",
			"-proto", "../.././api/proto/disperser/disperser.proto",
			"-d", cmdData,
			"disperser-holesky.eigenda.xyz:443", "disperser.Disperser/DisperseBlob",
		)
		var out bytes.Buffer
		var errOut bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &errOut
		//fmt.Println("cmd:", cmd.String())
		err = cmd.Run()
		if err != nil {
			return "", "", err
		}
		fmt.Println("stdOut:", out.String())

		type Resp struct {
			Status    string `json:"result"`
			RequestId string `json:"requestId"`
		}

		var resp Resp
		err = json.Unmarshal(out.Bytes(), &resp)
		if err != nil {
			return "", "", err
		}
		fmt.Println("status:", resp.Status, ", requestId:", resp.RequestId)
		return resp.Status, resp.RequestId, nil
	}

}

func main() {
	interval := int64(1)
	blobNumber := 1

	interval = interval * 1 * int64(time.Second)
	ticker := time.NewTicker(time.Duration(interval))
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	for {
		select {
		case <-ctx2.Done():
			break
		case <-ticker.C:
			go func() {
				for i := 0; i < blobNumber; i++ {
					_, _, err := SendData()
					if err != nil {
						fmt.Println("send fail", err.Error())
						continue
					}
				}
			}()
		}
	}
}
