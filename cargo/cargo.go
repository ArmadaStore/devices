package cargo

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/ArmadaStore/cargo/pkg/cmd"
	"github.com/ArmadaStore/comms/rpc/taskToCargo"
)

type CargoInfo struct {
	IP      string
	Port    string
	AppID   string
	UserID  string
	conn    *grpc.ClientConn
	service taskToCargo.RpcTaskToCargoClient
}

func InitCargo(IP string, Port string, AppID string, UserID string) *CargoInfo {
	var cargoInfo CargoInfo

	cargoInfo.IP = IP
	cargoInfo.Port = Port
	cargoInfo.AppID = AppID
	cargoInfo.UserID = UserID

	conn, err := grpc.Dial(IP+":"+Port, grpc.WithInsecure())
	cmd.CheckError(err)

	fmt.Println("Connection Success with", IP, "and", Port, "\n")

	cargoInfo.conn = conn
	cargoInfo.service = taskToCargo.NewRpcTaskToCargoClient(conn)

	return &cargoInfo
}

// fileName - absolute of relative path of the file
func (cargoInfo *CargoInfo) Send(fileName string) {
	// single shot file transfer
	// the data will be stored in

	fileBuf, err := ioutil.ReadFile(fileName)
	cmd.CheckError(err)

	dts := taskToCargo.DataToStore{
		FileName:   fileName,
		FileBuffer: fileBuf,
		FileSize:   int64(len(fileBuf)),
		FileType:   filepath.Ext(fileName),
		AppID:      cargoInfo.AppID,
	}

	ack, err := cargoInfo.service.StoreInCargo(context.Background(), &dts)
	cmd.CheckError(err)

	fmt.Println(ack.GetAck())
}

func (cargo *CargoInfo) Recv(fileName string) {

	fileInfo := taskToCargo.FileInfo{FileName: fileName}
	dtl, err := cargo.service.LoadFromCargo(context.Background(), &fileInfo)
	cmd.CheckError(err)

	fileBuffer := dtl.GetFileBuffer()
	//fileSize := dts.GetFileSize()
	//fileType := dts.GetFileType()

	err = ioutil.WriteFile(fileName, fileBuffer, 0644)
	cmd.CheckError(err)
}

func (cargo *CargoInfo) Write(fileName string, content string) {
	wtc := taskToCargo.WriteData{
		FileName:   fileName,
		FileBuffer: []byte(content),
		WriteSize:  int64(len(content)),
		AppID:      cargo.AppID,
	}

	ack, err := cargo.service.WriteToCargo(context.Background(), &wtc)
	cmd.CheckError(err)

	fmt.Println(ack.GetAck())
}

func (cargo *CargoInfo) Read(fileName string) string {
	readInfo := taskToCargo.ReadInfo{FileName: fileName}
	rfc, err := cargo.service.ReadFromCargo(context.Background(), &readInfo)
	cmd.CheckError(err)

	fileBuffer := rfc.GetFileBuffer()

	return string(fileBuffer)
}

func (cargoInfo *CargoInfo) SendStream(fileName string, fileBuffer []byte) {

}

func (cargoInfo *CargoInfo) RecvStream(fileName string, fileBuffer []byte) {

}

func (cargoInfo *CargoInfo) CleanUp() {
	cargoInfo.conn.Close()
}
