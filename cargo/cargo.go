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

	cargoInfo.conn = conn
	cargoInfo.service = taskToCargo.NewRpcTaskToCargoClient(conn)

	return &cargoInfo
}

// fileName - absolute of relative path of the file
func (cargoInfo *CargoInfo) Send(fileName string) {
	// single shot file transfer
	// the data will be stored in

	fileBuf := ioutil.ReadFile(fileName)
	dts := taskToCargo.DataToStore{
		FileName:   fileName,
		FileBuffer: fileBuf,
		FileSize:   len(fileBuf),
		FileType:   filepath.Ext(fileName),
	}

	ack, err := cargoInfo.service.StoreInCargo(context.Background(), &dts)
	cmd.CheckError(err)

	fmt.Println(ack.GetAck())

}

func (cargoInfo *CargoInfo) SendStream(fileName string, fileBuffer []byte) {

}

func (cargoInfo *CargoInfo) CleanUp() {
	cargoInfo.conn.Close()
}
