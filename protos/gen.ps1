# gen.ps1
$protoFile = "gateway.proto"
$protoPath = "."

$nodeOut = "..\node\internal\grpc\gateway"
$gatewayOut = "..\gateway\internal\grpc"

protoc --proto_path=$protoPath --go_out=$nodeOut --go-grpc_out=$nodeOut $protoFile
protoc --proto_path=$protoPath --go_out=$gatewayOut --go-grpc_out=$gatewayOut $protoFile
