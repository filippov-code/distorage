$protoFile = "node.proto"
$protoPath = "."

$out = "."

protoc --proto_path=$protoPath --go_out=$out --go-grpc_out=$out $protoFile