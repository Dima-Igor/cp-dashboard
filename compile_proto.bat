python -m grpc_tools.protoc -I./protos --python_out="./cf-service" --grpc_python_out="./cf-service" ./protos/submission.proto