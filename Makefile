pb_compile:
	protoc --go_out=. --go_opt=paths=source_relative \
		   --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		   api/api.proto

delete_pb:
	rm api/*.pb.go

run_server:
	rm -rf tmp_log_data && mkdir tmp_log_data
	go run server/server.go --log_data_dir ./tmp_log_data \
	--log_store_max_bytes 4096 --log_index_max_bytes 4096 \
	--tls true