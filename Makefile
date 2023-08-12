DEV_LOG_DATA_DIR    := "dev_log_data"
DEV_STORE_MAX_BYTES := 4096
DEV_INDEX_MAX_BYTES := 4096
DEV_LOG_USE_TLS     := true


pb_compile:
	protoc --go_out=. --go_opt=paths=source_relative \
		   --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		   api/api.proto


delete_pb:
	rm api/*.pb.go


run_server:
	if [ ! -d "${DEV_LOG_DATA_DIR}" ]; then \
		echo "Creating the log data direcotory ${DEV_LOG_DATA_DIR}..."; \
		mkdir ${DEV_LOG_DATA_DIR}; \
	fi

	go run server/server.go --log_data_dir ${DEV_LOG_DATA_DIR} \
	--log_store_max_bytes ${DEV_STORE_MAX_BYTES} \
	--log_index_max_bytes ${DEV_INDEX_MAX_BYTES} \
	--tls ${DEV_LOG_USE_TLS}
