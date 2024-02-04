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

	go run -race server/cmd/main.go --log-data-dir ${DEV_LOG_DATA_DIR} \
	--log-store-max-bytes ${DEV_STORE_MAX_BYTES} \
	--log-index-max-bytes ${DEV_INDEX_MAX_BYTES} \
	--tls ${DEV_LOG_USE_TLS}


default_server:
	if [ ! -d "${DEV_LOG_DATA_DIR}" ]; then \
		echo "Creating the log data direcotory ${DEV_LOG_DATA_DIR}..."; \
		mkdir ${DEV_LOG_DATA_DIR}; \
	fi

	go run -race server/cmd/main.go --log-data-dir ${DEV_LOG_DATA_DIR}


server_help:
	go run server/cmd/main.go --help


run_benchmarks:
	go test -benchmem -bench=. ./benchmarks
