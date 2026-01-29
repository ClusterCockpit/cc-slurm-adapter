.PHONY: cc-slurm-adapter clean format

cc-slurm-adapter:
	go build ./cmd/cc-slurm-adapter
	go vet ./...

clean:
	go clean
	rm -f cc-slurm-adapter

format:
	go fmt ./...

update:
	go get -u ./...
	go mod tidy
