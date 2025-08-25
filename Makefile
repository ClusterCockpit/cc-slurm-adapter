.PHONY: cc-slurm-adapter clean

cc-slurm-adapter:
	go build ./cmd/cc-slurm-adapter
	go vet ./...

clean:
	go clean
	rm -f cc-slurm-adapter
