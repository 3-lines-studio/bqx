BIN := bqx

include ../check.mk

.PHONY: build clean
build:
	$(GO) build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o $(BIN) .

clean:
	rm -f $(BIN)
